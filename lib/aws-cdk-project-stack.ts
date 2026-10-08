import * as cdk from 'aws-cdk-lib/core';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as rds from 'aws-cdk-lib/aws-rds';
import * as cr from 'aws-cdk-lib/custom-resources';
import * as lambda from 'aws-cdk-lib/aws-lambda-nodejs';
import * as lambdaBase from 'aws-cdk-lib/aws-lambda';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as subscriptions from 'aws-cdk-lib/aws-sns-subscriptions';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';

const DATABASE_NAME = 'orders';

export class AwsCdkProjectStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // =========================================================================
    // Data store — Aurora Serverless v2 (Postgres), queried via the RDS Data
    // API (HTTPS calls), so Lambda never needs to sit inside the VPC or
    // manage DB connections/pooling itself.
    // =========================================================================
    const vpc = new ec2.Vpc(this, 'OrdersVpc', {
      ipAddresses: ec2.ipAddresses.cidr('10.0.0.0/16'),
      maxAzs: 2,
      natGateways: 0, // not needed: nothing in this VPC talks to the internet
      subnetConfiguration: [
        { name: 'isolated', subnetType: ec2.SubnetType.PRIVATE_ISOLATED, cidrMask: 24 },
      ],
    });
    const cluster = new rds.DatabaseCluster(this, 'OrdersDbCluster', {
      engine: rds.DatabaseClusterEngine.auroraPostgres({
        version: rds.AuroraPostgresEngineVersion.VER_15_4,
      }),
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      writer: rds.ClusterInstance.serverlessV2('Writer'),
      serverlessV2MinCapacity: 0.5,
      serverlessV2MaxCapacity: 2,
      enableDataApi: true,
      defaultDatabaseName: DATABASE_NAME,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // One-time schema bootstrap, run through the Data API at deploy time —
    // no bastion host, no manual `psql`, no VPC access needed to set up
    // the table the Lambdas depend on.
    const initSchema = new cr.AwsCustomResource(this, 'OrdersTableInit', {
      onCreate: {
        service: 'RDSDataService',
        action: 'executeStatement',
        parameters: {
          resourceArn: cluster.clusterArn,
          secretArn: cluster.secret!.secretArn,
          database: DATABASE_NAME,
          sql: `CREATE TABLE IF NOT EXISTS orders (
            id TEXT PRIMARY KEY,
            item TEXT NOT NULL,
            quantity INTEGER NOT NULL DEFAULT 1,
            status TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            processed_at TIMESTAMPTZ,
            audited_at TIMESTAMPTZ
          )`,
        },
        physicalResourceId: cr.PhysicalResourceId.of('OrdersTableInit'),
      },
      policy: cr.AwsCustomResourcePolicy.fromSdkCalls({
        resources: cr.AwsCustomResourcePolicy.ANY_RESOURCE,
      }),
    });
    initSchema.node.addDependency(cluster);

    // =========================================================================
    // Messaging backbone
    //
    //   API --> [SNS Topic] --+--> [Processing Queue] --> processOrder Lambda
    //                         |                                  |
    //                         |                                  v
    //                         |                          [Custom EventBridge Bus]
    //                         |                                  |
    //                         |                                  v
    //                         |                       orderProcessedHandler Lambda
    //                         |
    //                         +--> [Audit Queue] --> auditOrder Lambda
    //
    // SNS fans one "OrderCreated" event out to two independent SQS consumers.
    // EventBridge then lets the processing consumer announce completion to
    // any number of *other* independent consumers, without either side
    // knowing about the other.
    // =========================================================================

    const topic = new sns.Topic(this, 'OrderEventsTopic', {
      topicName: 'order-events-topic',
    });

    // --- Queue 1: order processing (the "main" consumer) ---
    const processingDlq = new sqs.Queue(this, 'OrderProcessingDLQ', {
      queueName: 'order-processing-dlq',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    const processingQueue = new sqs.Queue(this, 'OrderProcessingQueue', {
      queueName: 'order-processing-queue',
      visibilityTimeout: cdk.Duration.seconds(300),
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      deadLetterQueue: { queue: processingDlq, maxReceiveCount: 3 },
    });
    topic.addSubscription(new subscriptions.SqsSubscription(processingQueue));

    // --- Queue 2: audit trail (a second, independent consumer) ---
    const auditDlq = new sqs.Queue(this, 'OrderAuditDLQ', {
      queueName: 'order-audit-dlq',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    const auditQueue = new sqs.Queue(this, 'OrderAuditQueue', {
      queueName: 'order-audit-queue',
      visibilityTimeout: cdk.Duration.seconds(60),
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      deadLetterQueue: { queue: auditDlq, maxReceiveCount: 3 },
    });
    topic.addSubscription(new subscriptions.SqsSubscription(auditQueue));

    // --- Custom EventBridge bus for post-processing events ---
    const eventBus = new events.EventBus(this, 'OrderEventBus', {
      eventBusName: 'order-event-bus',
    });

    // =========================================================================
    // Lambda functions
    // =========================================================================
    const nodeJsDefaults: Partial<lambda.NodejsFunctionProps> = {
      runtime: lambdaBase.Runtime.NODEJS_20_X,
      memorySize: 256,
      timeout: cdk.Duration.seconds(10),
      bundling: { minify: true, sourceMap: true, target: 'node20' },
    };

    const dbEnvironment = {
      CLUSTER_ARN: cluster.clusterArn,
      SECRET_ARN: cluster.secret!.secretArn,
      DATABASE_NAME,
    };

    // --- API handlers ---
    const createOrderFn = new lambda.NodejsFunction(this, 'CreateOrderFunction', {
      ...nodeJsDefaults,
      entry: 'lambda/create-order.ts',
      handler: 'handler',
      environment: { ...dbEnvironment, TOPIC_ARN: topic.topicArn },
    });
    cluster.grantDataApiAccess(createOrderFn);
    topic.grantPublish(createOrderFn);
    createOrderFn.node.addDependency(initSchema);

    const getOrderFn = new lambda.NodejsFunction(this, 'GetOrderFunction', {
      ...nodeJsDefaults,
      entry: 'lambda/get-order.ts',
      handler: 'handler',
      environment: dbEnvironment,
    });
    cluster.grantDataApiAccess(getOrderFn);
    getOrderFn.node.addDependency(initSchema);

    // --- SQS consumers ---
    const processOrderFn = new lambda.NodejsFunction(this, 'ProcessOrderFunction', {
      ...nodeJsDefaults,
      entry: 'lambda/process-order.ts',
      handler: 'handler',
      environment: { ...dbEnvironment, EVENT_BUS_NAME: eventBus.eventBusName },
    });
    cluster.grantDataApiAccess(processOrderFn);
    eventBus.grantPutEventsTo(processOrderFn);
    processOrderFn.node.addDependency(initSchema);
    processOrderFn.addEventSource(
      new SqsEventSource(processingQueue, { batchSize: 5, reportBatchItemFailures: true })
    );

    const auditOrderFn = new lambda.NodejsFunction(this, 'AuditOrderFunction', {
      ...nodeJsDefaults,
      entry: 'lambda/audit-order.ts',
      handler: 'handler',
    });
    auditOrderFn.addEventSource(
      new SqsEventSource(auditQueue, { batchSize: 5, reportBatchItemFailures: true })
    );

    // --- EventBridge consumer ---
    const orderProcessedHandlerFn = new lambda.NodejsFunction(
      this,
      'OrderProcessedHandlerFunction',
      {
        ...nodeJsDefaults,
        entry: 'lambda/order-processed-handler.ts',
        handler: 'handler',
        environment: dbEnvironment,
      }
    );
    cluster.grantDataApiAccess(orderProcessedHandlerFn);
    orderProcessedHandlerFn.node.addDependency(initSchema);

    new events.Rule(this, 'OrderProcessedRule', {
      eventBus,
      ruleName: 'order-processed-rule',
      eventPattern: {
        source: ['custom.orders'],
        detailType: ['OrderProcessed'],
      },
      targets: [new targets.LambdaFunction(orderProcessedHandlerFn)],
    });

    // =========================================================================
    // API Gateway — the only synchronous entry point into the whole pipeline
    // =========================================================================
    const api = new apigateway.RestApi(this, 'OrdersApi', {
      restApiName: 'Orders Service',
      description: 'POST an order; it is processed asynchronously via SNS/SQS/EventBridge',
      deployOptions: { stageName: 'prod' },
    });

    const orders = api.root.addResource('orders');
    orders.addMethod('POST', new apigateway.LambdaIntegration(createOrderFn));

    const order = orders.addResource('{id}');
    order.addMethod('GET', new apigateway.LambdaIntegration(getOrderFn));

    // =========================================================================
    // Outputs
    // =========================================================================
    new cdk.CfnOutput(this, 'ApiUrl', { value: api.url });
    new cdk.CfnOutput(this, 'DbClusterArn', { value: cluster.clusterArn });
    new cdk.CfnOutput(this, 'DbSecretArn', { value: cluster.secret!.secretArn });
    new cdk.CfnOutput(this, 'OrderEventsTopicArn', { value: topic.topicArn });
    new cdk.CfnOutput(this, 'OrderEventBusName', { value: eventBus.eventBusName });
  }
}
