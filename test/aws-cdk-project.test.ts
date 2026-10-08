import * as cdk from 'aws-cdk-lib/core';
import { Template } from 'aws-cdk-lib/assertions';
import { AwsCdkProjectStack } from '../lib/aws-cdk-project-stack';

describe('AwsCdkProjectStack', () => {
  const app = new cdk.App();
  const stack = new AwsCdkProjectStack(app, 'MyTestStack');
  const template = Template.fromStack(stack);

  test('SQS processing queue created with expected visibility timeout', () => {
    template.hasResourceProperties('AWS::SQS::Queue', {
      QueueName: 'order-processing-queue',
      VisibilityTimeout: 300,
    });
  });

  test('two queues are subscribed to the SNS topic (fan-out)', () => {
    template.resourceCountIs('AWS::SNS::Subscription', 2);
    template.hasResourceProperties('AWS::SNS::Topic', {
      TopicName: 'order-events-topic',
    });
  });

  test('a custom EventBridge bus and rule are created', () => {
    template.hasResourceProperties('AWS::Events::EventBus', {
      Name: 'order-event-bus',
    });
    template.hasResourceProperties('AWS::Events::Rule', {
      EventPattern: {
        source: ['custom.orders'],
        'detail-type': ['OrderProcessed'],
      },
    });
  });

  test('five app Lambda functions are created (plus the CDK custom-resource provider)', () => {
    // 5 of ours (create/get/process/audit-order + order-processed-handler) +
    // 1 singleton provider Lambda that CDK generates to back the
    // AwsCustomResource used for the schema-init call.
    template.resourceCountIs('AWS::Lambda::Function', 6);
  });

  test('API Gateway exposes POST /orders and GET /orders/{id}', () => {
    template.hasResourceProperties('AWS::ApiGateway::RestApi', {
      Name: 'Orders Service',
    });
    template.resourceCountIs('AWS::ApiGateway::Method', 2);
  });

  test('an Aurora Serverless v2 Postgres cluster is created with Data API enabled', () => {
    template.hasResourceProperties('AWS::RDS::DBCluster', {
      Engine: 'aurora-postgresql',
      EnableHttpEndpoint: true,
      DatabaseName: 'orders',
    });
  });

  test('a custom resource bootstraps the orders table schema', () => {
    template.resourceCountIs('Custom::AWS', 1);
    // The Create call is serialized as an Fn::Join, so just confirm the SQL
    // text appears somewhere in one of its literal string fragments.
    const resources = template.findResources('Custom::AWS');
    const [resource] = Object.values(resources);
    const joinParts: string[] = resource.Properties.Create['Fn::Join'][1];
    const containsCreateTable = joinParts.some(
      (part) => typeof part === 'string' && part.includes('CREATE TABLE IF NOT EXISTS orders')
    );
    expect(containsCreateTable).toBe(true);
  });
});
