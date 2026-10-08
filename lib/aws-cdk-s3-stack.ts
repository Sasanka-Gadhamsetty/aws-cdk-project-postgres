import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import { Bucket } from "aws-cdk-lib/aws-s3";

export class CdkS3Stack extends cdk.Stack {
    constructor(scope: Construct, id: string, props?: cdk.StackProps) {
        super(scope, id, props);

        // Simple deletable S3 bucket

        const bucket = new Bucket(this, "CdkBucket", {
            bucketName: `cdk-bucket-name-${cdk.Aws.ACCOUNT_ID}`,
            versioned: false,
            removalPolicy: cdk.RemovalPolicy.DESTROY,
            autoDeleteObjects: true,
        });
        
        // Outputs bucket name
        new cdk.CfnOutput(this, "BucketNameOutput", {
            value: bucket.bucketName,
            description: "The name of the s3 bucket",
        });
        // Outputs bucket arn
        new cdk.CfnOutput(this, "BucketArnOutput", {
            value: bucket.bucketArn,
            description: "The arn of the s3 bucket"
        })
    }
}