from aws_cdk import Stack, Duration
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_dynamodb as dynamodb
from constructs import Construct


class ReplicatorStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *,
                 bucket_src: s3.Bucket,
                 bucket_dst: s3.Bucket,
                 table: dynamodb.Table,
                 **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        fn = _lambda.Function(self, "Replicator",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset("lambda/replicator"),
            timeout=Duration.seconds(30),
            environment={
                "DST_BUCKET": bucket_dst.bucket_name,
                "TABLE_NAME": table.table_name,
                "MAX_COPIES": "3",
            },
        )

        # IAM permissions
        bucket_src.grant_read(fn)
        bucket_dst.grant_read_write(fn)
        table.grant_read_write_data(fn)

        # Use EventBridge instead of direct S3 notifications to avoid cross-stack
        # cyclic dependency (bucket is in StorageStack, Lambda is in ReplicatorStack)
        rule = events.Rule(self, "S3SrcEvents",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created", "Object Deleted"],
                detail={
                    "bucket": {
                        "name": [bucket_src.bucket_name]
                    }
                }
            )
        )
        rule.add_target(targets.LambdaFunction(fn))
