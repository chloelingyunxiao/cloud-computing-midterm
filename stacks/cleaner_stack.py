from aws_cdk import Stack, Duration
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_dynamodb as dynamodb
from constructs import Construct


class CleanerStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *,
                 bucket_dst: s3.Bucket,
                 table: dynamodb.Table,
                 **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        fn = _lambda.Function(self, "Cleaner",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset("lambda/cleaner"),
            timeout=Duration.seconds(30),
            environment={
                "DST_BUCKET": bucket_dst.bucket_name,
                "TABLE_NAME": table.table_name,
                "DISOWN_GRACE_SECONDS": "10",
            },
        )

        # IAM permissions
        bucket_dst.grant_delete(fn)
        table.grant_read_write_data(fn)

        # EventBridge scheduled rule: trigger Cleaner Lambda every 1 minute
        rule = events.Rule(self, "EveryMinute",
            schedule=events.Schedule.rate(Duration.minutes(1)),
        )
        rule.add_target(targets.LambdaFunction(fn))
