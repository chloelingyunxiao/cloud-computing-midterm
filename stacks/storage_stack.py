from aws_cdk import Stack, RemovalPolicy
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_dynamodb as dynamodb
from constructs import Construct


class StorageStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # create Src and Dst buckets for backup
        # event_bridge_enabled=True allows ReplicatorStack to listen to S3 events
        # via EventBridge rules without creating a cross-stack cyclic dependency
        self.bucket_src = s3.Bucket(
            self,
            "BucketSrc",
            event_bridge_enabled=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        self.bucket_dst = s3.Bucket(
            self,
            "BucketDst",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # create a DynamoDB table to track copies and their status
        self.table = dynamodb.Table(
            self,
            "TableT",
            partition_key=dynamodb.Attribute(
                name="PK",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="SK",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # Since it is not accept to scan DynamoDB tables, we need to create a global secondary index for Cleaner to query 'disowned copies' by disowned_at time.
        # GSI: PK = status ("DISOWNED"), SK = disowned_at (ISO timestamp, sortable)
        self.table.add_global_secondary_index(
            index_name="DisownedIndex",
            partition_key=dynamodb.Attribute(
                name="status",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="disowned_at",
                type=dynamodb.AttributeType.STRING,
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )
