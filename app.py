import aws_cdk as cdk
from stacks.storage_stack import StorageStack
from stacks.replicator_stack import ReplicatorStack
from stacks.cleaner_stack import CleanerStack

app = cdk.App()

# Initialize the stacks
storage = StorageStack(app, "StorageStack")

ReplicatorStack(
    app,
    "ReplicatorStack",
    bucket_src=storage.bucket_src,
    bucket_dst=storage.bucket_dst,
    table=storage.table,
)

CleanerStack(
    app,
    "CleanerStack",
    bucket_dst=storage.bucket_dst,
    table=storage.table,
)

app.synth()
