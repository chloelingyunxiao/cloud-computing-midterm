import os
import boto3
from datetime import datetime, timezone, timedelta
from boto3.dynamodb.conditions import Key

s3 = boto3.client("s3")
ddb = boto3.resource("dynamodb")

DST_BUCKET = os.environ["DST_BUCKET"]
TABLE_NAME = os.environ["TABLE_NAME"]
GRACE_SECONDS = int(os.environ.get("DISOWN_GRACE_SECONDS", "10"))

table = ddb.Table(TABLE_NAME)


def lambda_handler(event, context):
    # compute cutoff: only delete copies disowned more than GRACE_SECONDS ago
    cutoff_dt = datetime.now(timezone.utc) - timedelta(seconds=GRACE_SECONDS)
    cutoff_str = cutoff_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    print(f"Cleaner running. Cutoff: {cutoff_str}")

    # query DisownedIndex GSI: status="DISOWNED" AND disowned_at < cutoff
    items = []
    kwargs = {
        "IndexName": "DisownedIndex",
        "KeyConditionExpression": Key("status").eq("DISOWNED")
        & Key("disowned_at").lt(cutoff_str),
    }

    while True:
        response = table.query(**kwargs)
        items.extend(response["Items"])
        # Handle DynamoDB pagination
        if "LastEvaluatedKey" not in response:
            break
        kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]

    print(f"Found {len(items)} disowned copies to delete.")

    # delete each copy from Dst bucket, then update DynamoDB
    for item in items:
        copy_key = item["copy_key"]

        # delete from S3
        try:
            s3.delete_object(Bucket=DST_BUCKET, Key=copy_key)
            print(f"Deleted from S3: {copy_key}")
        except Exception as e:
            # If S3 deletion fails, skip DynamoDB update so we retry next minute
            print(f"ERROR deleting {copy_key} from S3: {e}")
            continue

        # update status to DELETED and remove disowned_at so the item
        # no longer appears in the DisownedIndex GSI on future Cleaner runs
        deleted_at = (
            datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        )
        table.update_item(
            Key={"PK": item["PK"], "SK": item["SK"]},
            UpdateExpression="SET #s = :deleted, deleted_at = :now REMOVE disowned_at",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":deleted": "DELETED",
                ":now": deleted_at,
            },
        )
        print(f"Marked DELETED in DynamoDB: PK={item['PK']}, SK={item['SK']}")
