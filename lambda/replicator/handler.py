import os
import uuid
import urllib.parse
import boto3
from datetime import datetime, timezone
from boto3.dynamodb.conditions import Key

s3 = boto3.client("s3")
ddb = boto3.resource("dynamodb")

DST_BUCKET = os.environ["DST_BUCKET"]
TABLE_NAME = os.environ["TABLE_NAME"]
MAX_COPIES = int(os.environ.get("MAX_COPIES", "3"))

table = ddb.Table(TABLE_NAME)


def lambda_handler(event, context):
    # EventBridge S3 event format (different from direct S3 notification format):
    # event["detail-type"] = "Object Created" or "Object Deleted"
    # event["detail"]["bucket"]["name"] = bucket name
    # event["detail"]["object"]["key"]  = URL-encoded object key
    detail_type = event["detail-type"]
    src_key = urllib.parse.unquote_plus(event["detail"]["object"]["key"])
    src_bucket = event["detail"]["bucket"]["name"]

    print(f"Event: {detail_type}, Key: {src_key}")

    if detail_type == "Object Created":
        put_into_src_bucket(src_bucket, src_key)
    elif detail_type == "Object Deleted":
        delete_from_src_bucket(src_key)


def put_into_src_bucket(src_bucket: str, src_key: str):
    """
    DynamoDB record structure:
        - PK: "SRC#{src_key}"
        - SK: "COPY#{timestamp}#{copy_id}"
        - copy_key: the S3 key of the copy in the Dst bucket
        - status: "ACTIVE" or "DISOWNED"
        - created_at: when the copy was created
        - disowned_at: when the copy was disowned (only for DISOWNED status)
        - deleted_at: when the copy was deleted (only for DELETED status, set by Cleaner)

    S3 structure in Src bucket:
        - Key: "{src_key}"
    S3 structure in Dst bucket:
        - Key: "{src_key}__{timestamp}__{copy_id}"

    This function handles:
    1. Copying the new object from Src to Dst bucket with a unique name.
    2. Writing a new record to DynamoDB for the copy.
    3. Querying all copies for this src_key and deleting the oldest if we exceed MAX_COPIES.
    """

    # Generate a unique copy name using timestamp + short uuid
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    copy_id = uuid.uuid4().hex[:8]
    copy_key = f"{src_key}__{timestamp}__{copy_id}"
    # Create PK and SK
    pk = f"SRC#{src_key}"
    sk = f"COPY#{timestamp}#{copy_id}"

    # Copy object from Src to Dst bucket
    s3.copy_object(
        CopySource={"Bucket": src_bucket, "Key": src_key},
        Bucket=DST_BUCKET,
        Key=copy_key,
    )
    print(f"Copied {src_key} -> {copy_key}")

    # Write the new record to DynamoDB
    table.put_item(
        Item={
            "PK": pk,
            "SK": sk,
            "copy_key": copy_key,
            "status": "ACTIVE",
            "created_at": timestamp,
        }
    )

    # sorted oldest-first (ScanIndexForward=True) so we can easily find the oldest copy to delete if we exceed MAX_COPIES
    response = table.query(
        KeyConditionExpression=Key("PK").eq(pk),
        ScanIndexForward=True,
    )
    items = response["Items"]

    while len(items) > MAX_COPIES:
        oldest = items.pop(0)
        s3.delete_object(Bucket=DST_BUCKET, Key=oldest["copy_key"])
        table.delete_item(Key={"PK": oldest["PK"], "SK": oldest["SK"]})
        print(f"Deleted oldest copy: {oldest['copy_key']}")


def delete_from_src_bucket(src_key: str):
    """
    Delete all copies of an object from the source bucket and mark them as disowned in DynamoDB.
    However, the copies are not immediately deleted from the destination bucket. Instead, they are marked as "DISOWNED"
    in DynamoDB with a disowned_at timestamp. The Cleaner Lambda will later query for DISOWNED copies that have been disowned
    for more than a certain grace period and delete them from the dst bucket.

    In the mean time, the real cleanup logic is in the Cleaner Lambda to avoid a long-running delete operation in this function.
    """

    pk = f"SRC#{src_key}"
    disowned_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    # Query all copies for this object
    response = table.query(
        KeyConditionExpression=Key("PK").eq(pk),
    )
    items = response["Items"]

    if not items:
        print(f"No copies found for {src_key}, nothing to disown.")
        return

    # update each copy's status to DISOWNED and set disowned_at timestamp. Cleaner will handle the actual deletion after the grace period.
    for item in items:
        table.update_item(
            Key={"PK": item["PK"], "SK": item["SK"]},
            UpdateExpression="SET #s = :disowned, disowned_at = :ts",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":disowned": "DISOWNED",  # use GSI on status + disowned_at to efficiently query for disowned copies in Cleaner
                ":ts": disowned_at,
            },
        )
        print(f"Disowned copy: {item['copy_key']}")
