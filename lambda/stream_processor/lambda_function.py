"""
Stream Processor Lambda
-----------------------
Triggered by the Kinesis Data Stream. For each batch of order events it:

  1. Decodes + parses the records.
  2. Writes the raw batch to the S3 "raw" zone, partitioned by date
     (one object per invocation -> avoids the small-files problem that
     hurts query performance later).
  3. Updates real-time metrics in DynamoDB using atomic counters
     (total revenue + order count per category per day).
  4. Publishes an SNS alert when a single order exceeds a threshold.

Environment variables (set by SAM):
  RAW_BUCKET          S3 bucket for the raw zone
  METRICS_TABLE       DynamoDB table name
  ALERT_TOPIC_ARN     SNS topic ARN for high-value alerts
  HIGH_VALUE_THRESHOLD Order total above which we alert (default 250)

Returns a partial-batch response so Kinesis only retries the records
that actually failed, not the whole batch.
"""

import json
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import boto3

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")

RAW_BUCKET = os.environ["RAW_BUCKET"]
METRICS_TABLE = os.environ["METRICS_TABLE"]
ALERT_TOPIC_ARN = os.environ.get("ALERT_TOPIC_ARN", "")
HIGH_VALUE_THRESHOLD = float(os.environ.get("HIGH_VALUE_THRESHOLD", "250"))

table = dynamodb.Table(METRICS_TABLE)


def _write_raw_to_s3(orders):
    """Write the whole batch as a single newline-delimited JSON object.

    Key layout uses Hive-style partitions (year=/month=/day=) so that
    Glue/Athena can prune partitions efficiently.
    """
    now = datetime.now(timezone.utc)
    key = (
        f"orders/year={now:%Y}/month={now:%m}/day={now:%d}/"
        f"{now:%H%M%S}-{uuid.uuid4().hex[:8]}.json"
    )
    body = "\n".join(json.dumps(o) for o in orders).encode("utf-8")
    s3.put_object(Bucket=RAW_BUCKET, Key=key, Body=body)
    return key


def _update_metrics(order):
    """Atomically increment per-category, per-day counters in DynamoDB.

    PK = CATEGORY#<category>, SK = DATE#<yyyy-mm-dd>
    ADD performs an atomic increment without a read-modify-write race.
    """
    if order.get("order_status") == "CANCELLED":
        return  # don't count cancelled orders toward revenue

    order_date = order["order_timestamp"][:10]  # yyyy-mm-dd
    table.update_item(
        Key={
            "pk": f"CATEGORY#{order['category']}",
            "sk": f"DATE#{order_date}",
        },
        UpdateExpression="ADD order_count :one, revenue :amt",
        ExpressionAttributeValues={
            ":one": 1,
            # DynamoDB needs Decimal, not float.
            ":amt": Decimal(str(order["total_amount"])),
        },
    )


def _maybe_alert(order):
    """Send an SNS notification for high-value, non-cancelled orders."""
    if not ALERT_TOPIC_ARN:
        return
    if order.get("order_status") == "CANCELLED":
        return
    if float(order["total_amount"]) <= HIGH_VALUE_THRESHOLD:
        return

    sns.publish(
        TopicArn=ALERT_TOPIC_ARN,
        Subject="High-value order alert",
        Message=(
            f"High-value order detected\n"
            f"Order ID: {order['order_id']}\n"
            f"Customer: {order['customer_id']}\n"
            f"Product:  {order['product_name']} ({order['category']})\n"
            f"Amount:   ${order['total_amount']} {order.get('currency', 'USD')}\n"
            f"City:     {order['shipping_city']}, {order['shipping_state']}\n"
            f"Time:     {order['order_timestamp']}"
        ),
    )


def handler(event, context):
    records = event.get("Records", [])
    parsed_orders = []
    failures = []

    # ---- Parse phase ------------------------------------------------
    for record in records:
        record_id = record["kinesis"]["sequenceNumber"]
        try:
            import base64
            raw = base64.b64decode(record["kinesis"]["data"])
            order = json.loads(raw)
            parsed_orders.append((record_id, order))
        except Exception as exc:  # malformed record
            print(f"Failed to parse record {record_id}: {exc}")
            failures.append({"itemIdentifier": record_id})

    # ---- Write raw batch to S3 (one object) -------------------------
    if parsed_orders:
        try:
            key = _write_raw_to_s3([o for _, o in parsed_orders])
            print(f"Wrote {len(parsed_orders)} records to s3://{RAW_BUCKET}/{key}")
        except Exception as exc:
            # If the S3 write fails, retry the whole batch.
            print(f"S3 write failed, retrying batch: {exc}")
            return {"batchItemFailures": [{"itemIdentifier": rid}
                                          for rid, _ in parsed_orders]}

    # ---- Per-record side effects (metrics + alerts) -----------------
    for record_id, order in parsed_orders:
        try:
            _update_metrics(order)
            _maybe_alert(order)
        except Exception as exc:
            print(f"Side-effect failed for {record_id}: {exc}")
            failures.append({"itemIdentifier": record_id})

    print(f"Processed {len(parsed_orders)} orders, {len(failures)} failures")
    # Kinesis will retry only these record IDs.
    return {"batchItemFailures": failures}
