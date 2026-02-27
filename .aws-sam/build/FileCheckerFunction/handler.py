import boto3
import json
from datetime import datetime, timezone

s3 = boto3.client("s3")

# 10 MB in bytes
MAX_SIZE_BYTES = 10 * 1024 * 1024

# S3 Standard price: $0.023 per GB per month
PRICE_PER_GB_MONTH = 0.023


def lambda_handler(event, context):
    """
    Triggered whenever a file is uploaded to the S3 bucket.
    - Deletes the file if it's larger than 10 MB.
    - Tags the file (owner, ttl_days, uploaded_at, size_bytes) if it's 10 MB or smaller.
    - Logs an estimated monthly storage cost either way.
    """
    # S3 sends one record per uploaded object (usually just one)
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        # Keys can have URL-encoded characters (e.g. spaces become '+'), decode them
        key = record["s3"]["object"]["key"].replace("+", " ")
        from urllib.parse import unquote_plus
        key = unquote_plus(record["s3"]["object"]["key"])

        # --- 1. Get the object's size via a HEAD request ---
        head = s3.head_object(Bucket=bucket, Key=key)
        size_bytes = head["ContentLength"]

        # --- 4. Calculate estimated monthly storage cost ---
        size_gb = size_bytes / (1024 ** 3)
        monthly_cost_usd = size_gb * PRICE_PER_GB_MONTH

        # --- 2. Delete if larger than 10 MB ---
        if size_bytes > MAX_SIZE_BYTES:
            s3.delete_object(Bucket=bucket, Key=key)
            log({
                "action": "deleted",
                "bucket": bucket,
                "key": key,
                "size_bytes": size_bytes,
                "reason": "exceeds 10 MB limit",
                "estimated_monthly_cost_usd": round(monthly_cost_usd, 6),
            })
            continue  # move on to the next record

        # --- 3. Tag the object if 10 MB or smaller ---
        uploaded_at = datetime.now(timezone.utc).isoformat()
        s3.put_object_tagging(
            Bucket=bucket,
            Key=key,
            Tagging={
                "TagSet": [
                    {"Key": "owner",       "Value": "unknown"},
                    {"Key": "ttl_days",    "Value": "7"},
                    {"Key": "uploaded_at", "Value": uploaded_at},
                    {"Key": "size_bytes",  "Value": str(size_bytes)},
                ]
            },
        )

        log({
            "action": "tagged",
            "bucket": bucket,
            "key": key,
            "size_bytes": size_bytes,
            "tags": {
                "owner": "unknown",
                "ttl_days": "7",
                "uploaded_at": uploaded_at,
                "size_bytes": str(size_bytes),
            },
            "estimated_monthly_cost_usd": round(monthly_cost_usd, 6),
        })


def log(data: dict):
    """Print a readable JSON log line to CloudWatch."""
    print(json.dumps(data, indent=2))

