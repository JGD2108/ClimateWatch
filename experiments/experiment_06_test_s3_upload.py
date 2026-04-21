"""Minimal S3 connectivity test used before wiring the full Lambda ingestion flow."""

import json
from datetime import datetime

import boto3

BUCKET_NAME = "climatewatch-colombia-dev"
REGION = "us-east-1"

run_timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
year = datetime.now().strftime("%Y")
month = datetime.now().strftime("%m")
day = datetime.now().strftime("%d")

object_key = (
    f"audit/test/year={year}/month={month}/day={day}/"
    f"test_upload_{run_timestamp}.json"
)

payload = {
    "message": "test upload from local python",
    "run_timestamp": run_timestamp,
    "layer": "audit_test"
}

s3 = boto3.client("s3", region_name=REGION)

s3.put_object(
    Bucket=BUCKET_NAME,
    Key=object_key,
    Body=json.dumps(payload, ensure_ascii=False, indent=2),
    ContentType="application/json",
)

print("Upload successful")
print("Bucket:", BUCKET_NAME)
print("Key:", object_key)
