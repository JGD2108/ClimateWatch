import csv
import io
import json
import os
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

BUCKET_NAME = os.environ["S3_BUCKET"]
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw/weather/")
CURATED_PREFIX = os.environ.get("CURATED_PREFIX", "curated/weather/")
CURATED_AUDIT_PREFIX = os.environ.get("CURATED_AUDIT_PREFIX", "audit/curated_weather/")

s3 = boto3.client("s3")

EXPECTED_HOURLY_COLUMNS = {
    "time",
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation_probability",
    "cloud_cover",
    "wind_speed_10m",
}

def get_all_raw_keys():
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=RAW_PREFIX):
        for obj in page.get("Contents",[]):
            key = obj["Key"]
            if key.endswith(".json"):
                keys.append(key)
    return sorted(keys)

def parse_weather_file_name_from_key(raw_key):
    # 1) get only the file name
    filename = raw_key.split("/")[-1]

    # 2) basic checks
    if not filename.endswith(".json"):
        raise ValueError("File must end with .json")
    if not filename.startswith("weather_"):
        raise ValueError("File must start with weather_")

    # 3) remove prefix and extension
    #    weather_bogota_20260421T192627.json -> bogota_20260421T192627
    core = filename[len("weather_"):-len(".json")]

    # 4) split from the RIGHT once:
    #    city_slug can contain underscores, timestamp is last part
    parts = core.rsplit("_", 1)
    if len(parts) != 2:
        raise ValueError("Invalid file name format")

    city_slug, timestamp = parts
    return city_slug, timestamp


def get_curated_output_key(raw_key):
    if not raw_key.startswith(RAW_PREFIX):
        raise ValueError(f"raw_key must start with '{RAW_PREFIX}'")

    relative = raw_key[len(RAW_PREFIX):]
    parts = relative.split("/")
    if len(parts) < 4:
        raise ValueError("Invalid raw key format")

    partition_path = "/".join(parts[:-1])
    city_slug, timestamp = parse_weather_file_name_from_key(raw_key)

    curated_filename = f"weather_curated_{city_slug}_{timestamp}.csv"
    curated_key = f"{CURATED_PREFIX}{partition_path}/{curated_filename}"

    return city_slug, curated_key

def s3_key_exists(key):
    try:
        s3.head_object(Bucket=BUCKET_NAME, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def transform_raw_object(raw_key, curated_key):
    city_slug, ingestion_timestamp_raw = parse_weather_file_name_from_key(raw_key)

    raw_response = s3.get_object(Bucket=BUCKET_NAME, Key=raw_key)
    raw_body_bytes = raw_response["Body"].read()
    raw_payload = json.loads(raw_body_bytes)

    hourly_data = raw_payload.get("hourly")
    if hourly_data is None:
        raise ValueError("Missing 'hourly' block in raw payload")

    missing_columns = EXPECTED_HOURLY_COLUMNS - set(hourly_data.keys())
    if missing_columns:
        raise ValueError(f"Missing expected hourly columns: {sorted(missing_columns)}")

    ingestion_timestamp = datetime.strptime(
        ingestion_timestamp_raw, "%Y%m%dT%H%M%S"
    ).isoformat()

    csv_columns = [
        "city",
        "ingestion_timestamp",
        "time",
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation_probability",
        "cloud_cover",
        "wind_speed_10m",
    ]

    row_count = len(hourly_data["time"])
    for column in EXPECTED_HOURLY_COLUMNS:
        if len(hourly_data[column]) != row_count:
            raise ValueError(f"Length mismatch in column '{column}'")

    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=csv_columns)
    writer.writeheader()

    for i in range(row_count):
        writer.writerow(
            {
                "city": city_slug,
                "ingestion_timestamp": ingestion_timestamp,
                "time": hourly_data["time"][i],
                "temperature_2m": hourly_data["temperature_2m"][i],
                "relative_humidity_2m": hourly_data["relative_humidity_2m"][i],
                "precipitation_probability": hourly_data["precipitation_probability"][i],
                "cloud_cover": hourly_data["cloud_cover"][i],
                "wind_speed_10m": hourly_data["wind_speed_10m"][i],
            }
        )

    csv_body = csv_buffer.getvalue()
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=curated_key,
        Body=csv_body.encode("utf-8"),
        ContentType="text/csv",
    )

    return {
        "status": "transformed",
        "city": city_slug,
        "raw_key": raw_key,
        "curated_key": curated_key,
        "rows_written": row_count,
    }


def save_transform_audit_report(run_timestamp, raw_files_found, results):
    run_dt = datetime.strptime(run_timestamp, "%Y%m%dT%H%M%S")
    year = run_dt.year
    month = f"{run_dt.month:02d}"
    day = f"{run_dt.day:02d}"

    audit_prefix = CURATED_AUDIT_PREFIX
    if not audit_prefix.endswith("/"):
        audit_prefix = f"{audit_prefix}/"

    audit_key = (
        f"{audit_prefix}year={year}/month={month}/day={day}/"
        f"curated_weather_run_{run_timestamp}.json"
    )

    audit_payload = {
        "run_timestamp": run_timestamp,
        "raw_files_found": raw_files_found,
        "files_attempted": len(results),
        "files_transformed": sum(1 for r in results if r.get("status") == "transformed"),
        "files_skipped": sum(1 for r in results if r.get("status") == "skipped"),
        "files_failed": sum(1 for r in results if r.get("status") == "error"),
        "results": results,
    }

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=audit_key,
        Body=json.dumps(audit_payload, ensure_ascii=False, indent=2),
        ContentType="application/json",
    )

    return audit_key


def run_transformation():
    raw_keys = get_all_raw_keys()
    run_timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    results = []

    for raw_key in raw_keys:
        city_slug = "unknown"
        curated_key = None

        try:
            city_slug, curated_key = get_curated_output_key(raw_key)

            if s3_key_exists(curated_key):
                result = {
                    "status": "skipped",
                    "city": city_slug,
                    "raw_key": raw_key,
                    "curated_key": curated_key,
                    "rows_written": 0,
                }
            else:
                result = transform_raw_object(raw_key, curated_key)

        except Exception as exc:
            result = {
                "status": "error",
                "city": city_slug,
                "raw_key": raw_key,
                "curated_key": curated_key,
                "rows_written": None,
                "error": str(exc),
            }

        results.append(result)

    raw_files_found = len(raw_keys)
    files_transformed = sum(1 for result in results if result["status"] == "transformed")
    files_skipped = sum(1 for result in results if result["status"] == "skipped")
    files_failed = sum(1 for result in results if result["status"] == "error")

    audit_key = save_transform_audit_report(run_timestamp, raw_files_found, results)

    return {
        "run_timestamp": run_timestamp,
        "bucket": BUCKET_NAME,
        "raw_files_found": raw_files_found,
        "files_transformed": files_transformed,
        "files_skipped": files_skipped,
        "files_failed": files_failed,
        "audit_key": audit_key,
    }


def main():
    summary = run_transformation()
    print(f"Run timestamp: {summary['run_timestamp']}")
    print(f"Bucket: {summary['bucket']}")
    print(f"Raw files found: {summary['raw_files_found']}")
    print(f"Files transformed: {summary['files_transformed']}")
    print(f"Files skipped: {summary['files_skipped']}")
    print(f"Files failed: {summary['files_failed']}")
    print(f"Audit key: {summary['audit_key']}")
    return summary


def lambda_handler(event, context):
    return run_transformation()


if __name__ == "__main__":
    main()
