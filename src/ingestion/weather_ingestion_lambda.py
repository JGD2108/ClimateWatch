"""AWS Lambda entry point for the ClimateWatch Colombia ingestion pipeline."""

import json
import os
from datetime import datetime
from pathlib import Path

import boto3
import requests
import yaml

MODULE_DIR = Path(__file__).resolve().parent

BUCKET_NAME = os.environ["S3_BUCKET"]
REGION = os.environ.get("AWS_REGION", "us-east-1")

s3 = boto3.client("s3", region_name=REGION)


def resolve_config_path() -> Path:
    config_path_env = os.environ.get("CONFIG_PATH")
    if config_path_env:
        config_path = Path(config_path_env).expanduser().resolve()
        if not config_path.exists():
            raise FileNotFoundError(f"CONFIG_PATH does not exist: {config_path}")
        return config_path

    # Prefer a Lambda-friendly layout where the YAML travels with the code package.
    candidates = (
        MODULE_DIR / "sources.yaml",
        MODULE_DIR / "config" / "sources.yaml",
        MODULE_DIR.parents[1] / "config" / "sources.yaml",
    )

    for candidate in candidates:
        if candidate.exists():
            return candidate

    searched_paths = ", ".join(str(path) for path in candidates)
    raise FileNotFoundError(
        "Could not find sources.yaml. Checked: "
        f"{searched_paths}. Set CONFIG_PATH or package the YAML with the Lambda code."
    )


CONFIG_PATH = resolve_config_path()



def load_config():
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ingest_city(city, weather_config, url):
    variables = weather_config["variables"]
    timezone = weather_config["timezone"]
    forecast_days = weather_config["forecast_days"]

    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "hourly": ",".join(variables),
        "timezone": timezone,
        "forecast_days": forecast_days,
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()

    data = response.json()
    hourly_data = data["hourly"]
    times = hourly_data["time"]
    temperatures = hourly_data["temperature_2m"]
    same_length = len(times) == len(temperatures)

    today = datetime.today()
    year = today.year
    month = f"{today.month:02d}"
    day = f"{today.day:02d}"

    city_name = city["name"].strip()
    city_slug = "".join(
        ch.lower() if ch.isalnum() else "-" for ch in city_name
    ).strip("-")
    timestamp = today.strftime("%Y%m%dT%H%M%S")

    raw_key = (
        f"raw/weather/year={year}/month={month}/day={day}/"
        f"weather_{city_slug}_{timestamp}.json"
    )

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=raw_key,
        Body=json.dumps(data, ensure_ascii=False, indent=2),
        ContentType="application/json",
    )

    return {
        "status": "success",
        "city": city_name,
        "bucket": BUCKET_NAME,
        "output_key": raw_key,
        "records": len(times),
        "same_length": same_length,
    }


def save_audit_report(run_timestamp, source, results):
    today = datetime.today()
    year = today.year
    month = f"{today.month:02d}"
    day = f"{today.day:02d}"

    audit_key = (
        f"audit/weather/year={year}/month={month}/day={day}/"
        f"weather_run_{run_timestamp}.json"
    )

    audit_payload = {
        "run_timestamp": run_timestamp,
        "source": source,
        "cities_processed": len(results),
        "results": results,
    }

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=audit_key,
        Body=json.dumps(audit_payload, ensure_ascii=False, indent=2),
        ContentType="application/json",
    )

    return audit_key


def run_ingestion():
    yaml_data = load_config()
    cities = yaml_data["cities"]
    weather = yaml_data["weather"]
    url = yaml_data["url"]["weather"][0]
    run_timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    run_results = []

    for city in cities:
        try:
            result = ingest_city(city, weather, url)
        except Exception as exc:
            result = {
                "status": "error",
                "city": city["name"].strip(),
                "bucket": BUCKET_NAME,
                "output_key": None,
                "records": None,
                "same_length": None,
                "error": str(exc),
            }

        run_results.append(result)

    audit_key = save_audit_report(run_timestamp, url, run_results)
    cities_attempted = len(run_results)
    cities_succeeded = sum(1 for result in run_results if result["status"] == "success")
    cities_failed = sum(1 for result in run_results if result["status"] == "error")

    return {
        "run_timestamp": run_timestamp,
        "bucket": BUCKET_NAME,
        "cities_attempted": cities_attempted,
        "cities_succeeded": cities_succeeded,
        "cities_failed": cities_failed,
        "audit_key": audit_key,
    }


def main():
    summary = run_ingestion()
    print(f"Run timestamp: {summary['run_timestamp']}")
    print(f"Bucket: {summary['bucket']}")
    print(f"Cities attempted: {summary['cities_attempted']}")
    print(f"Cities succeeded: {summary['cities_succeeded']}")
    print(f"Cities failed: {summary['cities_failed']}")
    print(f"Audit key: {summary['audit_key']}")
    return summary


def lambda_handler(event, context):
    return run_ingestion()


if __name__ == "__main__":
    main()
