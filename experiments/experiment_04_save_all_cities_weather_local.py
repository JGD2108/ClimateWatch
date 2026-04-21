"""Prototype that ingests all configured cities and stores raw JSON locally."""

import json
from datetime import datetime
from pathlib import Path

import requests
import yaml



REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPO_ROOT / "config" / "sources.yaml"


def load_config():
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ingest_city(city, weather_config, url, repo_root):
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

    output_dir = (
        repo_root
        / "data"
        / "raw"
        / "weather"
        / f"year={year}"
        / f"month={month}"
        / f"day={day}"
    )

    city_name = city["name"].strip()
    city_slug = "".join(
        ch.lower() if ch.isalnum() else "-" for ch in city_name
    ).strip("-")
    timestamp = today.strftime("%Y%m%dT%H%M%S")

    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"weather_{city_slug}_{timestamp}.json"

    with output_file.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return {
        "status": "success",
        "city": city_name,
        "output_file": str(output_file),
        "records": len(times),
        "same_length": same_length,
    }


def save_audit_report(run_timestamp, source, results, repo_root):
    today = datetime.today()
    year = today.year
    month = f"{today.month:02d}"
    day = f"{today.day:02d}"
    
    audit_dir = repo_root / "data" / "audit" / "weather" / f"year={year}" / f"month={month}" / f"day={day}"
    audit_dir.mkdir(parents=True, exist_ok=True)

    audit_file = audit_dir / f"weather_run_{run_timestamp}.json"
    audit_payload = {
        "run_timestamp": run_timestamp,
        "source": source,
        "cities_processed": len(results),
        "results": results,
    }

    with audit_file.open("w", encoding="utf-8") as f:
        json.dump(audit_payload, f, ensure_ascii=False, indent=2)

    return audit_file


def main():
    yaml_data = load_config()
    cities = yaml_data["cities"]
    weather = yaml_data["weather"]
    url = yaml_data["url"]["weather"][0]
    run_timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    run_results = []

    for city in cities:
        try:
            result = ingest_city(city, weather, url, REPO_ROOT)
        except Exception as exc:
            result = {
                "status": "error",
                "city": city["name"].strip(),
                "output_file": None,
                "records": None,
                "same_length": None,
                "error": str(exc),
            }

        run_results.append(result)
        print(f"Status: {result['status']}")
        print(f"City processed: {result['city']}")
        print(f"Output file: {result['output_file']}")
        print(f"Hourly records: {result['records']}")
        print(f"Integrity check: {result['same_length']}")

        if result["status"] == "error":
            print(f"Error: {result['error']}")

    audit_file = save_audit_report(run_timestamp, url, run_results, REPO_ROOT)
    print(f"Audit file: {audit_file}")


if __name__ == "__main__":
    main()
