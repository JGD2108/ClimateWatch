"""Prototype that saves one city's weather response as a local raw JSON file."""

import requests
import yaml
from datetime import datetime
from pathlib import Path
import json

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPO_ROOT / "config" / "sources.yaml"

with CONFIG_PATH.open("r", encoding="utf-8") as f:
    yaml_data = yaml.safe_load(f)


cities = yaml_data["cities"]

weather = yaml_data["weather"]
variables = weather["variables"]
timezone = weather["timezone"]
forecast_days = weather["forecast_days"]

city = cities[0]

params = {
    "latitude": city["latitude"],
    "longitude": city["longitude"],
    "hourly": ",".join(variables),
    "timezone": timezone,
    "forecast_days": forecast_days,
}

url = yaml_data["url"]["weather"][0]

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
    REPO_ROOT
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

