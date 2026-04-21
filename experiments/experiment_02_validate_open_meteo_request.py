"""Prototype used to validate Open-Meteo request parameters and response shape."""

import requests
import yaml
from pathlib import Path

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

print(f"City: {city['name']}")
print(f"Status code: {response.status_code}")
print(f"Request URL: {response.url}")
print(f"Main JSON keys: {list(data.keys())}")
print(f"Hourly keys: {list(hourly_data.keys())}")
print(f"len(hourly['time']): {len(times)}")
print(f"len(hourly['temperature_2m']): {len(temperatures)}")
print(f"First 3 hourly timestamps: {times[:3]}")
print(f"First 3 temperatures: {temperatures[:3]}")
print(f"Integrity check (same length): {same_length}")
