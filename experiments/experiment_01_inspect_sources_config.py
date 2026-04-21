"""Prototype used to validate that the YAML configuration could be read correctly."""

import yaml
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPO_ROOT / "config" / "sources.yaml"

with CONFIG_PATH.open("r", encoding="utf-8") as f:
    yaml_data = yaml.safe_load(f)
    
print(yaml_data)

cities = yaml_data["cities"]
print(cities)


for city in yaml_data["cities"]:
    name = city['name']
    lat  = city['latitude']
    lon  = city['longitude']
    print(name, lat, lon)

weather = yaml_data["weather"]
data_type    = weather["data_type"]
variables = weather["variables"]
timezone = weather["timezone"]
forecast = weather["forecast_days"]

print(data_type, variables, timezone, forecast)

exec_config = yaml_data["execution"]
run_times = exec_config["run_times"]


