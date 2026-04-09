import argparse
import json
import re
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import yaml

SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from utils.run_logging import build_start_time, get_logger, log_run_end, log_run_start


@dataclass(frozen=True)
class WeatherSourceConfig:
    source_name: str
    base_url: str
    raw_directory: Path
    field_parameter: str
    fields_to_request: list[str]
    recommended_parameters: dict[str, Any]
    timeout_seconds: int = 60


@dataclass(frozen=True)
class WeatherRequest:
    latitude: float
    longitude: float
    start_date: str
    end_date: str
    station_id: str | None = None


class WeatherFetcher:
    def __init__(self, config_path: Path | None = None) -> None:
        self.repo_root = Path(__file__).resolve().parents[2]
        self.config_path = config_path or self.repo_root / "config" / "sources.yaml"
        self.source_config = self._load_source_config()
        self.logger = get_logger("fetch_weather")

    def _load_source_config(self) -> WeatherSourceConfig:
        with self.config_path.open("r", encoding="utf-8") as file:
            config = yaml.safe_load(file)

        weather_cfg = config["sources"]["weather_archive"]
        raw_directory = self.repo_root / weather_cfg["raw_directory"]

        return WeatherSourceConfig(
            source_name=weather_cfg["source_name"],
            base_url=weather_cfg["base_url"],
            raw_directory=raw_directory,
            field_parameter=weather_cfg["field_parameter"],
            fields_to_request=weather_cfg["fields_to_request"],
            recommended_parameters=weather_cfg.get("recommended_parameters", {}),
        )

    def _build_request_params(self, request_data: WeatherRequest) -> dict[str, Any]:
        api_fields = [
            field
            for field in self.source_config.fields_to_request
            if field != "time"
        ]

        return {
            "latitude": request_data.latitude,
            "longitude": request_data.longitude,
            "start_date": request_data.start_date,
            "end_date": request_data.end_date,
            self.source_config.field_parameter: ",".join(api_fields),
            **self.source_config.recommended_parameters,
        }

    def fetch_weather(self, request_data: WeatherRequest) -> dict[str, Any]:
        params = self._build_request_params(request_data)
        response = requests.get(
            self.source_config.base_url,
            params=params,
            timeout=self.source_config.timeout_seconds,
        )
        response.raise_for_status()
        return response.json()

    def _build_file_stem(self, request_data: WeatherRequest) -> str:
        if request_data.station_id:
            clean_station_id = re.sub(r"[^A-Za-z0-9_-]", "_", request_data.station_id)
            return clean_station_id

        latitude_part = str(request_data.latitude).replace(".", "_").replace("-", "m")
        longitude_part = str(request_data.longitude).replace(".", "_").replace("-", "m")
        return f"lat_{latitude_part}_lon_{longitude_part}"

    def save_raw_payload(
        self,
        payload: dict[str, Any],
        request_data: WeatherRequest,
    ) -> Path:
        extract_date = date.today().isoformat()
        timestamp = build_start_time().replace(":", "").replace("-", "")
        file_stem = self._build_file_stem(request_data)
        output_directory = (
            self.source_config.raw_directory
            / f"extract_date={extract_date}"
            / f"station_id={file_stem}"
        )
        output_directory.mkdir(parents=True, exist_ok=True)
        output_path = output_directory / f"weather_{timestamp}.json"

        with output_path.open("w", encoding="utf-8") as file:
            json.dump(payload, file, indent=2, ensure_ascii=False)

        return output_path

    def to_dataframe(self, payload: dict[str, Any]) -> pd.DataFrame:
        dataframe = pd.DataFrame(payload["hourly"])
        dataframe["time"] = pd.to_datetime(dataframe["time"])
        return dataframe

    def run(
        self,
        latitude: float,
        longitude: float,
        start_date: str,
        end_date: str,
        station_id: str | None = None,
    ) -> tuple[Path, dict[str, Any], pd.DataFrame]:
        start_time = build_start_time()
        raw_path: str | Path = "unavailable"

        log_run_start(
            logger=self.logger,
            start_time=start_time,
            source_name=self.source_config.source_name,
        )

        try:
            request_data = WeatherRequest(
                latitude=latitude,
                longitude=longitude,
                start_date=start_date,
                end_date=end_date,
                station_id=station_id,
            )
            payload = self.fetch_weather(request_data)
            raw_path = self.save_raw_payload(payload, request_data)
            dataframe = self.to_dataframe(payload)
            log_run_end(
                logger=self.logger,
                start_time=start_time,
                source_name=self.source_config.source_name,
                output_path=raw_path,
                succeeded=True,
            )
            return raw_path, payload, dataframe
        except Exception:
            log_run_end(
                logger=self.logger,
                start_time=start_time,
                source_name=self.source_config.source_name,
                output_path=raw_path,
                succeeded=False,
            )
            raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Open-Meteo historical weather data for a station coordinate.",
    )
    parser.add_argument("--latitude", type=float, required=True)
    parser.add_argument("--longitude", type=float, required=True)
    parser.add_argument("--start-date", type=str, required=True)
    parser.add_argument("--end-date", type=str, required=True)
    parser.add_argument("--station-id", type=str, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    fetcher = WeatherFetcher()
    raw_path, payload, dataframe = fetcher.run(
        latitude=args.latitude,
        longitude=args.longitude,
        start_date=args.start_date,
        end_date=args.end_date,
        station_id=args.station_id,
    )

    print(f"Saved raw weather JSON to: {raw_path}")
    print()
    print(f"Top-level keys: {', '.join(payload.keys())}")
    print(f"Fetched {len(dataframe)} hourly rows")
    print(dataframe.to_string(index=False))


if __name__ == "__main__":
    main()
