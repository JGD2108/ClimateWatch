import json
import sys
from dataclasses import asdict, dataclass
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
class StationSourceConfig:
    source_name: str
    download_url: str
    raw_directory: Path
    timeout_seconds: int = 60


@dataclass(frozen=True)
class StationRecord:
    station_id: str
    station_name: str
    latitude: float | None
    longitude: float | None


class StationMetadataFetcher:
    def __init__(self, config_path: Path | None = None) -> None:
        self.repo_root = Path(__file__).resolve().parents[2]
        self.config_path = config_path or self.repo_root / "config" / "sources.yaml"
        self.source_config = self._load_source_config()
        self.logger = get_logger("fetch_stations")

    def _load_source_config(self) -> StationSourceConfig:
        with self.config_path.open("r", encoding="utf-8") as file:
            config = yaml.safe_load(file)

        station_cfg = config["sources"]["station_metadata"]
        raw_directory = self.repo_root / station_cfg["raw_directory"]

        return StationSourceConfig(
            source_name=station_cfg["source_name"],
            download_url=station_cfg["download_url"],
            raw_directory=raw_directory,
        )

    def fetch_geojson(self) -> dict[str, Any]:
        response = requests.get(
            self.source_config.download_url,
            timeout=self.source_config.timeout_seconds,
        )
        response.raise_for_status()
        return response.json()

    def save_raw_geojson(self, payload: dict[str, Any]) -> Path:
        extract_date = date.today().isoformat()
        output_directory = (
            self.source_config.raw_directory
            / f"extract_date={extract_date}"
        )
        output_directory.mkdir(parents=True, exist_ok=True)
        output_path = output_directory / "stations.geojson"

        with output_path.open("w", encoding="utf-8") as file:
            json.dump(payload, file, indent=2, ensure_ascii=False)

        return output_path

    def _build_station_record(
        self,
        feature: dict[str, Any],
        feature_index: int,
    ) -> StationRecord:
        properties = feature.get("properties", {})
        geometry = feature.get("geometry", {})
        coordinates = geometry.get("coordinates", [])

        station_id = properties.get("cod_estac") or properties.get("OBJECTID")
        station_name = properties.get("estacion")
        longitude = coordinates[0] if len(coordinates) > 0 else None
        latitude = coordinates[1] if len(coordinates) > 1 else None

        if not station_id:
            station_id = f"station_{feature_index}"
        if not station_name:
            station_name = f"Station {feature_index}"

        return StationRecord(
            station_id=str(station_id),
            station_name=str(station_name),
            latitude=latitude,
            longitude=longitude,
        )

    def extract_stations(self, payload: dict[str, Any]) -> list[StationRecord]:
        features = payload.get("features", [])
        return [
            self._build_station_record(feature, index)
            for index, feature in enumerate(features, start=1)
        ]

    def to_dataframe(self, stations: list[StationRecord]) -> pd.DataFrame:
        return pd.DataFrame([asdict(station) for station in stations])

    def run(
        self,
        return_stations: bool = True,
    ) -> tuple[Path, list[StationRecord] | None]:
        start_time = build_start_time()
        raw_path: str | Path = "unavailable"

        log_run_start(
            logger=self.logger,
            start_time=start_time,
            source_name=self.source_config.source_name,
        )

        try:
            payload = self.fetch_geojson()
            raw_path = self.save_raw_geojson(payload)

            if not return_stations:
                log_run_end(
                    logger=self.logger,
                    start_time=start_time,
                    source_name=self.source_config.source_name,
                    output_path=raw_path,
                    succeeded=True,
                )
                return raw_path, None

            stations = self.extract_stations(payload)
            log_run_end(
                logger=self.logger,
                start_time=start_time,
                source_name=self.source_config.source_name,
                output_path=raw_path,
                succeeded=True,
            )
            return raw_path, stations
        except Exception:
            log_run_end(
                logger=self.logger,
                start_time=start_time,
                source_name=self.source_config.source_name,
                output_path=raw_path,
                succeeded=False,
            )
            raise


def main() -> None:
    fetcher = StationMetadataFetcher()
    raw_path, stations = fetcher.run(return_stations=True)

    print(f"Saved raw GeoJSON to: {raw_path}")

    if not stations:
        return

    stations_df = fetcher.to_dataframe(stations)
    print()
    print(f"Fetched {len(stations_df)} stations")
    print(stations_df.to_string(index=False))


if __name__ == "__main__":
    main()
