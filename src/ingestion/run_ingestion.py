import argparse
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path

import pandas as pd

CURRENT_DIRECTORY = Path(__file__).resolve().parent
SRC_ROOT = CURRENT_DIRECTORY.parents[0]
if str(CURRENT_DIRECTORY) not in sys.path:
    sys.path.append(str(CURRENT_DIRECTORY))
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from fetch_air_quality import AirQualityFetcher
from fetch_stations import StationMetadataFetcher, StationRecord
from fetch_weather import WeatherFetcher
from utils.run_logging import build_start_time, get_logger, log_run_end, log_run_start


@dataclass(frozen=True)
class IngestionConfig:
    batch_id: str
    station_limit: int
    start_date: str
    end_date: str


@dataclass(frozen=True)
class StationIngestionResult:
    station_id: str
    station_name: str
    latitude: float | None
    longitude: float | None
    air_quality_raw_path: str
    air_quality_rows: int
    weather_raw_path: str
    weather_rows: int


class IngestionOrchestrator:
    def __init__(self, config: IngestionConfig) -> None:
        self.config = config
        self.station_fetcher = StationMetadataFetcher()
        self.air_quality_fetcher = AirQualityFetcher()
        self.weather_fetcher = WeatherFetcher()

    def _select_test_stations(
        self,
        stations: list[StationRecord],
    ) -> list[StationRecord]:
        return stations[: self.config.station_limit]

    def _run_station_ingestion(
        self,
        station: StationRecord,
    ) -> StationIngestionResult:
        air_quality_raw_path, _, air_quality_df = self.air_quality_fetcher.run(
            latitude=station.latitude,
            longitude=station.longitude,
            station_id=station.station_id,
        )
        weather_raw_path, _, weather_df = self.weather_fetcher.run(
            latitude=station.latitude,
            longitude=station.longitude,
            start_date=self.config.start_date,
            end_date=self.config.end_date,
            station_id=station.station_id,
        )

        return StationIngestionResult(
            station_id=station.station_id,
            station_name=station.station_name,
            latitude=station.latitude,
            longitude=station.longitude,
            air_quality_raw_path=str(air_quality_raw_path),
            air_quality_rows=len(air_quality_df),
            weather_raw_path=str(weather_raw_path),
            weather_rows=len(weather_df),
        )

    def run(self) -> tuple[Path, list[StationIngestionResult]]:
        stations_raw_path, stations = self.station_fetcher.run(return_stations=True)

        if not stations:
            return stations_raw_path, []

        selected_stations = self._select_test_stations(stations)
        results = [
            self._run_station_ingestion(station)
            for station in selected_stations
            if station.latitude is not None and station.longitude is not None
        ]
        return stations_raw_path, results

    def to_dataframe(
        self,
        results: list[StationIngestionResult],
    ) -> pd.DataFrame:
        return pd.DataFrame([asdict(result) for result in results])


def parse_args() -> argparse.Namespace:
    today = date.today().isoformat()
    parser = argparse.ArgumentParser(
        description="Run the basic Bogota ingestion flow in the correct order.",
    )
    parser.add_argument("--station-limit", type=int, default=2)
    parser.add_argument("--start-date", type=str, default=today)
    parser.add_argument("--end-date", type=str, default=today)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = IngestionConfig(
        batch_id=datetime.now().strftime("batch_%Y%m%dT%H%M%S"),
        station_limit=args.station_limit,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    orchestrator = IngestionOrchestrator(config)
    logger = get_logger("run_ingestion")
    start_time = build_start_time()
    output_path = orchestrator.station_fetcher.repo_root / "data" / "raw"

    log_run_start(
        logger=logger,
        start_time=start_time,
        source_name="basic_ingestion",
        output_path=output_path,
        batch_id=config.batch_id,
    )

    try:
        stations_raw_path, results = orchestrator.run()
        log_run_end(
            logger=logger,
            start_time=start_time,
            source_name="basic_ingestion",
            output_path=output_path,
            succeeded=True,
            batch_id=config.batch_id,
        )
    except Exception:
        log_run_end(
            logger=logger,
            start_time=start_time,
            source_name="basic_ingestion",
            output_path=output_path,
            succeeded=False,
            batch_id=config.batch_id,
        )
        raise

    print(f"Batch ID: {config.batch_id}")
    print(f"Saved stations raw file to: {stations_raw_path}")
    print(f"Station test limit: {config.station_limit}")
    print(f"Weather date range: {config.start_date} to {config.end_date}")

    if not results:
        print("No station ingestion results were produced.")
        return

    summary_df = orchestrator.to_dataframe(results)
    print()
    print("Ingestion Summary")
    print(summary_df.to_string(index=False))


if __name__ == "__main__":
    main()
