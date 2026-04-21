"""Prototype that transforms local raw weather JSON files into curated CSV outputs."""

import json
from datetime import datetime
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_WEATHER_DIR = REPO_ROOT / "data" / "raw" / "weather"
CURATED_WEATHER_DIR = REPO_ROOT / "data" / "curated" / "weather"
EXPECTED_HOURLY_COLUMNS = {
    "time",
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation_probability",
    "cloud_cover",
    "wind_speed_10m",
}


def get_all_weather_files(root: Path) -> list[Path]:
    weather_files = sorted(root.rglob("weather_*.json"))
    if not weather_files:
        raise FileNotFoundError(f"No weather JSON files found under {root}")
    return weather_files


def parse_weather_file_name(file_path: Path) -> tuple[str, str]:
    stem = file_path.stem

    if not stem.startswith("weather_"):
        raise ValueError(f"Unexpected weather file name: {file_path.name}")

    city_slug, separator, timestamp = stem.removeprefix("weather_").rpartition("_")
    if not separator or not city_slug or not timestamp:
        raise ValueError(f"Could not parse city and timestamp from file name: {file_path.name}")

    return city_slug, timestamp


def get_curated_output_file(raw_file_path: Path) -> tuple[str, Path]:
    city_slug, timestamp = parse_weather_file_name(raw_file_path)
    relative_partition = raw_file_path.parent.relative_to(RAW_WEATHER_DIR)
    output_dir = CURATED_WEATHER_DIR / relative_partition
    output_file = output_dir / f"weather_curated_{city_slug}_{timestamp}.csv"
    return city_slug, output_file


def get_validation_summary(df: pd.DataFrame) -> dict[str, object]:
    missing_columns = sorted(EXPECTED_HOURLY_COLUMNS - set(df.columns))
    return {
        "empty_dataframe": df.empty,
        "time_column_present": "time" in df.columns,
        "time_converted_ok": False,
        "missing_expected_columns": missing_columns,
    }


def transform_weather_file(raw_file_path: Path, output_file: Path) -> dict[str, object]:
    city_slug, ingestion_timestamp_raw = parse_weather_file_name(raw_file_path)

    with raw_file_path.open("r", encoding="utf-8") as f:
        json_data = json.load(f)

    hourly_data = json_data["hourly"]
    df = pd.DataFrame(hourly_data)
    validation_summary = get_validation_summary(df)

    if validation_summary["missing_expected_columns"]:
        missing_columns_text = ", ".join(validation_summary["missing_expected_columns"])
        raise ValueError(f"Missing expected columns: {missing_columns_text}")

    if validation_summary["empty_dataframe"]:
        raise ValueError("Weather DataFrame is empty")

    try:
        df["time"] = pd.to_datetime(df["time"])
    except Exception as exc:
        raise ValueError(f"Failed to convert time column to datetime: {exc}") from exc

    try:
        ingestion_timestamp = pd.to_datetime(ingestion_timestamp_raw, format="%Y%m%dT%H%M%S")
    except Exception as exc:
        raise ValueError(f"Failed to convert ingestion timestamp to datetime: {exc}") from exc

    validation_summary["time_converted_ok"] = True
    df["city"] = city_slug
    df["ingestion_timestamp"] = ingestion_timestamp
    ordered_columns = [
        "city",
        "ingestion_timestamp",
        "time",
        *[col for col in df.columns if col not in {"city", "ingestion_timestamp", "time"}],
    ]
    df = df[ordered_columns]

    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False, encoding="utf-8")

    return {
        "status": "transformed",
        "city": city_slug,
        "raw_file": str(raw_file_path),
        "output_file": str(output_file),
        "rows_written": len(df),
        **validation_summary,
    }


def save_transform_audit_report(
    run_timestamp: str,
    raw_files_found: int,
    results: list[dict[str, object]],
    repo_root: Path,
) -> Path:
    run_dt = datetime.strptime(run_timestamp, "%Y%m%dT%H%M%S")
    year = run_dt.strftime("%Y")
    month = run_dt.strftime("%m")
    day = run_dt.strftime("%d")

    audit_dir = (
        repo_root
        / "data"
        / "audit"
        / "curated_weather"
        / f"year={year}"
        / f"month={month}"
        / f"day={day}"
    )
    audit_dir.mkdir(parents=True, exist_ok=True)

    transformed_count = sum(1 for result in results if result["status"] == "transformed")
    skipped_count = sum(1 for result in results if result["status"] == "skipped")
    failed_count = sum(1 for result in results if result["status"] == "error")

    audit_payload = {
        "run_timestamp": run_timestamp,
        "layer": "curated_weather",
        "raw_files_found": raw_files_found,
        "files_transformed": transformed_count,
        "files_skipped": skipped_count,
        "files_failed": failed_count,
        "results": results,
    }

    audit_file = audit_dir / f"curated_weather_run_{run_timestamp}.json"
    with audit_file.open("w", encoding="utf-8") as f:
        json.dump(audit_payload, f, ensure_ascii=False, indent=2)

    return audit_file


def main() -> None:
    raw_files = get_all_weather_files(RAW_WEATHER_DIR)
    run_timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    results: list[dict[str, object]] = []

    print(f"Raw weather files found: {len(raw_files)}")

    for raw_file_path in raw_files:
        city_slug = "unknown"
        output_file: Path | None = None

        try:
            city_slug, output_file = get_curated_output_file(raw_file_path)

            print(f"Raw file: {raw_file_path}")
            print(f"City: {city_slug}")
            print(f"Expected output file: {output_file}")

            if output_file.exists():
                result = {
                    "status": "skipped",
                    "city": city_slug,
                    "raw_file": str(raw_file_path),
                    "output_file": str(output_file),
                    "rows_written": 0,
                    "empty_dataframe": None,
                    "time_column_present": None,
                    "time_converted_ok": None,
                    "missing_expected_columns": None,
                }
                print(f"Status: skipped | City: {result['city']} | Reason: curated file already exists")
            else:
                result = transform_weather_file(raw_file_path, output_file)
                print(f"Status: transformed | City: {result['city']} | Rows written: {result['rows_written']}")
        except Exception as exc:
            result = {
                "status": "error",
                "city": city_slug,
                "raw_file": str(raw_file_path),
                "output_file": str(output_file) if output_file is not None else None,
                "rows_written": 0,
                "empty_dataframe": None,
                "time_column_present": None,
                "time_converted_ok": None,
                "missing_expected_columns": None,
                "error": str(exc),
            }
            print(f"Status: error | City: {city_slug} | Error: {exc}")

        results.append(result)

    transformed_count = sum(1 for result in results if result["status"] == "transformed")
    skipped_count = sum(1 for result in results if result["status"] == "skipped")
    failed_count = sum(1 for result in results if result["status"] == "error")
    audit_file = save_transform_audit_report(run_timestamp, len(raw_files), results, REPO_ROOT)

    print(f"Files transformed: {transformed_count}")
    print(f"Files skipped: {skipped_count}")
    print(f"Files failed: {failed_count}")
    print(f"Transform audit file: {audit_file}")


if __name__ == "__main__":
    main()
