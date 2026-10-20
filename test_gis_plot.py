"""Generate a representative RDS alert map as a developer smoke test."""

import argparse
from pathlib import Path

import pandas as pd

from flask_app.app.gis_mapping import generate_gis_map


DEFAULT_OUTPUT = (
    Path(__file__).resolve().parent / "data" / "maps" / "tests" / "gis_smoke.html"
)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Smoke-test RDS GIS map generation")
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="HTML file to create (defaults to data/maps/tests/gis_smoke.html)",
    )
    args = parser.parse_args(argv)

    alert_row = pd.Series(
        {
            "site_id": "98372",
            "latitude_a": 37.76,
            "longitude_a": -75.503333,
            "range_ring_meters_a": 5000,
            "latitude_b": 38.15,
            "longitude_b": -70.12,
            "range_ring_meters_b": 5000,
            "nearest_weather_stations_a": [
                {
                    "station_id": "KWAL",
                    "latitude": 37.9372,
                    "longitude": -75.46619,
                    "temperature": 2.2,
                    "wind_speed": None,
                    "visibility": None,
                    "station_name": "Wallops Island",
                }
            ],
            "nearest_weather_stations_b": [
                {
                    "station_id": "KSBY",
                    "latitude": 38.34056,
                    "longitude": -75.51028,
                    "temperature": 3.5,
                    "wind_speed": 5.0,
                    "visibility": 10,
                    "station_name": "Salisbury",
                }
            ],
        }
    )

    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    map_path = generate_gis_map(alert_row, str(output))
    if map_path is None or not output.is_file() or output.stat().st_size == 0:
        raise SystemExit("GIS smoke failed: no map HTML was generated")

    print(f"GIS map smoke passed: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
