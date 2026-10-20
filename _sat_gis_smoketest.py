"""Generate a satellite-aware GIS map without requiring network access."""

import argparse
from pathlib import Path

import pandas as pd

from flask_app.app.gis_map_inputs_builder import build_gis_map_inputs_df
from flask_app.app.gis_mapping import generate_gis_map_html_from_dfs
from flask_app.app.sat_pipeline import build_sat_overlay_df


DEFAULT_OUTPUT = (
    Path(__file__).resolve().parent / "data" / "maps" / "tests" / "sat_gis_smoke.html"
)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Smoke-test satellite GIS integration")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--live",
        action="store_true",
        help="contact the configured TLE provider for orbital propagation",
    )
    args = parser.parse_args(argv)

    alert_time = pd.Timestamp.now(tz="UTC")
    alert_df = pd.DataFrame(
        [
            {
                "alert_id": "SAT-GIS-SMOKE",
                "alert_time_utc": alert_time,
                "satellite_name": "NOAA 19",
                "norad_id": 33591,
                "alert_lat_dd": 35.42,
                "alert_lon_dd": -76.61,
            }
        ]
    )
    positions_df = pd.DataFrame(
        [
            {
                "site_id": "SAT-GIS-SMOKE",
                "role": "A",
                "lat_dd": 35.42,
                "lon_dd": -76.61,
                "ts_utc": alert_time,
                "range_ring_meters": 5000,
            }
        ]
    )
    satellite_df = build_sat_overlay_df(
        alert_df,
        types=("LEO",),
        use_tle=args.live,
        fallback_to_nearest=args.live,
    )
    map_inputs = build_gis_map_inputs_df(
        positions_df,
        sat_overlays=satellite_df,
    )

    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    generated = generate_gis_map_html_from_dfs(
        map_inputs,
        alert_df.iloc[0],
        str(output),
    )
    if generated is None or not output.is_file() or output.stat().st_size == 0:
        raise SystemExit("Satellite GIS smoke failed: no map HTML was generated")

    mode = "live" if args.live else "offline contract"
    print(f"Satellite GIS {mode} smoke passed: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
