"""Smoke-test the active satellite overlay contract, optionally using live TLEs."""

import argparse

import pandas as pd

from flask_app.app.sat_pipeline import build_sat_overlay_df


def main(argv=None):
    parser = argparse.ArgumentParser(description="Smoke-test RDS satellite overlays")
    parser.add_argument(
        "--live",
        action="store_true",
        help="contact the configured TLE provider and attempt orbital propagation",
    )
    parser.add_argument("--norad-id", type=int, default=33591)
    args = parser.parse_args(argv)

    alert_df = pd.DataFrame(
        [
            {
                "alert_id": "SMOKE-SAT",
                "alert_time_utc": pd.Timestamp.now(tz="UTC"),
                "satellite_name": "NOAA 19",
                "norad_id": args.norad_id,
                "alert_lat_dd": 40.0,
                "alert_lon_dd": -120.0,
            }
        ]
    )
    overlay = build_sat_overlay_df(
        alert_df,
        scope="reporting",
        types=("LEO",),
        use_tle=args.live,
        fallback_to_nearest=args.live,
    )

    required = {"sat_name", "norad_id", "at_time_utc", "lat_dd", "lon_dd"}
    missing = required - set(overlay.columns)
    if missing:
        raise SystemExit(
            "Satellite smoke failed; missing columns: " + ", ".join(sorted(missing))
        )

    mode = "live" if args.live else "offline contract"
    print(f"Satellite {mode} smoke passed: {len(overlay)} overlay row(s)")
    if not overlay.empty:
        available = [column for column in required if column in overlay.columns]
        print(overlay[available].to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
