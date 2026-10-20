"""Smoke-test satellite reference resolution, with live TLE access opt-in."""

import argparse

import pandas as pd

from flask_app.app.sat_fetch_tle import (
    load_sat_reference,
    load_tle_snapshot,
    resolve_reporting_sat,
)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Smoke-test RDS satellite data")
    parser.add_argument(
        "--live",
        action="store_true",
        help="contact CelesTrak for current NOAA-19 and Galileo TLE data",
    )
    args = parser.parse_args(argv)

    reference = load_sat_reference()
    required = {"common_name", "norad_id", "type"}
    missing = required - set(reference.columns)
    if reference.empty or missing:
        raise SystemExit(
            "TLE smoke failed; satellite reference is empty or missing: "
            + ", ".join(sorted(missing))
        )

    resolved = resolve_reporting_sat(
        pd.DataFrame([{"satellite_name": "NOAA 19", "norad_id": 33591}])
    )
    if resolved is None or int(resolved["norad_id"]) != 33591:
        raise SystemExit("TLE smoke failed: NOAA-19 did not resolve to NORAD 33591")

    if not args.live:
        print(f"TLE offline contract smoke passed: {len(reference)} reference row(s)")
        return 0

    snapshot = load_tle_snapshot(["33591", "E401"])
    if snapshot.empty:
        raise SystemExit("TLE smoke failed: provider returned no valid records")
    print(f"TLE live smoke passed: {len(snapshot)} record(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
