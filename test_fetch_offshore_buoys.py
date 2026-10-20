"""Smoke-test the active offshore weather and wave-data contract."""

import argparse

import pandas as pd

from flask_app.app.wx_fetch_open_meteo import fetch_open_meteo_obs


MARINE_COLUMNS = {"wave_height_m", "wave_period_s", "wave_dir_deg", "sst_c"}


def main(argv=None):
    parser = argparse.ArgumentParser(description="Smoke-test offshore weather data")
    parser.add_argument(
        "--live",
        action="store_true",
        help="contact Open-Meteo for current offshore weather and wave data",
    )
    parser.add_argument("--latitude", type=float, default=37.5)
    parser.add_argument("--longitude", type=float, default=-74.5)
    args = parser.parse_args(argv)

    end_utc = pd.Timestamp.now(tz="UTC").floor("h")
    start_utc = end_utc - pd.Timedelta(hours=6)
    if args.live:
        observations = fetch_open_meteo_obs(
            args.latitude,
            args.longitude,
            start_utc,
            end_utc,
            include_marine=True,
        )
        if observations.empty:
            raise SystemExit("Offshore weather smoke failed: no observations returned")
        if observations[list(MARINE_COLUMNS)].notna().sum().sum() == 0:
            raise SystemExit("Offshore weather smoke failed: no marine values returned")
        print(f"Offshore weather live smoke passed: {len(observations)} row(s)")
        return 0

    # Invalid coordinates are rejected before any provider request, while the
    # empty result still exposes the full shared weather-and-marine schema.
    observations = fetch_open_meteo_obs(
        91.0,
        args.longitude,
        start_utc,
        end_utc,
        include_marine=True,
    )
    missing = MARINE_COLUMNS - set(observations.columns)
    if not observations.empty or missing:
        raise SystemExit(
            "Offshore weather smoke failed; missing columns: "
            + ", ".join(sorted(missing))
        )
    print("Offshore weather offline contract smoke passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
