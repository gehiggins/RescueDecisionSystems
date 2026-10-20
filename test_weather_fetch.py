"""Smoke-test the active weather provider, with live access opt-in only."""

import argparse

import pandas as pd

from flask_app.app.wx_fetch_open_meteo import (
    _HISTORICAL_FORECAST_URL,
    _atmosphere_url,
    fetch_open_meteo_obs,
)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Smoke-test RDS weather retrieval")
    parser.add_argument(
        "--live",
        action="store_true",
        help="contact Open-Meteo and verify a current observation response",
    )
    parser.add_argument("--latitude", type=float, default=37.76)
    parser.add_argument("--longitude", type=float, default=-75.503333)
    args = parser.parse_args(argv)

    if not args.live:
        # A fixed clock keeps the offline routing check deterministic over time.
        alert_time = pd.Timestamp("2025-03-02T13:32:17Z")
        current_time = pd.Timestamp("2026-10-20T00:00:00Z")
        endpoint = _atmosphere_url(alert_time, current_utc=current_time)
        if endpoint != _HISTORICAL_FORECAST_URL:
            raise SystemExit(f"Weather smoke failed: unexpected endpoint {endpoint}")
        print("Weather offline contract smoke passed")
        return 0

    end_utc = pd.Timestamp.now(tz="UTC").floor("h")
    start_utc = end_utc - pd.Timedelta(hours=6)
    observations = fetch_open_meteo_obs(
        args.latitude,
        args.longitude,
        start_utc,
        end_utc,
        include_marine=True,
    )
    required = {"provider", "valid_utc", "lat", "lon", "wind_ms"}
    missing = required - set(observations.columns)
    if missing:
        raise SystemExit(
            "Weather smoke failed; missing columns: " + ", ".join(sorted(missing))
        )
    if observations.empty:
        raise SystemExit("Weather smoke failed: Open-Meteo returned no observations")

    print(f"Weather live smoke passed: {len(observations)} observation row(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
