"""Developer diagnostic for the maintained NOAA/NWS shore adapter."""

import argparse

import pandas as pd

from flask_app.app.fetcher_noaa_shore import (
    _temperature_c,
    _wind_speed_ms,
    fetch_single_noaa_station,
    query_nearest_noaa_stations,
)


DEFAULT_LATITUDE = 37.76
DEFAULT_LONGITUDE = -75.503333


def fetch_weather_stations(lat, lon, max_stations=3, request_timeout=10):
    """Return nearby NWS stations and the nearest station's latest observation."""
    stations = query_nearest_noaa_stations(
        lat,
        lon,
        max_stations=max_stations,
        request_timeout=request_timeout,
    )
    if stations.empty:
        return stations, pd.DataFrame()
    observation = fetch_single_noaa_station(
        stations.iloc[0]["station_id"],
        request_timeout=request_timeout,
    )
    return stations, observation


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Check NOAA/NWS shore station discovery and observations"
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="contact api.weather.gov; omitted by default for offline safety",
    )
    parser.add_argument("--latitude", type=float, default=DEFAULT_LATITUDE)
    parser.add_argument("--longitude", type=float, default=DEFAULT_LONGITUDE)
    parser.add_argument("--max-stations", type=int, default=3)
    parser.add_argument("--request-timeout", type=float, default=10.0)
    args = parser.parse_args(argv)

    if not args.live:
        # Exercise the unit contract without contacting the provider.
        assert _temperature_c(
            {"value": 68, "unitCode": "wmoUnit:degF"}
        ) == 20
        assert _wind_speed_ms(
            {"value": 36, "unitCode": "wmoUnit:km_h-1"}
        ) == 10
        print("NOAA shore diagnostic offline contract passed")
        return 0

    stations, observation = fetch_weather_stations(
        args.latitude,
        args.longitude,
        max_stations=args.max_stations,
        request_timeout=args.request_timeout,
    )
    if stations.empty:
        raise SystemExit("NOAA shore diagnostic failed: no stations returned")
    if observation.empty:
        raise SystemExit("NOAA shore diagnostic failed: no observation returned")

    print("Nearby NWS stations:")
    print(
        stations[["station_id", "station_name", "distance_nm"]].to_string(
            index=False
        )
    )
    print("Nearest station observation (RDS metric schema):")
    print(
        observation[
            [
                "observation_time",
                "temperature",
                "wind_speed",
                "wind_direction",
            ]
        ].to_string(index=False)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
