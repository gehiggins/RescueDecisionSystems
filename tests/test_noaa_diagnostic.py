"""Offline orchestration coverage for the NOAA/NWS diagnostic command."""

import pandas as pd

from flask_app import test_noaa_api


def test_diagnostic_fetches_observation_for_nearest_station(monkeypatch):
    stations = pd.DataFrame(
        [
            {"station_id": "NEAR", "distance_nm": 1.0},
            {"station_id": "FAR", "distance_nm": 10.0},
        ]
    )
    observation = pd.DataFrame([{"temperature": 20.0}])
    captured = {}
    monkeypatch.setattr(
        test_noaa_api,
        "query_nearest_noaa_stations",
        lambda lat, lon, max_stations, request_timeout: captured.update(
            lat=lat,
            lon=lon,
            max_stations=max_stations,
            request_timeout=request_timeout,
        )
        or stations,
    )
    monkeypatch.setattr(
        test_noaa_api,
        "fetch_single_noaa_station",
        lambda station_id, request_timeout: captured.update(
            station_id=station_id,
            observation_timeout=request_timeout,
        )
        or observation,
    )

    returned_stations, returned_observation = test_noaa_api.fetch_weather_stations(
        37.76,
        -75.503333,
        max_stations=2,
        request_timeout=4,
    )

    assert returned_stations.equals(stations)
    assert returned_observation.equals(observation)
    assert captured["station_id"] == "NEAR"
    assert captured["request_timeout"] == 4
    assert captured["observation_timeout"] == 4
