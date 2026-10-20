"""Offline coverage for NOAA/NWS onshore station discovery and observations."""

import pandas as pd
import pytest

from flask_app.app import fetcher_noaa_shore


class _Response:
    def __init__(self, payload):
        self._payload = payload

    @staticmethod
    def raise_for_status():
        return None

    def json(self):
        return self._payload


def test_station_discovery_uses_alert_location_and_sorts_by_distance(monkeypatch):
    captured = {}
    payload = {
        "features": [
            {
                "properties": {"stationIdentifier": "FAR", "name": "Far"},
                "geometry": {"coordinates": [-75.0, 39.0]},
            },
            {
                "properties": {"stationIdentifier": "NEAR", "name": "Near"},
                "geometry": {"coordinates": [-74.5, 37.5]},
            },
        ]
    }

    def get(url, timeout, headers):
        captured.update(url=url, timeout=timeout, headers=headers)
        return _Response(payload)

    monkeypatch.setattr(fetcher_noaa_shore.requests, "get", get)

    result = fetcher_noaa_shore.query_nearest_noaa_stations(
        37.5, -74.5, max_stations=1, request_timeout=4
    )

    assert captured["url"].endswith("/points/37.5,-74.5/stations")
    assert captured["timeout"] == 4
    assert result["station_id"].tolist() == ["NEAR"]
    assert result.iloc[0]["distance_nm"] == pytest.approx(0.0)


def test_invalid_station_reference_skips_provider_request(monkeypatch):
    monkeypatch.setattr(
        fetcher_noaa_shore.requests,
        "get",
        lambda *_args, **_kwargs: pytest.fail("provider called"),
    )

    assert fetcher_noaa_shore.query_nearest_noaa_stations(91, 0).empty


def test_latest_observation_preserves_shared_metric_units(monkeypatch):
    payload = {
        "properties": {
            "timestamp": "2026-10-20T18:00:00Z",
            "temperature": {"value": 16.9},
            "windSpeed": {"value": 12.0},
            "windDirection": {"value": 20.0},
        }
    }
    captured = {}

    def get(url, timeout, headers):
        captured.update(url=url, timeout=timeout, headers=headers)
        return _Response(payload)

    monkeypatch.setattr(fetcher_noaa_shore.requests, "get", get)

    result = fetcher_noaa_shore.fetch_single_noaa_station(
        "KTEST", request_timeout=6
    )

    assert captured["url"].endswith("/stations/KTEST/observations/latest")
    assert captured["timeout"] == 6
    assert result.iloc[0]["temperature"] == pytest.approx(16.9)
    assert result.iloc[0]["wind_speed"] == pytest.approx(12.0)
    assert result.iloc[0]["wind_direction"] == pytest.approx(20.0)
    assert pd.api.types.is_datetime64_any_dtype(result["observation_time"])
