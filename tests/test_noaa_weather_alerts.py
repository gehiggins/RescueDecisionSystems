"""Offline regression coverage for NOAA/NWS active-zone alerts."""

import pytest
import pandas as pd

from flask_app.app import fetcher_noaa_weather_alerts
from flask_app.app import noaa_weather_alerts_fetch


class _Response:
    def __init__(self, payload):
        self._payload = payload

    @staticmethod
    def raise_for_status():
        return None

    def json(self):
        return self._payload


def _mock_noaa_get(calls):
    def get(url, timeout, headers):
        calls.append((url, timeout, headers))
        if "/points/" in url:
            return _Response(
                {
                    "properties": {
                        "forecastZone": "https://api.weather.gov/zones/forecast/VAZ099"
                    }
                }
            )
        return _Response(
            {
                "features": [
                    {
                        "properties": {
                            "headline": "Coastal Flood Advisory",
                            "event": "Coastal Flood Advisory",
                            "severity": "Minor",
                            "certainty": "Likely",
                            "urgency": "Expected",
                            "effective": "2026-10-20T18:00:00Z",
                            "expires": "2026-10-21T00:00:00Z",
                        }
                    },
                    {"properties": None},
                ]
            }
        )

    return get


def test_structured_alert_fetch_uses_active_zone_endpoint(monkeypatch):
    calls = []
    monkeypatch.setattr(
        fetcher_noaa_weather_alerts.requests,
        "get",
        _mock_noaa_get(calls),
    )

    result = fetcher_noaa_weather_alerts.fetch_weather_alerts_zone(
        37.76, -75.503333, request_timeout=4
    )

    assert calls[1][0] == "https://api.weather.gov/alerts/active/zone/VAZ099"
    assert all(call[1] == 4 for call in calls)
    assert result["zone_id"].tolist() == ["VAZ099"]
    assert result.iloc[0]["headline"] == "Coastal Flood Advisory"


@pytest.mark.parametrize(
    "invalid_timeout", [True, float("nan"), float("inf"), 0, -1, "bad"]
)
def test_alert_fetch_replaces_invalid_request_timeout(
    monkeypatch, invalid_timeout
):
    calls = []
    monkeypatch.setattr(
        fetcher_noaa_weather_alerts.requests,
        "get",
        _mock_noaa_get(calls),
    )

    fetcher_noaa_weather_alerts.fetch_weather_alerts_zone(
        37.76, -75.503333, request_timeout=invalid_timeout
    )

    assert all(
        timeout == fetcher_noaa_weather_alerts.NOAA_ALERT_TIMEOUT_SECONDS
        for _url, timeout, _headers in calls
    )


def test_compatibility_alert_fetch_preserves_tuple_contract(monkeypatch):
    monkeypatch.setattr(
        noaa_weather_alerts_fetch,
        "fetch_weather_alert_features",
        lambda *_args, **_kwargs: (
            "https://api.weather.gov/zones/forecast/VAZ099",
            "VAZ099",
            [
                {"properties": {"headline": "Coastal Flood Advisory"}},
                {"properties": None},
            ],
        ),
    )

    zone_url, headlines = noaa_weather_alerts_fetch.fetch_weather_alerts_zone(
        37.76, -75.503333
    )

    assert zone_url.endswith("/VAZ099")
    assert headlines == ["Coastal Flood Advisory"]


@pytest.mark.parametrize(
    ("latitude", "longitude"),
    [(91, 0), (0, 181), ("bad", 0), (True, False)],
)
def test_invalid_alert_coordinates_skip_provider(monkeypatch, latitude, longitude):
    monkeypatch.setattr(
        fetcher_noaa_weather_alerts.requests,
        "get",
        lambda *_args, **_kwargs: pytest.fail("provider called"),
    )

    result = fetcher_noaa_weather_alerts.fetch_weather_alerts_zone(
        latitude, longitude
    )

    assert result.empty


@pytest.mark.parametrize(
    ("latitude", "longitude"),
    [(None, 0), ([], 0), ({}, 0), (0, []), ("1e1000000", 0)],
)
def test_invalid_alert_coordinates_are_rejected_by_shared_fetcher(
    monkeypatch, latitude, longitude
):
    monkeypatch.setattr(
        fetcher_noaa_weather_alerts.requests,
        "get",
        lambda *_args, **_kwargs: pytest.fail("provider called"),
    )

    result = fetcher_noaa_weather_alerts.fetch_weather_alert_features(
        latitude, longitude
    )

    assert result == (None, None, [])


@pytest.mark.parametrize("features", [None, {}, "unexpected", 42])
def test_shared_alert_fetcher_rejects_non_list_features(monkeypatch, features):
    responses = iter(
        [
            _Response(
                {
                    "properties": {
                        "forecastZone": "https://api.weather.gov/zones/forecast/VAZ099"
                    }
                }
            ),
            _Response({"features": features}),
        ]
    )
    monkeypatch.setattr(
        fetcher_noaa_weather_alerts.requests,
        "get",
        lambda *_args, **_kwargs: next(responses),
    )

    zone_url, zone_id, parsed_features = (
        fetcher_noaa_weather_alerts.fetch_weather_alert_features(37.76, -75.5)
    )

    assert zone_url.endswith("/VAZ099")
    assert zone_id == "VAZ099"
    assert parsed_features == []


def test_compatibility_alert_fetch_skips_nullable_or_non_text_headlines(
    monkeypatch,
):
    monkeypatch.setattr(
        noaa_weather_alerts_fetch,
        "fetch_weather_alert_features",
        lambda *_args, **_kwargs: (
            "https://api.weather.gov/zones/forecast/VAZ099",
            "VAZ099",
            [
                {"properties": pd.NA},
                {"properties": {"headline": ["not", "text"]}},
                {"properties": {"headline": "Coastal Flood Advisory"}},
            ],
        ),
    )

    zone_url, headlines = noaa_weather_alerts_fetch.fetch_weather_alerts_zone(
        37.76, -75.5
    )

    assert zone_url.endswith("/VAZ099")
    assert headlines == ["Coastal Flood Advisory"]
