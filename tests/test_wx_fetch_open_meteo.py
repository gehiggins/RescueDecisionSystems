import pandas as pd

from flask_app.app import wx_fetch_open_meteo


class _Response:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def test_open_meteo_requests_and_preserves_metres_per_second(monkeypatch):
    calls = []

    def fake_get(url, params, timeout):
        calls.append((url, params, timeout))
        return _Response(
            {
                "hourly": {
                    "time": ["2026-10-20T12:00"],
                    "temperature_2m": [20.0],
                    "relative_humidity_2m": [50.0],
                    "wind_speed_10m": [10.0],
                    "wind_gusts_10m": [18.0],
                    "wind_direction_10m": [270.0],
                }
            }
        )

    monkeypatch.setattr(wx_fetch_open_meteo.requests, "get", fake_get)

    result = wx_fetch_open_meteo.fetch_open_meteo_obs(
        37.5,
        -75.5,
        pd.Timestamp("2026-10-20T12:00:00Z"),
        pd.Timestamp("2026-10-20T12:00:00Z"),
        include_marine=False,
    )

    assert len(calls) == 1
    assert calls[0][1]["wind_speed_unit"] == "ms"
    assert result.loc[0, "wind_ms"] == 10.0
    assert result.loc[0, "wind_gust_ms"] == 18.0
