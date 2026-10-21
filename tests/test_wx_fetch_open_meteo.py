import numpy as np
import pandas as pd
import pytest

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


@pytest.mark.parametrize("include_marine", ["0", "false", " no ", "OFF"])
def test_open_meteo_does_not_treat_false_text_as_true(
    monkeypatch, include_marine
):
    calls = []

    def fake_get(url, params, timeout):
        calls.append(url)
        return _Response({"hourly": {}})

    monkeypatch.setattr(wx_fetch_open_meteo.requests, "get", fake_get)

    wx_fetch_open_meteo.fetch_open_meteo_obs(
        37.5,
        -75.5,
        "2026-10-20T12:00:00Z",
        "2026-10-20T12:00:00Z",
        include_marine=include_marine,
    )

    assert len(calls) == 1
    assert "marine-api" not in calls[0]


def test_open_meteo_defaults_invalid_marine_flag(monkeypatch, caplog):
    calls = []

    def fake_get(url, params, timeout):
        calls.append(url)
        return _Response({"hourly": {}})

    monkeypatch.setattr(wx_fetch_open_meteo.requests, "get", fake_get)

    wx_fetch_open_meteo.fetch_open_meteo_obs(
        37.5,
        -75.5,
        "2026-10-20T12:00:00Z",
        "2026-10-20T12:00:00Z",
        include_marine="sometimes",
    )

    assert len(calls) == 2
    assert "marine-api" in calls[1]
    assert "Invalid Open-Meteo include_marine" in caplog.text


def test_open_meteo_uses_historical_forecast_service_for_old_alert(monkeypatch):
    calls = []
    target = pd.Timestamp.now(tz="UTC").floor("h") - pd.Timedelta(days=93)

    def fake_get(url, params, timeout):
        calls.append((url, params, timeout))
        return _Response({"hourly": {"time": [target.strftime("%Y-%m-%dT%H:%M")]}})

    monkeypatch.setattr(wx_fetch_open_meteo.requests, "get", fake_get)

    result = wx_fetch_open_meteo.fetch_open_meteo_obs(
        37.5,
        -75.5,
        target,
        target,
        include_marine=False,
    )

    assert calls[0][0] == wx_fetch_open_meteo._HISTORICAL_FORECAST_URL
    assert result["valid_utc"].tolist() == [target]


def test_open_meteo_uses_long_term_archive_before_forecast_history(monkeypatch):
    calls = []
    target = pd.Timestamp("2020-06-15T12:00:00Z")

    def fake_get(url, params, timeout):
        calls.append((url, params, timeout))
        return _Response({"hourly": {"time": ["2020-06-15T12:00"]}})

    monkeypatch.setattr(wx_fetch_open_meteo.requests, "get", fake_get)

    result = wx_fetch_open_meteo.fetch_open_meteo_obs(
        37.5,
        -75.5,
        target,
        target,
        include_marine=False,
    )

    assert calls[0][0] == wx_fetch_open_meteo._HISTORICAL_WEATHER_URL
    assert result["valid_utc"].tolist() == [target]


def test_open_meteo_keeps_live_forecast_service_for_recent_alert(monkeypatch):
    calls = []
    target = pd.Timestamp.now(tz="UTC").floor("h") - pd.Timedelta(days=1)

    def fake_get(url, params, timeout):
        calls.append((url, params, timeout))
        return _Response({"hourly": {"time": [target.strftime("%Y-%m-%dT%H:%M")]}})

    monkeypatch.setattr(wx_fetch_open_meteo.requests, "get", fake_get)

    wx_fetch_open_meteo.fetch_open_meteo_obs(
        37.5,
        -75.5,
        target,
        target,
        include_marine=False,
    )

    assert calls[0][0] == wx_fetch_open_meteo._FORECAST_URL


def test_open_meteo_keeps_usable_rows_from_partial_hourly_arrays(monkeypatch):
    def fake_get(_url, params, timeout):
        assert params["wind_speed_unit"] == "ms"
        assert timeout == 20
        return _Response(
            {
                "hourly": {
                    "time": ["invalid", "2026-10-20T12:00"],
                    "temperature_2m": [10.0, 20.0],
                    "relative_humidity_2m": [50.0],
                    "wind_speed_10m": [5.0],
                }
            }
        )

    monkeypatch.setattr(wx_fetch_open_meteo.requests, "get", fake_get)

    result = wx_fetch_open_meteo.fetch_open_meteo_obs(
        37.5,
        -75.5,
        "2026-10-20T12:00:00Z",
        "2026-10-20T12:00:00Z",
        include_marine=False,
    )

    assert len(result) == 1
    assert result.loc[0, "temp_c"] == 20.0
    assert pd.isna(result.loc[0, "rh_pct"])
    assert pd.isna(result.loc[0, "wind_ms"])


def test_open_meteo_rejects_invalid_time_window_without_request(monkeypatch):
    monkeypatch.setattr(
        wx_fetch_open_meteo.requests,
        "get",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("provider called with invalid time window")
        ),
    )

    result = wx_fetch_open_meteo.fetch_open_meteo_obs(
        37.5,
        -75.5,
        "invalid",
        "2026-10-20T12:00:00Z",
        include_marine=False,
    )

    assert result.empty
    assert "valid_utc" in result.columns


@pytest.mark.parametrize(
    ("latitude", "longitude"),
    [
        ("bad", -75.5),
        (91.0, -75.5),
        (37.5, -181.0),
        (float("inf"), 0.0),
        (True, False),
    ],
)
def test_open_meteo_rejects_invalid_coordinates_without_request(
    monkeypatch, latitude, longitude
):
    monkeypatch.setattr(
        wx_fetch_open_meteo.requests,
        "get",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("provider called with invalid coordinates")
        ),
    )

    result = wx_fetch_open_meteo.fetch_open_meteo_obs(
        latitude,
        longitude,
        "2026-10-20T12:00:00Z",
        "2026-10-20T12:00:00Z",
        include_marine=False,
    )

    assert result.empty
    assert "valid_utc" in result.columns


def test_open_meteo_non_mapping_payload_returns_empty(monkeypatch):
    monkeypatch.setattr(
        wx_fetch_open_meteo.requests,
        "get",
        lambda *_args, **_kwargs: _Response([]),
    )

    result = wx_fetch_open_meteo.fetch_open_meteo_obs(
        37.5,
        -75.5,
        "2026-10-20T12:00:00Z",
        "2026-10-20T12:00:00Z",
        include_marine=False,
    )

    assert result.empty


def test_open_meteo_preserves_marine_rows_when_atmosphere_is_empty(monkeypatch):
    calls = []

    def fake_get(url, params, timeout):
        calls.append(url)
        if "marine-api" in url:
            return _Response(
                {
                    "hourly": {
                        "time": ["2026-10-20T12:00"],
                        "wave_height": [2.5],
                        "wave_period": [8.0],
                        "wave_direction": [225.0],
                    }
                }
            )
        return _Response({"hourly": {}})

    monkeypatch.setattr(wx_fetch_open_meteo.requests, "get", fake_get)

    result = wx_fetch_open_meteo.fetch_open_meteo_obs(
        37.5,
        -75.5,
        "2026-10-20T12:00:00Z",
        "2026-10-20T12:00:00Z",
        include_marine=True,
    )

    assert len(calls) == 2
    assert len(result) == 1
    assert result.loc[0, "valid_utc"] == pd.Timestamp("2026-10-20T12:00:00Z")
    assert result.loc[0, "wave_height_m"] == 2.5
    assert result.loc[0, "wave_period_s"] == 8.0
    assert result.loc[0, "wave_dir_deg"] == 225.0
    assert pd.isna(result.loc[0, "temp_c"])
    assert pd.isna(result.loc[0, "wind_ms"])


def test_open_meteo_rejects_physically_invalid_provider_measurements(monkeypatch):
    def fake_get(url, params, timeout):
        if "marine-api" in url:
            return _Response(
                {
                    "hourly": {
                        "time": ["2026-10-20T12:00"],
                        "wave_height": [-1.0],
                        "wave_period": [-2.0],
                        "wave_direction": [361.0],
                    }
                }
            )
        return _Response(
            {
                "hourly": {
                    "time": ["2026-10-20T12:00"],
                    "temperature_2m": [20.0],
                    "relative_humidity_2m": [101.0],
                    "wind_speed_10m": [-1.0],
                    "wind_direction_10m": [-1.0],
                    "wind_gusts_10m": [-2.0],
                    "pressure_msl": [-1.0],
                    "precipitation": [-1.0],
                    "cloud_cover": [101.0],
                }
            }
        )

    monkeypatch.setattr(wx_fetch_open_meteo.requests, "get", fake_get)

    result = wx_fetch_open_meteo.fetch_open_meteo_obs(
        37.5,
        -75.5,
        "2026-10-20T12:00:00Z",
        "2026-10-20T12:00:00Z",
        include_marine=True,
    )

    invalid_fields = [
        "rh_pct",
        "dewpoint_c",
        "wind_ms",
        "wind_dir_deg",
        "wind_gust_ms",
        "pressure_hpa",
        "precip_mmhr",
        "cloud_cover_pct",
        "wave_height_m",
        "wave_period_s",
        "wave_dir_deg",
    ]
    assert result.loc[0, invalid_fields].isna().all()
    assert result.loc[0, "temp_c"] == 20.0


@pytest.mark.parametrize(
    ("minimum", "maximum", "value"),
    [(0, 100, 0), (0, 100, 100), (0, 360, 0), (0, 360, 360)],
)
def test_open_meteo_numeric_range_preserves_boundaries(
    minimum, maximum, value
):
    assert wx_fetch_open_meteo._numeric_at(
        {"value": [value]}, "value", 0, minimum=minimum, maximum=maximum
    ) == float(value)


@pytest.mark.parametrize("value", [True, False, np.bool_(True)])
def test_open_meteo_numeric_values_reject_booleans(value):
    assert pd.isna(
        wx_fetch_open_meteo._numeric_at({"value": [value]}, "value", 0)
    )


def test_open_meteo_merges_additional_marine_timestamp(monkeypatch):
    def fake_get(url, params, timeout):
        if "marine-api" in url:
            return _Response(
                {
                    "hourly": {
                        "time": ["2026-10-20T13:00"],
                        "wave_height": [1.5],
                    }
                }
            )
        return _Response(
            {
                "hourly": {
                    "time": ["2026-10-20T12:00"],
                    "temperature_2m": [20.0],
                }
            }
        )

    monkeypatch.setattr(wx_fetch_open_meteo.requests, "get", fake_get)

    result = wx_fetch_open_meteo.fetch_open_meteo_obs(
        37.5,
        -75.5,
        "2026-10-20T12:00:00Z",
        "2026-10-20T13:00:00Z",
        include_marine=True,
    )

    assert result["valid_utc"].tolist() == [
        pd.Timestamp("2026-10-20T12:00:00Z"),
        pd.Timestamp("2026-10-20T13:00:00Z"),
    ]
    assert result.loc[0, "temp_c"] == 20.0
    assert pd.isna(result.loc[0, "wave_height_m"])
    assert pd.isna(result.loc[1, "temp_c"])
    assert result.loc[1, "wave_height_m"] == 1.5
