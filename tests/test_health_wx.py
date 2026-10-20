import numpy as np
import pandas as pd

from flask_app.app import health_wx, pipeline_health_entrypoint


def test_health_check_normalizes_invalid_runtime_parameters(monkeypatch, caplog):
    calls = {}

    def capture_open_meteo(lat, lon, start_utc, end_utc, **kwargs):
        calls["open_meteo"] = (lat, lon, start_utc, end_utc, kwargs)
        return pd.DataFrame()

    def capture_meteostat(lat, lon, start_utc, end_utc, **kwargs):
        calls["meteostat"] = (lat, lon, start_utc, end_utc, kwargs)
        return pd.DataFrame()

    monkeypatch.setattr(health_wx, "fetch_open_meteo_obs", capture_open_meteo)
    monkeypatch.setattr(health_wx, "fetch_meteostat_obs_near", capture_meteostat)
    monkeypatch.setattr(health_wx, "HAS_METEOSTAT", True)
    monkeypatch.setattr(
        health_wx,
        "now_utc",
        lambda: pd.Timestamp("2026-10-20T12:34:56Z"),
    )

    summary = health_wx.run_wx_health_check(
        lat=float("inf"),
        lon="bad",
        hours=0,
        radius_km=float("nan"),
        max_stations=2.5,
    )

    lat, lon, start_utc, end_utc, _ = calls["open_meteo"]
    assert (lat, lon) == (47.6062, -122.3321)
    assert end_utc - start_utc == pd.Timedelta(hours=6)
    assert calls["meteostat"][4] == {"radius_km": 75.0, "max_stations": 5}
    assert summary["params"] == {
        "lat": 47.6062,
        "lon": -122.3321,
        "hours": 6.0,
        "radius_km": 75.0,
        "max_stations": 5,
    }
    assert "Invalid weather health" in caplog.text


def test_health_check_rejects_boolean_runtime_parameters(monkeypatch, caplog):
    calls = {}

    def capture_open_meteo(lat, lon, start_utc, end_utc, **kwargs):
        calls["open_meteo"] = (lat, lon, start_utc, end_utc, kwargs)
        return pd.DataFrame()

    monkeypatch.setattr(health_wx, "fetch_open_meteo_obs", capture_open_meteo)
    monkeypatch.setattr(health_wx, "HAS_METEOSTAT", False)
    monkeypatch.setattr(
        health_wx,
        "now_utc",
        lambda: pd.Timestamp("2026-10-20T12:34:56Z"),
    )

    summary = health_wx.run_wx_health_check(
        lat=True,
        lon=False,
        hours=True,
        radius_km=False,
        max_stations=True,
    )

    lat, lon, start_utc, end_utc, _ = calls["open_meteo"]
    assert (lat, lon) == (47.6062, -122.3321)
    assert end_utc - start_utc == pd.Timedelta(hours=6)
    assert summary["params"] == {
        "lat": 47.6062,
        "lon": -122.3321,
        "hours": 6.0,
        "radius_km": 75.0,
        "max_stations": 5,
    }
    assert "Invalid weather health" in caplog.text


def test_health_check_rejects_numpy_boolean_runtime_parameters(monkeypatch):
    monkeypatch.setattr(
        health_wx,
        "fetch_open_meteo_obs",
        lambda *_args, **_kwargs: pd.DataFrame(),
    )
    monkeypatch.setattr(health_wx, "HAS_METEOSTAT", False)

    summary = health_wx.run_wx_health_check(
        lat=np.bool_(True),
        lon=np.bool_(False),
        hours=np.bool_(True),
        radius_km=np.bool_(False),
        max_stations=np.bool_(True),
    )

    assert summary["params"] == {
        "lat": 47.6062,
        "lon": -122.3321,
        "hours": 6.0,
        "radius_km": 75.0,
        "max_stations": 5,
    }


def test_health_entrypoint_leaves_values_for_safe_normalization(monkeypatch):
    monkeypatch.setenv("RDS_HEALTH_HOURS", "bad")
    monkeypatch.setenv("RDS_HEALTH_MAX_STATIONS", "2.5")
    monkeypatch.setenv("RDS_HEALTH_INCLUDE_MARINE", " No ")

    parameters = pipeline_health_entrypoint._health_parameters_from_env()

    assert parameters["hours"] == "bad"
    assert parameters["max_stations"] == "2.5"
    assert parameters["include_marine"] is False


def test_health_summary_normalizes_mixed_offsets_to_utc():
    observations = pd.DataFrame(
        {
            "valid_utc": [
                "2026-10-20T12:00:00Z",
                "2026-10-20T09:00:00-05:00",
            ],
            "lat": [37.5, 37.5],
            "lon": [-75.5, -75.5],
            "provider": ["test", "test"],
            "source_type": ["model", "model"],
        }
    )

    summary = health_wx._summarize_df(observations)

    assert summary["min_ts"] == pd.Timestamp("2026-10-20T12:00:00Z")
    assert summary["max_ts"] == pd.Timestamp("2026-10-20T14:00:00Z")
    assert summary["missing_cols"] == []


def test_health_summary_reports_unknown_span_for_invalid_timestamps():
    observations = pd.DataFrame(
        {"valid_utc": ["bad", None], "provider": ["test", "test"]}
    )

    summary = health_wx._summarize_df(observations)

    assert summary["rows"] == 2
    assert summary["min_ts"] is None
    assert summary["max_ts"] is None
    assert summary["missing_cols"] == ["lat", "lon", "source_type"]
