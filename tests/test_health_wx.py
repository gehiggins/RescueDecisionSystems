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


def test_health_entrypoint_leaves_values_for_safe_normalization(monkeypatch):
    monkeypatch.setenv("RDS_HEALTH_HOURS", "bad")
    monkeypatch.setenv("RDS_HEALTH_MAX_STATIONS", "2.5")
    monkeypatch.setenv("RDS_HEALTH_INCLUDE_MARINE", " No ")

    parameters = pipeline_health_entrypoint._health_parameters_from_env()

    assert parameters["hours"] == "bad"
    assert parameters["max_stations"] == "2.5"
    assert parameters["include_marine"] is False
