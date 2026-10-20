import pytest
import pandas as pd
from app.gis_map_inputs_builder import build_gis_map_inputs_df
import importlib

def make_positions_df():
    return pd.DataFrame([{
        "site_id": "TEST",
        "role": "A",
        "lat_dd": 37.7749,
        "lon_dd": -122.4194,
        "ts_utc": "2025-09-15T12:00:00Z",
        "range_ring_meters": 1000
    }])

def make_wx_df():
    return pd.DataFrame([{
        "lat_dd": 37.7749,
        "lon_dd": -122.4194,
        "obs_type": "wave_height_m",
        "obs_value": 2.0,
        "temp_C": 20.0,
        "obs_time": "2025-09-15T12:00:00Z",
        "station_id": "WX1"
    }, {
        "lat_dd": 37.7749,
        "lon_dd": -122.4194,
        "obs_type": "wind_ms",
        "obs_value": 5.0,
        "temp_C": 20.0,
        "obs_time": "2025-09-15T12:00:00Z",
        "station_id": "WX1"
    }])

def make_stations_df():
    return pd.DataFrame([{
        "station_id": "ST1",
        "name": "Station One",
        "type": "station",
        "lat_dd": 37.7749,
        "lon_dd": -122.4194
    }])

def test_gis_map_inputs_all_layers(monkeypatch):
    positions_df = make_positions_df()
    wx_df = make_wx_df()
    stations_df = make_stations_df()
    sat_pipeline = importlib.import_module("app.sat_pipeline")

    def unexpected_satellite_fetch(*_args, **_kwargs):
        pytest.fail("satellite pipeline ran without an alert timestamp")

    monkeypatch.setattr(
        sat_pipeline, "build_sat_overlay_df", unexpected_satellite_fetch
    )

    df = build_gis_map_inputs_df(positions_df, wx_df, stations_df, op_tz_env="UTC")
    assert not df.empty
    required_cols = [
        'site_id','layer','geom_type','geometry','ts_utc','ts_local','local_tz',
        'label','popup_html','style_hint','source_table','source_id','is_maritime',
        'range_ring_meters','wave_height_m','wind_ms','temp_C',
        'wave_height_display','wind_display','temp_display'
    ]
    for col in required_cols:
        assert col in df.columns

    # Should have one alert_position, one range_ring, two weather (wave/wind), one station
    layers = df["layer"].value_counts().to_dict()
    assert layers.get("alert_position", 0) == 1
    assert layers.get("range_ring", 0) == 1
    assert layers.get("weather", 0) == 2
    assert layers.get("station", 0) == 1

    # Dual time fields present and correct
    for _, row in df.iterrows():
        if row["layer"] in ("alert_position", "weather"):
            assert row["ts_utc"] is None or isinstance(row["ts_utc"], str)
            assert row["ts_local"] is None or isinstance(row["ts_local"], str)
            assert row["local_tz"] == "UTC"

    # Display fields formatted
    wx_rows = df[df["layer"] == "weather"]
    found_ft = any(str(r.get("wave_height_display", "")).endswith("ft") for _, r in wx_rows.iterrows())
    found_kt = any(str(r.get("wind_display", "")).endswith("kt") for _, r in wx_rows.iterrows())
    assert found_ft
    assert found_kt

    # Range ring geometry
    ring_rows = df[df["layer"] == "range_ring"]
    for _, r in ring_rows.iterrows():
        geom = r["geometry"]
        assert isinstance(geom, dict)
        assert geom.get("type") == "Circle"
        assert "center" in geom and "radius_m" in geom

def test_gis_map_inputs_missing_layers():
    positions_df = make_positions_df()
    # wx_df missing
    df = build_gis_map_inputs_df(positions_df, None, None, op_tz_env="UTC")
    layers = df["layer"].value_counts().to_dict()
    assert layers.get("alert_position", 0) == 1
    assert layers.get("range_ring", 0) == 1
    assert "weather" not in layers
    assert "station" not in layers

    # stations_df missing
    wx_df = make_wx_df()
    df2 = build_gis_map_inputs_df(positions_df, wx_df, None, op_tz_env="UTC")
    layers2 = df2["layer"].value_counts().to_dict()
    assert layers2.get("alert_position", 0) == 1
    assert layers2.get("range_ring", 0) == 1
    assert layers2.get("weather", 0) == 2
    assert "station" not in layers2

def test_gis_map_inputs_empty_inputs():
    positions_df = pd.DataFrame()
    wx_df = pd.DataFrame()
    stations_df = pd.DataFrame()
    df = build_gis_map_inputs_df(positions_df, wx_df, stations_df, op_tz_env="UTC")
    assert df.empty


def test_satellite_overlay_uses_alert_detection_time(monkeypatch):
    captured = {}
    sat_pipeline = importlib.import_module("app.sat_pipeline")

    def capture_alert_time(alert_df, **_kwargs):
        captured["alert_time_utc"] = alert_df.iloc[0]["alert_time_utc"]
        return pd.DataFrame()

    monkeypatch.setattr(sat_pipeline, "build_sat_overlay_df", capture_alert_time)

    build_gis_map_inputs_df(
        make_positions_df(),
        alert_time_utc="2025-03-02T13:32:17Z",
        op_tz_env="UTC",
    )

    assert captured["alert_time_utc"] == pd.Timestamp("2025-03-02T13:32:17Z")


def test_invalid_map_coordinates_and_radii_are_removed():
    positions_df = pd.DataFrame(
        [
            {
                "site_id": "TEST",
                "role": "A",
                "lat_dd": 37.0,
                "lon_dd": -75.0,
                "range_ring_meters": 0.0,
            },
            {
                "site_id": "TEST",
                "role": "B",
                "lat_dd": 91.0,
                "lon_dd": -75.0,
                "range_ring_meters": 1000.0,
            },
        ]
    )
    wx_df = pd.DataFrame(
        [{"station_id": "BAD", "lat_dd": float("inf"), "lon_dd": -75.0}]
    )
    stations_df = pd.DataFrame(
        [{"station_id": "BAD2", "lat_dd": 37.0, "lon_dd": -181.0}]
    )

    result = build_gis_map_inputs_df(
        positions_df, wx_df, stations_df, op_tz_env="UTC"
    )

    assert result["layer"].tolist() == ["alert_position"]
    assert result.iloc[0]["geometry"]["coordinates"] == [-75.0, 37.0]
