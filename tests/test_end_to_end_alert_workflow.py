"""End-to-end coverage for the browser-facing sample-alert workflow."""

import importlib
from pathlib import Path

import pandas as pd

from flask_app.app import create_app
from flask_app.app import pipeline_controller


def test_sample_alert_processes_to_retrievable_map_without_live_providers(
    monkeypatch, tmp_path
):
    repository_root = Path(__file__).resolve().parents[1]
    raw_alert = (repository_root / "sample_sarsat_message.txt").read_text(
        encoding="utf-8-sig"
    )
    captured_satellite_alert = {}
    sat_pipeline = importlib.import_module("app.sat_pipeline")
    wx_obs = importlib.import_module("app.wx_obs")

    def capture_satellite_alert(alert_df, **_kwargs):
        captured_satellite_alert.update(alert_df.iloc[0].to_dict())
        return pd.DataFrame()

    # This test exercises every in-process boundary while replacing only
    # external data providers and diagnostic writes with deterministic inputs.
    monkeypatch.setattr(pipeline_controller, "dump_dataframe", lambda *_a, **_k: None)
    monkeypatch.setattr(
        pipeline_controller,
        "_get_existing_alerts_for_sequence",
        lambda: pd.DataFrame(columns=["site_id", "alert_sequence_number"]),
    )
    monkeypatch.setattr(
        pipeline_controller,
        "run_wx_pipeline",
        lambda **_kwargs: wx_obs._empty_wx_obs(),
    )
    monkeypatch.setattr(
        pipeline_controller, "load_sat_overlays_for_site", lambda _site_id: []
    )
    monkeypatch.setattr(sat_pipeline, "build_sat_overlay_df", capture_satellite_alert)

    maps_dir = tmp_path / "maps"
    application = create_app(
        {"TESTING": True, "RDS_MAPS_DIR": str(maps_dir)}
    )
    client = application.test_client()

    response = client.post("/process_alert", json={"message": raw_alert})

    assert response.status_code == 200
    body = response.get_json()
    assert body["success"] is True
    parsed = body["parsed_data"][0]
    assert parsed["site_id"] == "98372"
    assert parsed["satellite_name"] == "GOES-16"
    assert parsed["position_device"] == "INTERNAL"
    assert parsed["detection_frequency"] == 406.0285
    assert body["layers"] == ["Alert Positions", "Range Rings"]
    assert body["weather_rows"] == 0
    assert body["satellite_rows"] == 0
    assert body["map_url"] == "/maps/98372/gis_map_98372.html"

    map_response = client.get(body["map_url"])
    assert map_response.status_code == 200
    assert b"leaflet" in map_response.data.lower()
    assert map_response.headers["Cache-Control"] == "no-store"

    assert captured_satellite_alert["satellite_name"] == "GOES-16"
    assert captured_satellite_alert["satellite_id"] == "GOES-16"
    assert captured_satellite_alert["alert_time_utc"] == pd.Timestamp(
        "2025-03-02T13:32:17Z"
    )
