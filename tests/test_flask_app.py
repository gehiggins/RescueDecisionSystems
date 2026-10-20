from pathlib import Path

import pandas as pd

from flask_app.app import create_app


def test_application_factory_serves_home_page():
    application = create_app({"TESTING": True})

    response = application.test_client().get("/")

    assert response.status_code == 200
    assert b"Rescue Decision Systems" in response.data


def test_process_alert_rejects_non_json_request():
    application = create_app({"TESTING": True})

    response = application.test_client().post("/process_alert", data="alert")

    assert response.status_code == 400
    assert response.get_json()["error"] == "Invalid request format. Must be JSON."


def test_process_alert_returns_active_parser_shape(monkeypatch, tmp_path):
    application = create_app(
        {"TESTING": True, "RDS_MAPS_DIR": str(tmp_path / "maps")}
    )
    parsed = {
        "site_id": "123",
        "latitude_a": 37.5,
        "longitude_a": -75.5,
        "latitude_b": None,
        "longitude_b": None,
        "range_ring_meters_a": 0.0,
        "range_ring_meters_b": 0.0,
    }

    monkeypatch.setattr(
        "flask_app.app.routes.pre_scan_for_coordinates", lambda _message: object()
    )
    monkeypatch.setattr(
        "flask_app.app.routes.parse_sarsat_message",
        lambda _message, pre_scan_results: parsed,
    )

    def write_map(_alert, output_path):
        Path(output_path).write_text("<html>map</html>", encoding="utf-8")
        return output_path

    monkeypatch.setattr("flask_app.app.routes.generate_gis_map", write_map)

    response = application.test_client().post(
        "/process_alert", json={"message": "sample alert"}
    )

    assert response.status_code == 200
    body = response.get_json()
    assert body["success"] is True
    assert body["parsed_data"][0]["site_id"] == "123"
    assert body["map_url"] == "/maps/gis_map_123.html"


def test_map_generation_rejects_site_id_path_traversal(tmp_path):
    application = create_app(
        {"TESTING": True, "RDS_MAPS_DIR": str(tmp_path / "maps")}
    )

    response = application.test_client().get(
        "/generate_gis_map/37.5/-75.5/..%2Foutside"
    )

    assert response.status_code == 400
    assert not (tmp_path / "outside").exists()


def test_full_pipeline_import_and_sequence_fallback_without_database(monkeypatch):
    """Local alert processing must not require a configured database."""
    monkeypatch.delenv("DATABASE_URL", raising=False)

    from flask_app.app import pipeline_controller

    existing_alerts = pipeline_controller._get_existing_alerts_for_sequence()

    assert isinstance(existing_alerts, pd.DataFrame)
    assert existing_alerts.empty
    assert list(existing_alerts.columns) == ["site_id", "alert_sequence_number"]
