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
    def run_pipeline(_message, maps_root):
        output_path = Path(maps_root) / "123" / "gis_map_123.html"
        output_path.parent.mkdir(parents=True)
        output_path.write_text("<html>map</html>", encoding="utf-8")
        return {
            "status": "ok",
            "parsed_data": {
                "site_id": "123",
                "latitude_a": 37.5,
                "longitude_a": -75.5,
            },
            "map_html_path": str(output_path),
            "layers": ["Alert Positions", "Weather"],
            "weather_rows": 1,
            "satellite_rows": 0,
        }

    monkeypatch.setattr("flask_app.app.routes.run_alert_pipeline", run_pipeline)

    response = application.test_client().post(
        "/process_alert", json={"message": "sample alert"}
    )

    assert response.status_code == 200
    body = response.get_json()
    assert body["success"] is True
    assert body["parsed_data"][0]["site_id"] == "123"
    assert body["map_url"] == "/maps/123/gis_map_123.html"
    assert body["layers"] == ["Alert Positions", "Weather"]
    assert body["weather_rows"] == 1


def test_process_alert_reports_full_pipeline_failure(monkeypatch):
    application = create_app({"TESTING": True})
    monkeypatch.setattr(
        "flask_app.app.routes.run_alert_pipeline",
        lambda _message, maps_root: {"status": "error", "error": "weather failed"},
    )

    response = application.test_client().post(
        "/process_alert", json={"message": "sample alert"}
    )

    assert response.status_code == 500
    assert response.get_json()["error"] == "weather failed"


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


def test_missing_satellite_overlay_does_not_create_placeholder(monkeypatch, tmp_path):
    from flask_app.app import pipeline_controller

    monkeypatch.chdir(tmp_path)

    assert pipeline_controller.load_sat_overlays_for_site("98372") == []


def test_model_spot_predictions_are_added_to_map_weather_rows():
    from flask_app.app import pipeline_controller

    observations = pd.DataFrame(
        [
            {
                "lat": 37.76,
                "lon": -75.50,
                "valid_utc": "2026-10-19T18:00:00Z",
                "temp_c": 18.0,
                "wind_ms": 7.0,
                "source_type": "model",
            },
            {
                "lat": 37.50,
                "lon": -75.25,
                "source_type": "station",
            },
        ]
    )

    result = pipeline_controller._append_model_spot_predictions(
        observations, pd.DataFrame()
    )

    assert len(result) == 1
    assert result.iloc[0]["source_type"] == "model_interpolated"
    assert result.iloc[0]["lat_dd"] == 37.76
    assert result.iloc[0]["temp_C"] == 18.0


def test_weather_source_ids_fall_back_to_provider_and_coordinates():
    from flask_app.app import pipeline_controller

    observations = pd.DataFrame(
        [
            {"provider": "Open-Meteo", "lat": 37.76, "lon": -75.50},
            {"provider": "Open-Meteo", "lat": 38.25, "lon": -70.20},
        ]
    )

    identifiers = pipeline_controller._weather_source_ids(observations)

    assert identifiers.is_unique
    assert identifiers.iloc[0] == "Open-Meteo:37.76:-75.5"
