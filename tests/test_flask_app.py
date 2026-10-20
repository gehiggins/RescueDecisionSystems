from pathlib import Path
import re
from urllib.parse import unquote

import pandas as pd
import pytest

from flask_app.app import create_app


def test_application_factory_serves_home_page():
    application = create_app({"TESTING": True})

    response = application.test_client().get("/")

    assert response.status_code == 200
    assert b"Rescue Decision Systems" in response.data
    assert response.headers["X-Frame-Options"] == "SAMEORIGIN"
    assert response.headers["X-Content-Type-Options"] == "nosniff"
    assert response.headers["Referrer-Policy"] == "same-origin"


@pytest.mark.parametrize(
    "page_path",
    ["/", "/sarsat-analysis", "/our-process", "/future-projects"],
)
def test_rendered_pages_reference_existing_static_assets(page_path):
    application = create_app({"TESTING": True})

    response = application.test_client().get(page_path)

    assert response.status_code == 200
    static_urls = re.findall(
        rb'(?:href|src)="(/static/[^"]+)"', response.data
    )
    assert static_urls
    static_root = Path(application.static_folder)
    for encoded_url in static_urls:
        relative_path = unquote(encoded_url.decode("utf-8"))[len("/static/"):]
        assert (static_root / relative_path).is_file(), encoded_url


def test_analysis_page_has_existing_placeholder_and_processing_status():
    application = create_app({"TESTING": True})

    response = application.test_client().get("/sarsat-analysis")

    assert response.status_code == 200
    assert b"images/alerts_map.webp" in response.data
    assert b'id="processing-status"' in response.data
    assert b"console.log" not in response.data


def test_process_alert_rejects_non_json_request():
    application = create_app({"TESTING": True})

    response = application.test_client().post("/process_alert", data="alert")

    assert response.status_code == 400
    assert response.get_json()["error"] == "Invalid request format. Must be JSON."


def test_process_alert_rejects_blank_message():
    application = create_app({"TESTING": True})

    response = application.test_client().post(
        "/process_alert", json={"message": "   "}
    )

    assert response.status_code == 400
    assert response.get_json()["error"] == "No message provided"
    assert response.headers["Cache-Control"] == "no-store"


def test_process_alert_rejects_message_over_configured_limit(monkeypatch):
    application = create_app({"TESTING": True, "RDS_MAX_ALERT_CHARS": 10})
    pipeline_called = False

    def run_pipeline(_message, maps_root):
        nonlocal pipeline_called
        pipeline_called = True

    monkeypatch.setattr("flask_app.app.routes.run_alert_pipeline", run_pipeline)

    response = application.test_client().post(
        "/process_alert", json={"message": "x" * 11}
    )

    assert response.status_code == 413
    assert response.get_json()["error"].endswith("10 characters.")
    assert pipeline_called is False


def test_process_alert_returns_json_when_request_body_is_too_large():
    application = create_app({"TESTING": True, "MAX_CONTENT_LENGTH": 32})

    response = application.test_client().post(
        "/process_alert", json={"message": "x" * 64}
    )

    assert response.status_code == 413
    assert response.get_json()["error"] == (
        "Request is too large to process as a SARSAT alert."
    )


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


def test_process_alert_does_not_expose_pipeline_failure(monkeypatch, caplog):
    application = create_app({"TESTING": True})
    monkeypatch.setattr(
        "flask_app.app.routes.run_alert_pipeline",
        lambda _message, maps_root: {"status": "error", "error": "weather failed"},
    )

    response = application.test_client().post(
        "/process_alert", json={"message": "sample alert"}
    )

    assert response.status_code == 500
    assert response.get_json()["error"] == "Alert processing failed"
    assert "weather failed" in caplog.text


def test_process_alert_returns_json_when_pipeline_raises(monkeypatch, caplog):
    application = create_app({"TESTING": True})

    def raise_pipeline_error(_message, maps_root):
        raise RuntimeError("private provider detail")

    monkeypatch.setattr(
        "flask_app.app.routes.run_alert_pipeline", raise_pipeline_error
    )

    response = application.test_client().post(
        "/process_alert", json={"message": "sample alert"}
    )

    assert response.status_code == 500
    assert response.is_json
    assert response.get_json() == {"error": "Alert processing failed"}
    assert b"private provider detail" not in response.data
    assert "private provider detail" in caplog.text


def test_process_alert_rejects_invalid_success_payload(monkeypatch, caplog):
    application = create_app({"TESTING": True})
    monkeypatch.setattr(
        "flask_app.app.routes.run_alert_pipeline",
        lambda _message, maps_root: {"status": "ok", "parsed_data": None},
    )

    response = application.test_client().post(
        "/process_alert", json={"message": "sample alert"}
    )

    assert response.status_code == 500
    assert response.get_json()["error"] == "Alert processing failed"
    assert "invalid parsed data" in caplog.text


def test_map_generation_rejects_site_id_path_traversal(tmp_path):
    application = create_app(
        {"TESTING": True, "RDS_MAPS_DIR": str(tmp_path / "maps")}
    )

    response = application.test_client().get(
        "/generate_gis_map/37.5/-75.5/..%2Foutside"
    )

    assert response.status_code == 400
    assert not (tmp_path / "outside").exists()


def test_generated_map_response_is_not_cached(tmp_path):
    maps_dir = tmp_path / "maps"
    map_path = maps_dir / "98372" / "gis_map_98372.html"
    map_path.parent.mkdir(parents=True)
    map_path.write_text("<html>map</html>", encoding="utf-8")
    application = create_app(
        {"TESTING": True, "RDS_MAPS_DIR": str(maps_dir)}
    )

    response = application.test_client().get(
        "/maps/98372/gis_map_98372.html"
    )

    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "no-store"
    assert response.headers["X-Frame-Options"] == "SAMEORIGIN"


def test_map_route_rejects_symlink_outside_map_directory(tmp_path):
    maps_dir = tmp_path / "maps"
    maps_dir.mkdir()
    outside_map = tmp_path / "outside.html"
    outside_map.write_text("not an RDS map", encoding="utf-8")
    (maps_dir / "linked.html").symlink_to(outside_map)
    application = create_app(
        {"TESTING": True, "RDS_MAPS_DIR": str(maps_dir)}
    )

    response = application.test_client().get("/maps/linked.html")

    assert response.status_code == 404
    assert b"not an RDS map" not in response.data


@pytest.mark.parametrize(
    ("latitude", "longitude"),
    [
        ("91", "0"),
        ("-91", "0"),
        ("0", "181"),
        ("0", "-181"),
        ("nan", "0"),
        ("0", "inf"),
    ],
)
def test_map_generation_rejects_invalid_coordinate_bounds(
    latitude, longitude, monkeypatch
):
    application = create_app({"TESTING": True})
    map_called = False

    def generate_map(_alert_row, _output_path):
        nonlocal map_called
        map_called = True

    monkeypatch.setattr("flask_app.app.routes.generate_gis_map", generate_map)

    response = application.test_client().get(
        f"/generate_gis_map/{latitude}/{longitude}/98372"
    )

    assert response.status_code == 400
    assert response.get_json()["error"] == "Coordinates are outside valid bounds"
    assert map_called is False


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
