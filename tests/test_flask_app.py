from pathlib import Path
import re
from urllib.parse import unquote

import numpy as np
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
    assert b'contentType.includes("application/json")' in response.data
    assert b"The server returned an invalid response." in response.data
    assert b'if (data.map_url && (hasPositionA || hasPositionB))' in response.data
    assert b'parsedAlert.site_id' not in response.data
    assert b'mapFrame.removeAttribute("src")' in response.data
    assert b'id="gis-map-frame" src=""' not in response.data
    assert b"Parsed Alert Summary" in response.data
    assert b"Distress Probability Report" not in response.data
    assert b"nearest RCCs" not in response.data


def test_process_alert_rejects_non_json_request():
    application = create_app({"TESTING": True})

    response = application.test_client().post("/process_alert", data="alert")

    assert response.status_code == 400
    assert response.get_json()["error"] == "Invalid request format. Must be JSON."


@pytest.mark.parametrize("payload", [[], ["message"], "message", 42])
def test_process_alert_rejects_json_that_is_not_an_object(payload):
    application = create_app({"TESTING": True})

    response = application.test_client().post("/process_alert", json=payload)

    assert response.status_code == 400
    assert response.get_json()["error"] == (
        "Invalid request format. Must be a JSON object."
    )


def test_process_alert_rejects_json_null():
    application = create_app({"TESTING": True})

    response = application.test_client().post(
        "/process_alert", data="null", content_type="application/json"
    )

    assert response.status_code == 400
    assert response.get_json()["error"] == (
        "Invalid request format. Must be a JSON object."
    )


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


def test_process_alert_accepts_integer_text_character_limit(monkeypatch):
    application = create_app({"TESTING": True, "RDS_MAX_ALERT_CHARS": "10"})
    pipeline_called = False

    def run_pipeline(_message, maps_root):
        nonlocal pipeline_called
        pipeline_called = True

    monkeypatch.setattr("flask_app.app.routes.run_alert_pipeline", run_pipeline)

    response = application.test_client().post(
        "/process_alert", json={"message": "x" * 11}
    )

    assert response.status_code == 413
    assert pipeline_called is False


@pytest.mark.parametrize(
    "invalid_limit", ["bad", 0, -1, 2.5, float("inf"), True]
)
def test_process_alert_falls_back_from_invalid_character_limit(
    monkeypatch, caplog, invalid_limit
):
    application = create_app(
        {"TESTING": True, "RDS_MAX_ALERT_CHARS": invalid_limit}
    )
    monkeypatch.setattr(
        "flask_app.app.routes.run_alert_pipeline",
        lambda _message, maps_root: {
            "status": "ok",
            "parsed_data": {"site_id": "123"},
        },
    )

    response = application.test_client().post(
        "/process_alert", json={"message": "sample alert"}
    )

    assert response.status_code == 200
    assert "Invalid RDS_MAX_ALERT_CHARS" in caplog.text


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


def test_process_alert_serializes_pandas_missing_values(monkeypatch):
    application = create_app({"TESTING": True})
    monkeypatch.setattr(
        "flask_app.app.routes.run_alert_pipeline",
        lambda _message, maps_root: {
            "status": "ok",
            "parsed_data": {
                "site_id": "123",
                "latitude_b": pd.NA,
                "detect_time": pd.NaT,
            },
        },
    )

    response = application.test_client().post(
        "/process_alert", json={"message": "sample alert"}
    )

    assert response.status_code == 200
    parsed = response.get_json()["parsed_data"][0]
    assert parsed["latitude_b"] is None
    assert parsed["detect_time"] is None


@pytest.mark.parametrize("non_finite", [float("nan"), float("inf"), float("-inf")])
def test_process_alert_serializes_non_finite_numbers_as_null(
    monkeypatch, non_finite
):
    application = create_app({"TESTING": True})
    monkeypatch.setattr(
        "flask_app.app.routes.run_alert_pipeline",
        lambda _message, maps_root: {
            "status": "ok",
            "parsed_data": {"site_id": "123", "latitude_b": non_finite},
        },
    )

    response = application.test_client().post(
        "/process_alert", json={"message": "sample alert"}
    )

    assert response.status_code == 200
    assert response.get_json()["parsed_data"][0]["latitude_b"] is None
    assert b"NaN" not in response.data
    assert b"Infinity" not in response.data


def test_process_alert_recursively_serializes_numpy_and_missing_values(
    monkeypatch,
):
    application = create_app({"TESTING": True})
    monkeypatch.setattr(
        "flask_app.app.routes.run_alert_pipeline",
        lambda _message, maps_root: {
            "status": "ok",
            "parsed_data": {
                "site_id": "123",
                "nested": {"value": np.float64(float("nan"))},
                "values": [np.int64(2), pd.NA],
                "array": np.array([1.0, float("inf")]),
            },
            "layers": ("Alert Positions", np.str_("Weather")),
            "weather_rows": np.int64(2),
            "satellite_rows": np.int64(1),
        },
    )

    response = application.test_client().post(
        "/process_alert", json={"message": "sample alert"}
    )

    assert response.status_code == 200
    body = response.get_json()
    assert body["parsed_data"][0]["nested"]["value"] is None
    assert body["parsed_data"][0]["values"] == [2, None]
    assert body["parsed_data"][0]["array"] == [1.0, None]
    assert body["layers"] == ["Alert Positions", "Weather"]
    assert body["weather_rows"] == 2
    assert body["satellite_rows"] == 1
    assert b"NaN" not in response.data
    assert b"Infinity" not in response.data


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


@pytest.mark.parametrize("invalid_result", [None, [], "unexpected"])
def test_process_alert_rejects_non_mapping_pipeline_result(
    monkeypatch, caplog, invalid_result
):
    application = create_app({"TESTING": True})
    monkeypatch.setattr(
        "flask_app.app.routes.run_alert_pipeline",
        lambda _message, maps_root: invalid_result,
    )

    response = application.test_client().post(
        "/process_alert", json={"message": "sample alert"}
    )

    assert response.status_code == 500
    assert response.get_json() == {"error": "Alert processing failed"}
    assert "instead of a result mapping" in caplog.text


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


def test_process_alert_rejects_missing_reported_map(monkeypatch, caplog, tmp_path):
    maps_dir = tmp_path / "maps"
    application = create_app(
        {"TESTING": True, "RDS_MAPS_DIR": str(maps_dir)}
    )
    missing_map = maps_dir / "123" / "gis_map_123.html"
    monkeypatch.setattr(
        "flask_app.app.routes.run_alert_pipeline",
        lambda _message, maps_root: {
            "status": "ok",
            "parsed_data": {"site_id": "123"},
            "map_html_path": str(missing_map),
        },
    )

    response = application.test_client().post(
        "/process_alert", json={"message": "sample alert"}
    )

    assert response.status_code == 500
    assert response.get_json() == {"error": "Alert processing failed"}
    assert "did not create its reported map" in caplog.text


def test_map_generation_rejects_site_id_path_traversal(tmp_path):
    application = create_app(
        {"TESTING": True, "RDS_MAPS_DIR": str(tmp_path / "maps")}
    )

    response = application.test_client().get(
        "/generate_gis_map/37.5/-75.5/..%2Foutside"
    )

    assert response.status_code == 400
    assert not (tmp_path / "outside").exists()


def test_map_generation_rejects_overlong_site_id(monkeypatch, tmp_path):
    application = create_app(
        {"TESTING": True, "RDS_MAPS_DIR": str(tmp_path / "maps")}
    )
    renderer_called = False

    def generate_map(_alert_row, _output_path):
        nonlocal renderer_called
        renderer_called = True

    monkeypatch.setattr("flask_app.app.routes.generate_gis_map", generate_map)

    response = application.test_client().get(
        f"/generate_gis_map/37.5/-75.5/{'A' * 81}"
    )

    assert response.status_code == 400
    assert renderer_called is False
    assert not (tmp_path / "maps").exists()


def test_direct_map_generation_returns_json_when_renderer_raises(
    monkeypatch, caplog, tmp_path
):
    application = create_app(
        {"TESTING": True, "RDS_MAPS_DIR": str(tmp_path / "maps")}
    )

    def raise_render_error(_alert_row, _output_path):
        raise RuntimeError("private renderer detail")

    monkeypatch.setattr(
        "flask_app.app.routes.generate_gis_map", raise_render_error
    )

    response = application.test_client().get(
        "/generate_gis_map/37.5/-75.5/98372"
    )

    assert response.status_code == 500
    assert response.get_json() == {"error": "Map generation failed"}
    assert b"private renderer detail" not in response.data
    assert "private renderer detail" in caplog.text


def test_direct_map_generation_rejects_missing_output(monkeypatch, caplog, tmp_path):
    application = create_app(
        {"TESTING": True, "RDS_MAPS_DIR": str(tmp_path / "maps")}
    )
    monkeypatch.setattr(
        "flask_app.app.routes.generate_gis_map",
        lambda _alert_row, _output_path: None,
    )

    response = application.test_client().get(
        "/generate_gis_map/37.5/-75.5/98372"
    )

    assert response.status_code == 500
    assert response.get_json() == {"error": "Map generation failed"}
    assert "did not create output" in caplog.text


def test_direct_map_generation_rejects_output_file_symlink(
    monkeypatch, caplog, tmp_path
):
    maps_dir = tmp_path / "maps"
    maps_dir.mkdir()
    outside_map = tmp_path / "outside.html"
    outside_map.write_text("preserve me", encoding="utf-8")
    output_link = maps_dir / "gis_map_98372.html"
    try:
        output_link.symlink_to(outside_map)
    except OSError:
        pytest.skip("file symlinks are unavailable on this platform")
    application = create_app(
        {"TESTING": True, "RDS_MAPS_DIR": str(maps_dir)}
    )
    renderer_called = False

    def generate_map(_alert_row, _output_path):
        nonlocal renderer_called
        renderer_called = True

    monkeypatch.setattr("flask_app.app.routes.generate_gis_map", generate_map)

    response = application.test_client().get(
        "/generate_gis_map/37.5/-75.5/98372"
    )

    assert response.status_code == 500
    assert response.get_json() == {"error": "Map generation failed"}
    assert renderer_called is False
    assert outside_map.read_text(encoding="utf-8") == "preserve me"
    assert "output path is unsafe" in caplog.text


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


def test_local_satellite_overlay_is_typed_and_validated(monkeypatch, tmp_path):
    from flask_app.app import pipeline_controller

    overlays_root = tmp_path / "data" / "sat_overlays" / "98372"
    overlays_root.mkdir(parents=True)
    image_path = overlays_root / "coverage.png"
    image_path.write_bytes(b"png")
    image_path.with_suffix(".bounds.json").write_text(
        "[[35.0, -78.0], [40.0, -72.0]]", encoding="utf-8"
    )
    monkeypatch.setattr(
        pipeline_controller,
        "__file__",
        str(tmp_path / "flask_app" / "app" / "pipeline_controller.py"),
    )

    overlays = pipeline_controller.load_sat_overlays_for_site("98372")

    assert len(overlays) == 1
    assert overlays[0]["type"] == "ImageOverlay"
    assert overlays[0]["bounds"] == [[35.0, -78.0], [40.0, -72.0]]


@pytest.mark.parametrize(
    "bounds",
    [
        [[40.0, -78.0], [35.0, -72.0]],
        [[35.0, -181.0], [40.0, -72.0]],
        [[35.0, -78.0], [91.0, -72.0]],
        [[35.0, float("nan")], [40.0, -72.0]],
        [[35.0, -78.0]],
        "not-bounds",
    ],
)
def test_invalid_satellite_overlay_bounds_are_rejected(bounds):
    from flask_app.app import pipeline_controller

    assert pipeline_controller._valid_overlay_bounds(bounds) is False


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
