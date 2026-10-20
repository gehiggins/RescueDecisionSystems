"""Web routes for the RDS prototype interface."""

from pathlib import Path
import math
import os
import re

import pandas as pd

from flask import (
    Blueprint,
    abort,
    current_app,
    jsonify,
    redirect,
    render_template,
    request,
    send_from_directory,
    url_for,
)

from .gis_mapping import generate_gis_map
from .pipeline_controller import process_sarsat_alert as run_alert_pipeline


main_bp = Blueprint("main", __name__)

_MAX_MAP_SITE_ID_CHARS = 80
_DEFAULT_MAX_ALERT_CHARS = 100_000
_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_MAPS_DIR = _REPOSITORY_ROOT / "data" / "maps"


def _json_compatible(value):
    """Recursively convert pandas/NumPy values to strict JSON-compatible types."""
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, dict):
        return {key: _json_compatible(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_compatible(item) for item in value]
    # NumPy arrays and pandas containers expose ``tolist``; convert them before
    # scalar ``item`` handling so a multi-element array cannot raise here.
    if not pd.api.types.is_scalar(value) and hasattr(value, "tolist"):
        try:
            return _json_compatible(value.tolist())
        except (TypeError, ValueError):
            pass
    if hasattr(value, "item"):
        try:
            value = value.item()
        except (TypeError, ValueError):
            pass
        else:
            return _json_compatible(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _serialize_parsed_alert(parsed):
    return {key: _json_compatible(value) for key, value in parsed.items()}


def _map_path(site_id):
    normalized_site_id = str(site_id)
    if (
        len(normalized_site_id) > _MAX_MAP_SITE_ID_CHARS
        or not re.fullmatch(r"[A-Za-z0-9_-]+", normalized_site_id)
    ):
        abort(400, description="Invalid site ID")
    maps_dir = _maps_directory()
    maps_dir.mkdir(parents=True, exist_ok=True)
    output_path = maps_dir / f"gis_map_{normalized_site_id}.html"
    if output_path.is_symlink():
        raise ValueError("Map output file cannot be a symbolic link")
    return output_path


def _coordinates_are_valid(latitude, longitude):
    """Return whether a coordinate pair is finite and within Earth bounds."""
    return (
        math.isfinite(latitude)
        and math.isfinite(longitude)
        and -90.0 <= latitude <= 90.0
        and -180.0 <= longitude <= 180.0
    )


def _alert_character_limit():
    """Return a usable configured request limit without crashing the route."""
    configured = current_app.config.get(
        "RDS_MAX_ALERT_CHARS", _DEFAULT_MAX_ALERT_CHARS
    )
    if not isinstance(configured, bool):
        try:
            numeric = float(configured)
        except (TypeError, ValueError, OverflowError):
            numeric = float("nan")
        if math.isfinite(numeric) and numeric >= 1 and numeric.is_integer():
            return int(numeric)
    current_app.logger.warning(
        "Invalid RDS_MAX_ALERT_CHARS=%r; using %s",
        configured,
        _DEFAULT_MAX_ALERT_CHARS,
    )
    return _DEFAULT_MAX_ALERT_CHARS


def _maps_directory():
    """Resolve a dedicated map directory without trusting caller CWD."""
    configured = current_app.config.get("RDS_MAPS_DIR", _DEFAULT_MAPS_DIR)
    valid_type = isinstance(configured, (str, os.PathLike)) and not isinstance(
        configured, bool
    )
    if valid_type and str(configured).strip():
        candidate = Path(configured).expanduser()
        if not candidate.is_absolute():
            candidate = _REPOSITORY_ROOT / candidate
        resolved = candidate.resolve()
        # A maps root that contains the whole checkout could expose unrelated
        # HTML files through the public route. Require a dedicated descendant
        # or an explicitly configured external directory instead.
        try:
            _REPOSITORY_ROOT.relative_to(resolved)
            contains_repository = True
        except ValueError:
            contains_repository = False
        if not contains_repository:
            return resolved
    current_app.logger.warning(
        "Invalid or overly broad RDS_MAPS_DIR=%r; using %s",
        configured,
        _DEFAULT_MAPS_DIR,
    )
    return _DEFAULT_MAPS_DIR.resolve()


@main_bp.route("/")
def index():
    return render_template("index.html")


@main_bp.route("/sarsat-analysis")
def sarsat_analysis():
    return render_template("sarsat_analysis.html")


@main_bp.route("/our-process")
def our_process():
    return render_template("our_process.html")


@main_bp.route("/future-projects")
def future_projects():
    return render_template("future_projects.html")


@main_bp.route("/contact")
def contact():
    # Contact information lives on the home page; there is no contact template.
    return redirect(url_for("main.index", _anchor="contact"))


@main_bp.route("/process_alert", methods=["POST"])
def process_alert():
    """Process one alert through the full weather, satellite, and GIS pipeline."""
    if not request.is_json:
        return jsonify({"error": "Invalid request format. Must be JSON."}), 400

    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"error": "Invalid request format. Must be a JSON object."}), 400
    raw_message = payload.get("message")
    if not isinstance(raw_message, str) or not raw_message.strip():
        return jsonify({"error": "No message provided"}), 400
    max_alert_chars = _alert_character_limit()
    if len(raw_message) > max_alert_chars:
        return jsonify(
            {
                "error": (
                    "Alert message is too large. "
                    f"Maximum length is {max_alert_chars} characters."
                )
            }
        ), 413

    maps_dir = _maps_directory()
    try:
        result = run_alert_pipeline(raw_message, maps_root=maps_dir)
    except Exception:
        # Keep the traceback in server logs while preserving the JSON contract
        # expected by the browser and avoiding disclosure of internal details.
        current_app.logger.exception("Alert pipeline raised an exception")
        return jsonify({"error": "Alert processing failed"}), 500
    if not isinstance(result, dict):
        current_app.logger.error(
            "Alert pipeline returned %s instead of a result mapping",
            type(result).__name__,
        )
        return jsonify({"error": "Alert processing failed"}), 500
    if result.get("status") != "ok":
        error = result.get("error", "Alert processing failed")
        if error == "Message parsing failed":
            return jsonify({"error": error}), 422
        current_app.logger.error("Alert pipeline failed: %s", error)
        return jsonify({"error": "Alert processing failed"}), 500

    if not isinstance(result.get("parsed_data"), dict):
        current_app.logger.error("Alert pipeline returned invalid parsed data")
        return jsonify({"error": "Alert processing failed"}), 500
    parsed_data = _serialize_parsed_alert(result["parsed_data"])
    map_url = None
    reported_map_path = result.get("map_html_path")
    if reported_map_path is not None:
        valid_path_type = isinstance(
            reported_map_path, (str, os.PathLike)
        ) and not isinstance(reported_map_path, bool)
        if not valid_path_type or not str(reported_map_path).strip():
            current_app.logger.error(
                "Alert pipeline returned an invalid map path: %r", reported_map_path
            )
            return jsonify({"error": "Alert processing failed"}), 500
        map_path = Path(reported_map_path).resolve()
        try:
            relative_map_path = map_path.relative_to(maps_dir)
        except ValueError:
            current_app.logger.error(
                "Alert pipeline returned a map outside RDS_MAPS_DIR: %s", map_path
            )
            return jsonify({"error": "Alert processing failed"}), 500
        if relative_map_path.suffix.lower() != ".html":
            current_app.logger.error(
                "Alert pipeline returned a non-HTML map file: %s", map_path
            )
            return jsonify({"error": "Alert processing failed"}), 500
        if not map_path.is_file():
            current_app.logger.error(
                "Alert pipeline did not create its reported map: %s", map_path
            )
            return jsonify({"error": "Alert processing failed"}), 500
        map_url = url_for(
            "main.serve_map", filename=relative_map_path.as_posix()
        )

    return jsonify(
        {
            "success": True,
            "parsed_data": [parsed_data],
            "map_url": map_url,
            "layers": _json_compatible(result.get("layers", [])),
            "weather_rows": _json_compatible(result.get("weather_rows", 0)),
            "satellite_rows": _json_compatible(
                result.get("satellite_rows", 0)
            ),
        }
    )


@main_bp.route(
    "/generate_gis_map/<path:latitude>/<path:longitude>/<path:site_id>",
    methods=["GET"],
)
def gis_map(latitude, longitude, site_id):
    """Generate a basic map for a coordinate supplied by the web interface."""
    try:
        latitude_value = float(latitude)
        longitude_value = float(longitude)
    except ValueError:
        return jsonify({"error": "Invalid coordinates"}), 400
    if not _coordinates_are_valid(latitude_value, longitude_value):
        return jsonify({"error": "Coordinates are outside valid bounds"}), 400

    alert_row = {
        "site_id": str(site_id),
        "latitude_a": latitude_value,
        "longitude_a": longitude_value,
        "latitude_b": None,
        "longitude_b": None,
        "range_ring_meters_a": 0.0,
        "range_ring_meters_b": 0.0,
    }
    try:
        output_path = _map_path(site_id)
    except ValueError:
        current_app.logger.exception("Direct GIS map output path is unsafe")
        return jsonify({"error": "Map generation failed"}), 500
    try:
        generate_gis_map(alert_row, str(output_path))
    except Exception:
        current_app.logger.exception("Direct GIS map generation failed")
        return jsonify({"error": "Map generation failed"}), 500
    if not output_path.is_file():
        current_app.logger.error(
            "Direct GIS map generation did not create output: %s", output_path
        )
        return jsonify({"error": "Map generation failed"}), 500
    return redirect(url_for("main.serve_map", filename=output_path.name))


@main_bp.route("/maps/<path:filename>")
def serve_map(filename):
    maps_dir = _maps_directory()
    requested_path = (maps_dir / filename).resolve()
    if requested_path.suffix.lower() != ".html":
        return abort(404, description="Map not found")
    try:
        relative_path = requested_path.relative_to(maps_dir)
    except ValueError:
        return abort(404, description="Map not found")
    if not requested_path.is_file():
        return abort(404, description="Map not found")
    return send_from_directory(maps_dir, relative_path.as_posix())
