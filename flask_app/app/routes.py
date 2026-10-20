"""Web routes for the RDS prototype interface."""

from pathlib import Path
import math
import re

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


def _json_compatible(value):
    """Convert pandas/NumPy scalar values returned by the parser to JSON types."""
    if value is None:
        return None
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float) and math.isnan(value):
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _serialize_parsed_alert(parsed):
    return {key: _json_compatible(value) for key, value in parsed.items()}


def _map_path(site_id):
    if not re.fullmatch(r"[A-Za-z0-9_-]+", str(site_id)):
        abort(400, description="Invalid site ID")
    maps_dir = Path(current_app.config["RDS_MAPS_DIR"])
    maps_dir.mkdir(parents=True, exist_ok=True)
    return maps_dir / f"gis_map_{site_id}.html"


def _coordinates_are_valid(latitude, longitude):
    """Return whether a coordinate pair is finite and within Earth bounds."""
    return (
        math.isfinite(latitude)
        and math.isfinite(longitude)
        and -90.0 <= latitude <= 90.0
        and -180.0 <= longitude <= 180.0
    )


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

    payload = request.get_json(silent=True) or {}
    raw_message = payload.get("message")
    if not isinstance(raw_message, str) or not raw_message.strip():
        return jsonify({"error": "No message provided"}), 400
    max_alert_chars = current_app.config["RDS_MAX_ALERT_CHARS"]
    if len(raw_message) > max_alert_chars:
        return jsonify(
            {
                "error": (
                    "Alert message is too large. "
                    f"Maximum length is {max_alert_chars} characters."
                )
            }
        ), 413

    maps_dir = Path(current_app.config["RDS_MAPS_DIR"]).resolve()
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
    if result.get("map_html_path"):
        map_path = Path(result["map_html_path"]).resolve()
        try:
            relative_map_path = map_path.relative_to(maps_dir)
        except ValueError:
            current_app.logger.error(
                "Alert pipeline returned a map outside RDS_MAPS_DIR: %s", map_path
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
            "layers": result.get("layers", []),
            "weather_rows": result.get("weather_rows", 0),
            "satellite_rows": result.get("satellite_rows", 0),
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
    output_path = _map_path(site_id)
    generate_gis_map(alert_row, str(output_path))
    return redirect(url_for("main.serve_map", filename=output_path.name))


@main_bp.route("/maps/<path:filename>")
def serve_map(filename):
    maps_dir = Path(current_app.config["RDS_MAPS_DIR"]).resolve()
    requested_path = (maps_dir / filename).resolve()
    try:
        relative_path = requested_path.relative_to(maps_dir)
    except ValueError:
        return abort(404, description="Map not found")
    if not requested_path.is_file():
        return abort(404, description="Map not found")
    return send_from_directory(maps_dir, relative_path.as_posix())
