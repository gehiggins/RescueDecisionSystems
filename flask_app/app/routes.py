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
from .parser_sarsat_msg import parse_sarsat_message
from .preparse_coordinate_mapper import pre_scan_for_coordinates


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
    """Parse one alert and create its basic GIS map without writing to the DB."""
    if not request.is_json:
        return jsonify({"error": "Invalid request format. Must be JSON."}), 400

    payload = request.get_json(silent=True) or {}
    raw_message = payload.get("message")
    if not isinstance(raw_message, str) or not raw_message.strip():
        return jsonify({"error": "No message provided"}), 400

    pre_scan_results = pre_scan_for_coordinates(raw_message)
    parsed = parse_sarsat_message(raw_message, pre_scan_results=pre_scan_results)
    if not parsed:
        return jsonify({"error": "Message parsing failed"}), 422

    parsed_data = _serialize_parsed_alert(parsed)
    map_url = None
    site_id = parsed_data.get("site_id")

    # A parsed alert may be useful even if it has no location or map identifier.
    if site_id and (
        parsed_data.get("latitude_a") is not None
        or parsed_data.get("latitude_b") is not None
    ):
        output_path = _map_path(site_id)
        generate_gis_map(parsed_data, str(output_path))
        map_url = url_for("main.serve_map", filename=output_path.name)

    return jsonify(
        {
            "success": True,
            "parsed_data": [parsed_data],
            "map_url": map_url,
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
    maps_dir = Path(current_app.config["RDS_MAPS_DIR"])
    if not (maps_dir / filename).is_file():
        return abort(404, description="Map not found")
    return send_from_directory(maps_dir, filename)
