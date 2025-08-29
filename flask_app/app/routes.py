#routes.py

import os

from flask import Blueprint, render_template, request, jsonify, send_file, send_from_directory, current_app, redirect, url_for, abort
from flask_app.app.parser import parse_sarsat_message  # Ensure proper import
from flask_app.app.database import save_alert_to_db  # Ensure proper import
from flask_app.app.gis_plot import generate_gis_map
import pandas as pd

from flask_app.app.weather_fetch import fetch_nearest_weather_stations
from flask_app.app.distance_calc import compute_distance_to_shore


# Define a Blueprint for routes
main_bp = Blueprint('main', __name__)

# ✅ HTML Page Routes
@main_bp.route('/')
def index():
    return render_template('index.html')

@main_bp.route('/sarsat-analysis')
def sarsat_analysis():
    return render_template('sarsat_analysis.html')

@main_bp.route('/our-process')
def our_process():
    return render_template('our_process.html')

@main_bp.route('/future-projects')
def future_projects():
    return render_template('future_projects.html')

@main_bp.route('/contact')
def contact():
    return render_template('contact.html')

# ✅ API Route for Processing SARSAT Alerts
@main_bp.route('/process_alert', methods=['POST'])
def process_alert():
    """Receives SARSAT alert message, parses data, and stores it in a DataFrame."""
    
    if not request.is_json:
        return jsonify({"error": "Invalid request format. Must be JSON."}), 400

    raw_message = request.json.get("message")
    
    if not raw_message:
        return jsonify({"error": "No message provided"}), 400

    # ✅ Parse message into a DataFrame
    df_alert = parse_sarsat_message(raw_message)
    
    if df_alert.empty:
        return jsonify({"error": "Message parsing failed"}), 400

    # ✅ Compute Distance to Shore
    distance_to_shore_km = compute_distance_to_shore(df_alert.iloc[0]["latitude"], df_alert.iloc[0]["longitude"])
    
    # ✅ Fetch Nearest Weather Stations
    df_weather = fetch_nearest_weather_stations(df_alert.iloc[0]["latitude"], df_alert.iloc[0]["longitude"], distance_to_shore_km)

    # ✅ Attach results to the alert DataFrame
    df_alert["distance_to_shore_km"] = distance_to_shore_km
    df_alert["nearest_weather_stations"] = [df_weather.to_dict(orient="records")]

    return jsonify({"success": True, "alert_data": df_alert.to_dict(orient="records")})

@main_bp.route('/generate_gis_map/<path:latitude>/<path:longitude>/<path:site_id>', methods=['GET'])
def gis_map(latitude, longitude, site_id):
    """API Route to Generate GIS Map for a specific Site ID."""
    
    print(f"✅ DEBUG: Received request for GIS Map - Latitude: {latitude}, Longitude: {longitude}, Site ID: {site_id}")

    try:
        latitude_float = float(latitude)
        longitude_float = float(longitude)
        site_id_int = int(site_id)
    except ValueError:
        print(f"🚨 ERROR: Invalid parameters received -> Latitude: {latitude}, Longitude: {longitude}, Site ID: {site_id}")
        return jsonify({"error": "Invalid parameters"}), 400

    generate_gis_map(latitude_float, longitude_float, site_id_int)

    return redirect(url_for('main.serve_map', filename=f'gis_map_{site_id_int}.html'))



@main_bp.route('/maps/<path:filename>')  # ✅ Use <path:filename> to handle file paths
def serve_map(filename):
    """Serve GIS map files from the maps directory."""
    import os
    from flask import send_from_directory, current_app, abort

    maps_dir = os.path.abspath(os.path.join(current_app.root_path, "..", "maps"))


    # ✅ Debugging: Print the exact file path Flask is checking
    file_path = os.path.join(maps_dir, filename)
    print(f"✅ DEBUG: Flask is trying to serve file: {file_path}")

    # ✅ Check if the file actually exists before serving it
    if not os.path.exists(file_path):
        print(f"🚨 ERROR: Flask cannot find the file -> {file_path}")
        return abort(404, description="File not found")

    print(f"✅ Flask found the file and is serving it now: {file_path}")
    return send_from_directory(maps_dir, filename)

