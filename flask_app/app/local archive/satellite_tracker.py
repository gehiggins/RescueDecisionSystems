import os
import time
import pandas as pd
from skyfield.api import load, EarthSatellite
from flask_app.app.tle_fetcher import fetch_tle  # Use your existing fetcher

TLE_DIR = 'data/tle'
SATELLITE_CSV = 'data/reference/sarsat_satellites.csv'

# Cache for TLE-loaded Skyfield satellites
satellite_objects = {}

def load_satellite_metadata():
    """Load satellite metadata from CSV into a DataFrame."""
    return pd.read_csv(SATELLITE_CSV)

def is_recent_tle(filename, max_age_hours=24):
    """Check if a TLE file is recent enough."""
    file_path = os.path.join(TLE_DIR, filename)
    if not os.path.exists(file_path):
        return False
    file_age_hours = (time.time() - os.path.getmtime(file_path)) / 3600
    return file_age_hours <= max_age_hours

def ensure_fresh_tle():
    """Check all TLE files and fetch if needed."""
    tle_files = ['sarsat.tle', 'gps.tle', 'galileo.tle', 'glonass.tle']
    if all(is_recent_tle(tle) for tle in tle_files):
        print("✅ TLE files are up-to-date.")
    else:
        print("⚠️ TLE files outdated or missing - fetching fresh copies.")
        fetch_tle()

def load_tle_data():
    """Load TLE data into Skyfield satellite objects."""
    global satellite_objects
    satellite_objects.clear()

    ts = load.timescale()

    for tle_file in ['sarsat.tle', 'gps.tle', 'galileo.tle', 'glonass.tle']:
        file_path = os.path.join(TLE_DIR, tle_file)
        if os.path.exists(file_path):
            with open(file_path) as f:
                lines = f.readlines()
            for i in range(0, len(lines), 3):
                name = lines[i].strip()
                line1 = lines[i+1].strip()
                line2 = lines[i+2].strip()
                satellite_objects[name] = EarthSatellite(line1, line2, name, ts)

def get_satellite_position(satellite_name, timestamp=None):
    """Get current or specified time position (lat, lon, alt) for a given satellite."""
    if satellite_name not in satellite_objects:
        raise ValueError(f"Satellite {satellite_name} not loaded from TLE data.")

    if timestamp is None:
        ts = load.timescale()
        timestamp = ts.now()

    satellite = satellite_objects[satellite_name]
    geocentric = satellite.at(timestamp)
    subpoint = geocentric.subpoint()
    return {
        "latitude": subpoint.latitude.degrees,
        "longitude": subpoint.longitude.degrees,
        "altitude_m": subpoint.elevation.m
    }

def compute_ground_track(satellite_name, hours_back=1, hours_forward=3, step_minutes=5):
    """Generate a ground track (list of lat/lon) for the past and future time range."""
    if satellite_name not in satellite_objects:
        raise ValueError(f"Satellite {satellite_name} not loaded from TLE data.")

    ts = load.timescale()
    now = ts.now()

    times = []
    for offset in range(-hours_back * 60, hours_forward * 60, step_minutes):
        times.append(now + offset / (24 * 60))

    track = []
    satellite = satellite_objects[satellite_name]
    for t in times:
        geocentric = satellite.at(t)
        subpoint = geocentric.subpoint()
        track.append({
            "timestamp": t.utc_iso(),
            "la
