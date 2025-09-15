# utils.py
# Location: flask_app/app/utils.py
# Updated: 2025-03-06
# Follow all locked rules - no removals, only additive.

from app.setup_imports import *
from datetime import datetime, timezone

def log_error_and_continue(context: str, exc: Exception | None = None):
    """
    Logs an error with optional exception details, keeping callsites consistent.
    """
    if exc is not None:
        logging.error(f"âŒ {context}: {exc}")
    else:
        logging.error(f"âŒ {context}")

def calculate_distance_nm(lat1, lon1, lat2, lon2):
    """
    Calculates the great-circle distance between two coordinates in nautical miles.
    """
    try:
        coord1 = (lat1, lon1)
        coord2 = (lat2, lon2)
        return geopy.distance.distance(coord1, coord2).nautical
    except Exception as e:
        log_error_and_continue(f"Error calculating distance: {e}")
        return None

def parse_realtime2_data(station_id):
    """
    Parses the latest row of Realtime2 data for the given station.
    """
    try:
        url = f"https://www.ndbc.noaa.gov/data/realtime2/{station_id}.txt"
        response = requests.get(url)
        response.raise_for_status()

        lines = response.text.strip().split("\n")
        header_line = lines[1].strip().split()  # Line 2 is the header
        data_lines = [line.strip().split() for line in lines[2:]]

        best_row = _select_best_data_row(data_lines)

        if best_row is None:
            logging.warning(f"âš ï¸ No usable data found for Realtime2 buoy {station_id}")
            return None

        return _parse_data_row(header_line, best_row)

    except Exception as e:
        log_error_and_continue(f"Failed to fetch or parse Realtime2 data for {station_id}: {e}")
        return None

def parse_5day2_data(station_id):
    """
    Parses the latest row of 5day2 data for the given station.
    """
    try:
        url = f"https://www.ndbc.noaa.gov/data/5day2/{station_id}_5day.txt"
        response = requests.get(url)
        response.raise_for_status()

        lines = response.text.strip().split("\n")
        header_line = lines[1].strip().split()  # Line 2 is the header
        data_lines = [line.strip().split() for line in lines[2:]]

        best_row = _select_best_data_row(data_lines)

        if best_row is None:
            logging.warning(f"âš ï¸ No usable data found for 5day2 buoy {station_id}")
            return None

        return _parse_data_row(header_line, best_row)

    except Exception as e:
        log_error_and_continue(f"Failed to fetch or parse 5day2 data for {station_id}: {e}")
        return None

def _select_best_data_row(data_lines):
    """
    Selects the most complete data row within the last 60 minutes if available.
    Prefers the row with the most non-missing fields.
    """
    now = datetime.now(timezone.utc)

    best_row = None
    best_completeness = -1

    for line in data_lines:
        try:
            year, month, day, hour, minute = map(int, line[:5])
            obs_time = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
            age_hours = (now - obs_time).total_seconds() / 3600

            if age_hours > 60:
                continue

            completeness = sum(1 for val in line[5:] if val != 'MM')

            if completeness > best_completeness:
                best_row = line
                best_completeness = completeness

        except Exception:
            continue  # Skip bad rows

    return best_row

def _parse_data_row(header_line, data_row):
    """
    Converts a parsed row into a standardized dictionary with timelate.
    """
    try:
        now = datetime.now(timezone.utc)
        year, month, day, hour, minute = map(int, data_row[:5])
        obs_time = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
        timelate = (now - obs_time).total_seconds() / 3600

        data = {
            "observation_time": obs_time.isoformat(),
            "timelate": round(timelate, 2),
            "temperature": _safe_float_lookup(header_line, data_row, 'ATMP'),
            "wind_speed": _safe_float_lookup(header_line, data_row, 'WSPD'),
            "wind_direction": _safe_float_lookup(header_line, data_row, 'WDIR'),
            "wave_height": _safe_float_lookup(header_line, data_row, 'WVHT')
        }

        if timelate > 1:
            logging.warning(f"âš ï¸ OBSERVATION IS OVER 1 HOUR OLD ({timelate:.2f} hrs)")

        return data

    except Exception as e:
        log_error_and_continue(f"Failed to parse data row: {e}")
        return None

def _safe_float_lookup(header, row, field_name):
    """
    Safely fetches and converts a field from the data row, handling 'MM' missing values.
    """
    try:
        idx = header.index(field_name)
        value = row[idx]
        return None if value == 'MM' else float(value)
    except (ValueError, IndexError, TypeError):
        return None

def format_weather_summary(station_row):
    """
    Formats a summary string for weather data popups on GIS maps.
    Handles missing data gracefully.
    """
    summary_parts = []

    temp = station_row.get('temperature', 'N/A')
    wind_speed = station_row.get('wind_speed', 'N/A')
    wind_dir = station_row.get('wind_direction', 'N/A')
    wave_height = station_row.get('wave_height', 'N/A')
    timelate = station_row.get('timelate', 'N/A')
    deployment_notes = station_row.get('deployment_notes', 'N/A')

    summary_parts.append(f"Temperature: {temp}Â°C")
    summary_parts.append(f"Wind: {wind_speed} m/s @ {wind_dir}Â°")
    summary_parts.append(f"Wave Height: {wave_height} m")
    summary_parts.append(f"Timelate (hrs): {timelate}")
    summary_parts.append(f"Notes: {deployment_notes}")

    return "\n".join(summary_parts)

def load_sample_message(file_path):
    """
    Loads a sample SARSAT message from a text file.
    This function is used for testing and pipeline demos.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read()
    except Exception as e:
        log_error_and_continue(f"âŒ Failed to load sample message: {e}")
        return None

def get_current_utc_timestamp():
    """
    Returns current UTC time as a formatted string.
    """
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]


