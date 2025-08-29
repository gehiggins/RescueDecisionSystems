# sarsat_parser.py - SARSAT Alert Parser
# Location: flask_app/app/sarsat_parser.py
# 2025-03-06 Updated to use utils_coordinates.py for all coordinate handling

from flask_app.setup_imports import *
from app.utils import log_error_and_continue
from app.utils_coordinates import (
    preparse_coordinate_mapper,
    coordinate_pair_to_dd,
    is_valid_coordinate
)


def parse_sarsat_message(raw_message):
    """
    Parses SARSAT alert message text into structured data.
    Enhanced to leverage centralized coordinate handling.
    """

    parsed_data = {
        "latitude_a": None,
        "longitude_a": None,
        "latitude_b": None,
        "longitude_b": None,
        "position_status_a": None,
        "position_status_b": None
    }

    try:
        coordinate_pairs = preparse_coordinate_mapper(raw_message)

        if len(coordinate_pairs) >= 1:
            # Extract Position A
            start, end = coordinate_pairs[0]
            lat_str, lon_str = extract_lat_lon_from_message(raw_message[start:end])
            parsed_data["latitude_a"], parsed_data["longitude_a"] = coordinate_pair_to_dd(lat_str, lon_str)
            parsed_data["position_status_a"] = 'C'  # Confirmed position

        if len(coordinate_pairs) >= 2:
            # Extract Position B
            start, end = coordinate_pairs[1]
            lat_str, lon_str = extract_lat_lon_from_message(raw_message[start:end])
            parsed_data["latitude_b"], parsed_data["longitude_b"] = coordinate_pair_to_dd(lat_str, lon_str)
            parsed_data["position_status_b"] = 'C'  # Confirmed position

    except Exception as e:
        log_error_and_continue(f"❌ ❌ Error parsing SARSAT message: {e}")

    logging.info(f"📍 Parsed Positions - A: ({parsed_data['latitude_a']}, {parsed_data['longitude_a']}), "
                 f"B: ({parsed_data['latitude_b']}, {parsed_data['longitude_b']}), "
                 f"Status A: {parsed_data['position_status_a']}, Status B: {parsed_data['position_status_b']}")

    return parsed_data


def extract_lat_lon_from_message(segment):
    """
    Extracts the latitude and longitude strings from a detected coordinate segment.
    Example input: '37 45.600N 075 30.200W'
    """
    parts = segment.split()
    if len(parts) < 4:
        raise ValueError(f"Malformed coordinate segment: {segment}")

    lat_str = " ".join(parts[:2]) + parts[2][-1]  # Ensure direction stays attached
    lon_str = " ".join(parts[2:4])

    if not (is_valid_coordinate(lat_str) and is_valid_coordinate(lon_str)):
        raise ValueError(f"Invalid coordinates found: {lat_str}, {lon_str}")

    return lat_str, lon_str
