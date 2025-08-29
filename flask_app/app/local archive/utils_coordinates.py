# utils_coordinates.py - Enhanced Coordinate Handling for RDS
# Location: flask_app/app/utils_coordinates.py
# 2025-03-07 (Updated for Robust Pre-Parsing Integration)

from flask_app.setup_imports import *
import re

def clean_and_standardize_coordinate(coord_string):
    """
    Cleans, standardizes, and repairs minor formatting issues with coordinates.
    Removes extra spaces, handles missing symbols, and prepares for parsing.
    """

    if not coord_string or not isinstance(coord_string, str):
        return coord_string  # Leave non-strings untouched.

    coord_string = coord_string.strip()

    # Remove accidental commas, semicolons, extra spaces, or dashes
    coord_string = re.sub(r"[,;]", "", coord_string)
    coord_string = re.sub(r"\s{2,}", " ", coord_string)
    coord_string = re.sub(r"(\d)[-](\d{1,2}\.\d+)", r"\1 \2", coord_string)  # Normalize dashes in lat/lon

    # Ensure NSEW is at the end after cleaning
    coord_string = re.sub(r"([NSWE])\s*$", r"\1", coord_string)
    return coord_string

def parse_any_coordinate(coord_string):
    """
    Parses a latitude or longitude from various formats (NMEA, DMS, Decimal Degrees, etc.).
    Returns decimal degrees (float) or raises ValueError.
    """
    coord_string = clean_and_standardize_coordinate(coord_string)

    # Handle Degrees-Minutes-Seconds (DMS) format
    dms_pattern = re.compile(r"""
        ^\s*(\d{1,3})[°\s]*(\d{1,2})[\s'\-]*(\d{1,2}\.\d+)?\s*([NSWE])\s*$
    """, re.VERBOSE)

    match = dms_pattern.match(coord_string)
    if match:
        degrees = float(match.group(1))
        minutes = float(match.group(2))
        seconds = float(match.group(3)) if match.group(3) else 0.0
        direction = match.group(4).upper()

        decimal_degrees = degrees + (minutes / 60) + (seconds / 3600)
        if direction in ['S', 'W']:
            decimal_degrees *= -1
        return decimal_degrees

    # Handle NMEA-style (Decimal Minutes) format
    nmea_pattern = re.compile(r"""
        ^\s*(\d{1,3})[°\s]*(\d{1,2}\.\d+)?\s*([NSWE])\s*$
    """, re.VERBOSE)

    match = nmea_pattern.match(coord_string)
    if match:
        degrees = float(match.group(1))
        minutes = float(match.group(2))
        direction = match.group(3).upper()

        decimal_degrees = degrees + (minutes / 60)
        if direction in ['S', 'W']:
            decimal_degrees *= -1
        return decimal_degrees

    # Handle pure Decimal Degrees format
    dd_pattern = re.compile(r"""
        ^\s*(-?\d+(?:\.\d+)?)\s*([NSWE]?)\s*$
    """, re.VERBOSE)

    match = dd_pattern.match(coord_string)
    if match:
        decimal_degrees = float(match.group(1))
        direction = match.group(2).upper()

        if direction in ['S', 'W']:
            decimal_degrees *= -1
        return decimal_degrees

    raise ValueError(f"Failed to parse coordinate: {coord_string}")

def is_valid_coordinate(coord_string):
    """
    Validate coordinate format, returning True if parsable.
    """
    try:
        parse_any_coordinate(coord_string)
        return True
    except ValueError:
        return False

def coordinate_pair_to_dd(lat_string, lon_string):
    """
    Parses lat/lon pair into decimal degrees (with enhanced error handling).
    Returns (lat_dd, lon_dd) or (None, None) if parsing fails.
    """
    lat_string = clean_and_standardize_coordinate(lat_string)
    lon_string = clean_and_standardize_coordinate(lon_string)

    try:
        lat_dd = parse_any_coordinate(lat_string)
        lon_dd = parse_any_coordinate(lon_string)
        return lat_dd, lon_dd
    except ValueError as e:
        logging.warning(f"Coordinate parsing failed for pair: {lat_string} / {lon_string} - {str(e)}")
        return None, None

def convert_to_nmea(lat_dd, lon_dd):
    """
    Converts decimal degrees to NMEA-style coordinates.
    """
    def format_nmea(dd, is_lat):
        direction = 'N' if dd >= 0 else 'S'
        if not is_lat:
            direction = 'E' if dd >= 0 else 'W'
        dd = abs(dd)
        degrees = int(dd)
        minutes = (dd - degrees) * 60
        return f"{degrees} {minutes:.3f}{direction}"

    return f"{format_nmea(lat_dd, True)} {format_nmea(lon_dd, False)}"

def preparse_coordinate_mapper(raw_message):
    """
    Pre-scans the message for all coordinate pairs with flexible spacing.
    Returns a DataFrame instead of a raw list.
    """
    coord_regex = re.compile(r"""
        (\d{2,3}[\s°]*\d{1,2}(?:\.\d+)?[\s]*[NS])\s*
        (\d{2,3}[\s°]*\d{1,2}(?:\.\d+)?[\s]*[EW])
    """, re.VERBOSE)

    matches = []
    for match in coord_regex.finditer(raw_message):
        lat = clean_and_standardize_coordinate(match.group(1))
        lon = clean_and_standardize_coordinate(match.group(2))
        start = match.start()
        end = match.end()

        lat_dd, lon_dd = coordinate_pair_to_dd(lat, lon)
        is_valid = lat_dd is not None and lon_dd is not None

        matches.append({
            "latitude": lat,
            "longitude": lon,
            "lat_dd": lat_dd,
            "lon_dd": lon_dd,
            "start_pos": start,
            "end_pos": end,
            "is_valid": is_valid
        })

    df = pd.DataFrame(matches)
    logging.info(f"Preparse detected {len(df)} coordinate pairs (valid+invalid).")
    return df

