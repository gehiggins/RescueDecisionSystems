import re
import logging
from app.utils import log_error_and_continue

def is_valid_coordinate(coord_string):
    """
    Checks if a string is a valid coordinate (basic format + N/S/E/W).
    Returns True or False.
    """
    pattern = r"\d{2,3} \d{2,3}\.\d+[NSEW]"
    return bool(re.search(pattern, coord_string.strip().upper()))

def clean_and_standardize_coordinate(coord_string: str) -> str:
    if coord_string is None:
        return ""
    return re.sub(r"\s+", " ", coord_string.strip().upper())

def parse_any_coordinate(coord_string: str) -> float:
    """
    Accepts '37 45.600N' or '075 30.200W' or with seconds '47 06 36.0N'.
    Returns signed decimal degrees or raises ValueError.
    """
    s = clean_and_standardize_coordinate(coord_string)
    m = re.match(r"^(\d{1,3})\s+(\d{1,2}(?:\.\d+)?)(?:\s+(\d{1,2}(?:\.\d+)?))?\s*([NSEW])$", s)
    if not m:
        raise ValueError(f"Invalid coordinate format: {coord_string}")
    deg = int(m.group(1))
    minutes = float(m.group(2))
    seconds = float(m.group(3)) if m.group(3) else 0.0
    if not (0 <= minutes < 60): raise ValueError("minutes out of range")
    if not (0 <= seconds < 60): raise ValueError("seconds out of range")
    dd = deg + minutes/60.0 + seconds/3600.0
    if m.group(4) in ("S", "W"): dd *= -1
    return dd

def coordinate_pair_to_dd(coord_string):
    """
    Parses a combined lat/lon string into decimal degrees, tolerating variable spacing.
    Returns (lat_dd, lon_dd) or (None, None) if parsing fails.
    """
    coord_string = clean_and_standardize_coordinate(coord_string)

    try:
        # Find two coordinate-like strings (ending in N/S then E/W)
        pattern = r"(\d{2,3}\s*\d{2}(?:\.\d+)?[NS])\s*(\d{2,3}\s*\d{2}(?:\.\d+)?[EW])"
        match = re.search(pattern, coord_string.upper())

        if not match:
            logging.warning(f"âŒ No valid coordinate pair found in string: '{coord_string}'")
            return None, None

        lat_string, lon_string = match.groups()
        lat_dd = parse_any_coordinate(lat_string)
        lon_dd = parse_any_coordinate(lon_string)
        return lat_dd, lon_dd

    except Exception as e:
        logging.warning(f"âš ï¸ Coordinate pair parsing failed: {coord_string} â€” {e}")
        return None, None

def _bounds_ok_lat(lat):  
    return lat is not None and -90.0 <= lat <= 90.0

def _bounds_ok_lon(lon):  
    return lon is not None and -180.0 <= lon <= 180.0

def coordinate_pair_to_dd_safe(coord_string):
    lat, lon = coordinate_pair_to_dd(coord_string)
    if not (_bounds_ok_lat(lat) and _bounds_ok_lon(lon)):
        return None, None
    return lat, lon

# Placeholder stubs â€” original content preserved below
def convert_km_to_miles(km):
    return km * 0.621371

def convert_lat_lon_to_decimal(lat_str, lon_str):
    try:
        lat_dd = parse_any_coordinate(lat_str)
        lon_dd = parse_any_coordinate(lon_str)
        return lat_dd, lon_dd
    except Exception as e:
        log_error_and_continue("convert_lat_lon_to_decimal", e)
        return None, None

def calculate_bearing(lat1, lon1, lat2, lon2):
    try:
        import math
        dLon = math.radians(lon2 - lon1)
        y = math.sin(dLon) * math.cos(math.radians(lat2))
        x = math.cos(math.radians(lat1)) * math.sin(math.radians(lat2)) - \
            math.sin(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.cos(dLon)
        bearing = math.atan2(y, x)
        bearing = math.degrees(bearing)
        return (bearing + 360) % 360
    except Exception as e:
        log_error_and_continue("calculate_bearing", e)
        return None

def is_within_bbox(lat, lon, bbox):
    """
    bbox: (min_lat, min_lon, max_lat, max_lon)
    """
    try:
        min_lat, min_lon, max_lat, max_lon = bbox
        return min_lat <= lat <= max_lat and min_lon <= lon <= max_lon
    except Exception as e:
        log_error_and_continue("is_within_bbox", e)
        return False

def parse_coordinate_pair_block(text_block):
    """
    Parses a block of text for lat/lon pair and returns decimal degrees.
    """
    try:
        coords = re.findall(r"(\d{2,3} \d{2,3}\.\d+[NSEW])", text_block.upper())
        if len(coords) >= 2:
            lat_dd = parse_any_coordinate(coords[0])
            lon_dd = parse_any_coordinate(coords[1])
            return lat_dd, lon_dd
        return None, None
    except Exception as e:
        log_error_and_continue("parse_coordinate_pair_block", e)
        return None, None

def extract_cardinal(coord_string):
    """
    Returns the cardinal direction (N/S/E/W) from a coordinate string.
    """
    match = re.search(r"[NSEW]$", coord_string.strip().upper())
    return match.group(0) if match else None

def format_dd_as_dms(lat_dd, lon_dd):
    """
    Converts decimal degrees to DMS format (e.g., '37Â°45.6'N, 75Â°30.2'W').
    """
    try:
        def convert(dd, positive, negative):
            direction = positive if dd >= 0 else negative
            dd = abs(dd)
            degrees = int(dd)
            minutes = (dd - degrees) * 60
            return f"{degrees}Â°{minutes:.1f}'{direction}"

        lat_dms = convert(lat_dd, 'N', 'S')
        lon_dms = convert(lon_dd, 'E', 'W')
        return f"{lat_dms}, {lon_dms}"
    except Exception as e:
        log_error_and_continue("format_dd_as_dms", e)
        return ""

def format_dd_short(lat_dd, lon_dd):
    """
    Returns short string version of lat/lon rounded to 4 decimals.
    """
    try:
        return f"{lat_dd:.4f}, {lon_dd:.4f}"
    except Exception as e:
        log_error_and_continue("format_dd_short", e)
        return ""

def to_latlon_polyline(coords_lonlat):
    """
    Input:  [[lon, lat], ...]  (GeoJSON-style)
    Output: [[lat, lon], ...]  (Leaflet/Folium)
    Drops bad pairs gracefully.
    """
    out = []
    for pt in (coords_lonlat or []):
        if pt and len(pt) == 2:
            lon, lat = pt
            try:
                out.append([float(lat), float(lon)])
            except Exception:
                continue
    return out
