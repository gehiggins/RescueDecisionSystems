import math
import logging
import re

def _convert_lat_lon_to_decimal(degrees, minutes, hemisphere):
    """
    Private helper to convert latitude/longitude from degrees and minutes to decimal degrees.
    Handles northern/southern and eastern/western hemispheres.
    """
    try:
        decimal_degrees = float(degrees) + (float(minutes) / 60)
        if hemisphere in ["S", "W"]:
            decimal_degrees *= -1
        return decimal_degrees
    except Exception as e:
        logging.error(f"❌ Error converting lat/lon to decimal: {e}")
        return None

def convert_km_to_miles(km):
    return km * 0.621371

def convert_meters_to_feet(meters):
    return meters * 3.28084

def convert_celsius_to_fahrenheit(celsius):
    return (celsius * 9/5) + 32

def convert_hpa_to_inhg(hpa):
    return hpa * 0.029529983071445

def log_error_and_continue(msg, exception=None):
    if exception:
        logging.error(f"❌ {msg}: {exception}")
    else:
        logging.error(f"❌ {msg}")

def parse_coordinates(coord_string):
    """
    Parses a combined lat/lon coordinate string into decimal degrees.
    Example input: "37 45.600N 075 30.200W"
    Returns: (latitude_dd, longitude_dd) or (None, None) if parsing fails.
    """

    def convert_to_decimal(coord):
        """
        Converts a single coordinate (lat or lon) from DDM to decimal degrees.
        Supports N/S/E/W suffix.
        """
        try:
            parts = coord.strip().split()
            if len(parts) < 1 or len(parts) > 2:
                logging.error(f"Invalid coordinate part: {coord}")
                return None

            # Example part: "37 45.600N"
            deg, rest = parts[0], parts[1] if len(parts) > 1 else ""
            match = re.match(r"(\d+)\s*(\d+\.\d+)([NSEW])", f"{deg} {rest}")
            if not match:
                logging.error(f"Failed to parse coordinate: {coord}")
                return None

            degrees, minutes, direction = float(match.group(1)), float(match.group(2)), match.group(3).upper()
            decimal = degrees + (minutes / 60)

            if direction in ['S', 'W']:
                decimal *= -1

            return decimal

        except Exception as e:
            logging.error(f"Exception parsing coordinate: {coord} - {e}")
            return None

    try:
        lat_str, lon_str = coord_string.strip().split(maxsplit=2)[:2]
        lat_dd = convert_to_decimal(lat_str)
        lon_dd = convert_to_decimal(lon_str)

        if lat_dd is None or lon_dd is None:
            logging.warning(f"Failed to parse coordinate pair: {coord_string}")
            return None

        return lat_dd, lon_dd

    except Exception as e:
        logging.error(f"Exception parsing full coordinate string: {coord_string} - {e}")
        return None

def format_weather_summary(weather_df):
    """
    Formats weather data for display on GIS map.
    Expects a DataFrame with at least one row, representing the nearest station data.
    """
    if weather_df.empty:
        return "Weather: No Data"
    
    station = weather_df.iloc[0]  # First station is closest
    return (f"Station: {station.get('station_name', 'Unknown')}, "
            f"Temp: {station.get('temperature', 'N/A')}°F, "
            f"Wind: {station.get('wind_speed', 'N/A')}kt, "
            f"Visibility: {station.get('visibility', 'N/A')}NM")
