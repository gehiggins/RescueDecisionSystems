# utils_geo.py - Geographic Utilities for Rescue Decision Systems
# 2025-03-06 Initial Draft

from flask_app.setup_imports import *
from math import radians, cos, sin, sqrt, atan2

# Radius of Earth in nautical miles
EARTH_RADIUS_NM = 3440.065

def haversine_nm(lat1, lon1, lat2, lon2):
    """
    Calculates great-circle distance between two points in nautical miles.
    """
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return EARTH_RADIUS_NM * c

def is_within_5nm(lat1, lon1, lat2, lon2):
    """
    Returns True if the distance between two points is <= 5 nautical miles.
    """
    return haversine_nm(lat1, lon1, lat2, lon2) <= 5

def determine_position_type(lat, lon, coastline_shapefile_path):
    """
    Determines if a position is 'shore', 'nearshore', or 'offshore' using 5NM boundary logic.
    Placeholder - future coastline spatial check.
    """
    # Placeholder: Currently just returns 'offshore' for testing.
    # Future: Use shapefile proximity check to determine 'shore' or 'nearshore'.
    return 'offshore'
