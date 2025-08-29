"""
data_schema.py
Centralized data schema definition for SARSAT alert processing, weather data, and GIS mapping.
"""

import pandas as pd
import datetime

# ✅ SARSAT Alert Data Schema
SARSAT_ALERT_SCHEMA = {
    "beacon_id": str,           # Unique beacon identifier
    "site_id": str,             # Unique site identifier
    "detect_time": datetime.datetime,  # Detection timestamp (UTC)
    "latitude": float,          # Latitude in decimal degrees (WGS84)
    "longitude": float,         # Longitude in decimal degrees (WGS84)
    "distance_to_shore_nm": float,  # Distance to shore in nautical miles
    "alert_type": str,          # Alert classification (e.g., SIT 130, 170, etc.)
    "timestamp": datetime.datetime  # Processing timestamp
}

# ✅ Weather Data Schema (Per Station)
WEATHER_DATA_SCHEMA = {
    "station_id": str,          # NOAA/NDBC station identifier
    "station_name": str,        # Name of the weather station
    "latitude": float,          # Station latitude (decimal degrees)
    "longitude": float,         # Station longitude (decimal degrees)
    "distance_km": float,       # Distance from alert location (km)
    "wind_speed_mps": float,    # Wind speed in meters per second (m/s)
    "wind_direction_deg": float, # Wind direction in degrees
    "temperature_C": float,     # Air temperature (Celsius)
    "wave_height_m": float,     # Wave height in meters (if available)
    "precipitation_mm": float,  # Precipitation in millimeters (if available)
    "timestamp": datetime.datetime  # Timestamp of weather observation
}

# ✅ GIS Mapping Schema
GIS_DATA_SCHEMA = {
    "map_id": str,              # Unique identifier for generated GIS map
    "site_id": str,             # Site ID linked to the alert
    "alert_location": tuple,    # (latitude, longitude)
    "nearest_weather_stations": list,  # List of weather stations (dicts with WEATHER_DATA_SCHEMA)
    "distance_to_shore_nm": float,  # Distance from shore (nautical miles)
    "timestamp": datetime.datetime  # Timestamp of when the map was generated
}

# ✅ Function to Create DataFrames Based on Schema
def create_dataframe(schema):
    """Creates an empty pandas DataFrame based on the given schema."""
    return pd.DataFrame(columns=schema.keys()).astype(schema)

# ✅ Initialize DataFrames
df_alerts = create_dataframe(SARSAT_ALERT_SCHEMA)
df_weather = create_dataframe(WEATHER_DATA_SCHEMA)
df_gis_maps = create_dataframe(GIS_DATA_SCHEMA)

