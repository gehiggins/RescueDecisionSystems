# noaa_weather_alerts_fetch.py

from flask_app.setup_imports import *
from datetime import datetime

import requests
import logging

def fetch_weather_alerts_zone(lat, lon):
    """
    Fetches active weather alerts for the weather zone associated with the given lat/lon.

    Parameters:
        lat (float): Latitude of the position.
        lon (float): Longitude of the position.

    Returns:
        tuple: (zone_id (str), list of active alerts (list of str))
    """
    try:
        # Fetch the grid point metadata to find the forecast zone
        grid_url = f"https://api.weather.gov/points/{lat},{lon}"
        response = requests.get(grid_url)
        response.raise_for_status()
        properties = response.json().get('properties', {})

        zone_url = properties.get('forecastZone')
        if not zone_url:
            logging.warning(f"⚠️ No forecast zone found for ({lat}, {lon})")
            return None, []

        # Fetch active alerts for the forecast zone
        alerts_url = f"{zone_url}/alerts/active"
        alerts_response = requests.get(alerts_url)
        alerts_response.raise_for_status()

        alerts_data = alerts_response.json()
        alerts = [alert['properties']['headline'] for alert in alerts_data.get('features', [])]

        if alerts:
            logging.info(f"🚨 Active weather alerts for zone {zone_url}: {alerts}")
        else:
            logging.info(f"✅ No active weather alerts for zone {zone_url}")

        return zone_url, alerts

    except Exception as e:
        logging.error(f"❌ Error fetching weather alerts for ({lat}, {lon}): {e}")
        return None, []
