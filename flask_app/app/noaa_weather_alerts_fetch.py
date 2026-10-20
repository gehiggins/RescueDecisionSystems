# noaa_weather_alerts_fetch.py

from app.setup_imports import *
from flask_app.app.fetcher_noaa_weather_alerts import (
    NOAA_ALERT_TIMEOUT_SECONDS,
    fetch_weather_alert_features,
)

def fetch_weather_alerts_zone(
    lat, lon, request_timeout=NOAA_ALERT_TIMEOUT_SECONDS
):
    """
    Fetches active weather alerts for the weather zone associated with the given lat/lon.

    Parameters:
        lat (float): Latitude of the position.
        lon (float): Longitude of the position.

    Returns:
        tuple: (zone_id (str), list of active alerts (list of str))
    """
    try:
        zone_url, _zone_id, features = fetch_weather_alert_features(
            lat, lon, request_timeout=request_timeout
        )
        if not zone_url:
            logging.warning(f"âš ï¸ No forecast zone found for ({lat}, {lon})")
            return None, []

        alerts = []
        for feature in features:
            properties = feature.get("properties")
            if not isinstance(properties, dict):
                continue
            headline = properties.get("headline")
            if isinstance(headline, str) and headline.strip():
                alerts.append(headline)

        if alerts:
            logging.info(f"ðŸš¨ Active weather alerts for zone {zone_url}: {alerts}")
        else:
            logging.info(f"âœ… No active weather alerts for zone {zone_url}")

        return zone_url, alerts

    except Exception as e:
        logging.error(f"âŒ Error fetching weather alerts for ({lat}, {lon}): {e}")
        return None, []

