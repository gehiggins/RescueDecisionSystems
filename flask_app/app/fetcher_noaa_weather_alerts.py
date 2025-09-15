# fetcher_noaa_weather_alerts.py - NOAA Weather Alerts Fetcher for Rescue Decision Systems
# 2025-03-06 Initial Draft

from app.setup_imports import *
from flask_app.app.utils import log_error_and_continue, get_current_utc_timestamp

def fetch_weather_alerts_zone(lat, lon):
    """
    Fetches active weather alerts for the forecast zone associated with the given lat/lon.
    Args:
        lat (float): Latitude.
        lon (float): Longitude.
    Returns:
        DataFrame: Active weather alerts with columns:
            zone_id, headline, event, severity, certainty, urgency, effective, expires
    """
    try:
        grid_url = f"https://api.weather.gov/points/{lat},{lon}"

        response = requests.get(grid_url, timeout=10)
        response.raise_for_status()

        properties = response.json().get('properties', {})
        zone_url = properties.get('forecastZone')

        if not zone_url:
            logging.warning(f"{get_current_utc_timestamp()} âš ï¸ No forecast zone found for ({lat}, {lon})")
            return create_empty_alerts_df()

        alerts_url = f"{zone_url}/alerts/active"

        alerts_response = requests.get(alerts_url, timeout=10)
        alerts_response.raise_for_status()

        alerts_data = alerts_response.json()
        alerts = []

        for feature in alerts_data.get('features', []):
            props = feature.get('properties', {})
            alerts.append({
                'zone_id': zone_url.split('/')[-1],
                'headline': props.get('headline', 'N/A'),
                'event': props.get('event', 'N/A'),
                'severity': props.get('severity', 'Unknown'),
                'certainty': props.get('certainty', 'Unknown'),
                'urgency': props.get('urgency', 'Unknown'),
                'effective': props.get('effective', 'N/A'),
                'expires': props.get('expires', 'N/A'),
            })

        if alerts:
            logging.info(f"{get_current_utc_timestamp()} ðŸš¨ Active weather alerts found for zone {zone_url}: {len(alerts)} alerts")
        else:
            logging.info(f"{get_current_utc_timestamp()} âœ… No active weather alerts for zone {zone_url}")

        return pd.DataFrame(alerts)

    except Exception as e:
        log_error_and_continue(f"{get_current_utc_timestamp()} âŒ Error fetching weather alerts for ({lat}, {lon}): {e}")
        return create_empty_alerts_df()

def create_empty_alerts_df():
    """Creates an empty DataFrame with the correct schema for weather alerts."""
    columns = ['zone_id', 'headline', 'event', 'severity', 'certainty', 'urgency', 'effective', 'expires']
    return pd.DataFrame(columns=columns)

if __name__ == '__main__':
    sample_lat = 47.6062
    sample_lon = -122.3321
    alerts_df = fetch_weather_alerts_zone(sample_lat, sample_lon)
    print(alerts_df)

