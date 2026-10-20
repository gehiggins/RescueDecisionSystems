# fetcher_noaa_weather_alerts.py - NOAA Weather Alerts Fetcher for Rescue Decision Systems
# 2025-03-06 Initial Draft

from app.setup_imports import *
from flask_app.app.utils import log_error_and_continue, get_current_utc_timestamp

NOAA_ALERT_TIMEOUT_SECONDS = 10
NOAA_ALERT_HEADERS = {"User-Agent": "RDS-WEATHER/1.0"}


def fetch_weather_alert_features(
    lat, lon, request_timeout=NOAA_ALERT_TIMEOUT_SECONDS
):
    """Return a forecast-zone URL, zone ID, and active alert features."""
    lat, lon = float(lat), float(lon)
    if (
        not np.isfinite(lat)
        or not np.isfinite(lon)
        or not -90 <= lat <= 90
        or not -180 <= lon <= 180
    ):
        return None, None, []

    grid_url = f"https://api.weather.gov/points/{lat},{lon}"
    response = requests.get(
        grid_url,
        timeout=request_timeout,
        headers=NOAA_ALERT_HEADERS,
    )
    response.raise_for_status()
    point_payload = response.json()
    properties = (
        point_payload.get("properties", {})
        if isinstance(point_payload, dict)
        else {}
    )
    zone_url = properties.get("forecastZone")
    if not isinstance(zone_url, str) or not zone_url.strip():
        return None, None, []
    zone_id = zone_url.rstrip("/").split("/")[-1]
    if not re.fullmatch(r"[A-Za-z0-9-]{3,20}", zone_id):
        return None, None, []

    # NWS exposes active alerts by zone at this fixed API route. Appending
    # ``alerts/active`` to the forecast-zone resource produces a 404.
    alerts_url = f"https://api.weather.gov/alerts/active/zone/{zone_id}"
    alerts_response = requests.get(
        alerts_url,
        timeout=request_timeout,
        headers=NOAA_ALERT_HEADERS,
    )
    alerts_response.raise_for_status()
    alerts_payload = alerts_response.json()
    raw_features = (
        alerts_payload.get("features", [])
        if isinstance(alerts_payload, dict)
        else []
    )
    features = [feature for feature in raw_features if isinstance(feature, dict)]
    return zone_url, zone_id, features


def fetch_weather_alerts_zone(
    lat, lon, request_timeout=NOAA_ALERT_TIMEOUT_SECONDS
):
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
        zone_url, zone_id, features = fetch_weather_alert_features(
            lat, lon, request_timeout=request_timeout
        )

        if not zone_url:
            logging.warning(f"{get_current_utc_timestamp()} âš ï¸ No forecast zone found for ({lat}, {lon})")
            return create_empty_alerts_df()

        alerts = []

        for feature in features:
            props = feature.get('properties')
            if not isinstance(props, dict):
                continue
            alerts.append({
                'zone_id': zone_id,
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

