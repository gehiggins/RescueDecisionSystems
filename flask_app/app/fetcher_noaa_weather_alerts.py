# fetcher_noaa_weather_alerts.py - NOAA Weather Alerts Fetcher for Rescue Decision Systems
# 2025-03-06 Initial Draft

from app.setup_imports import *
from flask_app.app.utils import (
    get_current_utc_timestamp,
    log_error_and_continue,
    normalize_request_timeout,
)

NOAA_ALERT_TIMEOUT_SECONDS = 10
NOAA_ALERT_HEADERS = {"User-Agent": "RDS-WEATHER/1.0"}
NOAA_ALERT_COLUMNS = [
    "zone_id",
    "headline",
    "event",
    "severity",
    "certainty",
    "urgency",
    "effective",
    "expires",
]


def fetch_weather_alert_features(
    lat, lon, request_timeout=NOAA_ALERT_TIMEOUT_SECONDS
):
    """Return a forecast-zone URL, zone ID, and active alert features."""
    if isinstance(lat, (bool, np.bool_)) or isinstance(lon, (bool, np.bool_)):
        return None, None, []
    try:
        lat, lon = float(lat), float(lon)
    except (TypeError, ValueError, OverflowError):
        return None, None, []
    if (
        not np.isfinite(lat)
        or not np.isfinite(lon)
        or not -90 <= lat <= 90
        or not -180 <= lon <= 180
    ):
        return None, None, []
    request_timeout = normalize_request_timeout(
        request_timeout, NOAA_ALERT_TIMEOUT_SECONDS
    )

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
    if not isinstance(properties, dict):
        properties = {}
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
    if not isinstance(raw_features, list):
        raw_features = []
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

        return pd.DataFrame(alerts, columns=NOAA_ALERT_COLUMNS)

    except Exception as e:
        log_error_and_continue(f"{get_current_utc_timestamp()} âŒ Error fetching weather alerts for ({lat}, {lon}): {e}")
        return create_empty_alerts_df()

def create_empty_alerts_df():
    """Creates an empty DataFrame with the correct schema for weather alerts."""
    return pd.DataFrame(columns=NOAA_ALERT_COLUMNS)

if __name__ == '__main__':
    sample_lat = 47.6062
    sample_lon = -122.3321
    alerts_df = fetch_weather_alerts_zone(sample_lat, sample_lon)
    print(alerts_df)

