# fetcher_noaa_shore.py - Onshore NOAA Station Data Fetcher for Rescue Decision Systems
# 2025-03-06 Initial Draft

# fetcher_noaa_shore.py - Onshore NOAA Station Data Fetcher for Rescue Decision Systems
# Updated with debugging and column enforcement - 2025-03-06

import math

from app.setup_imports import *
from flask_app.app.utils import log_error_and_continue, get_current_utc_timestamp
from flask_app.app.utils_geo import haversine_nm
from flask_app.app.utils_weather import calculate_timelate

NOAA_REQUEST_TIMEOUT_SECONDS = 15
NOAA_REQUEST_HEADERS = {"User-Agent": "RDS-WEATHER/1.0"}


def _measurement(measurement):
    """Return one finite NWS value and its short unit code."""
    if not isinstance(measurement, dict):
        return None, None
    try:
        value = float(measurement.get("value"))
    except (TypeError, ValueError, OverflowError):
        return None, None
    if not math.isfinite(value):
        return None, None
    unit_code = measurement.get("unitCode")
    if not isinstance(unit_code, str):
        return None, None
    return value, unit_code.rsplit(":", 1)[-1]


def _temperature_c(measurement):
    value, unit = _measurement(measurement)
    if value is None:
        return np.nan
    if unit == "degC":
        return value
    if unit == "degF":
        return (value - 32.0) * 5.0 / 9.0
    if unit in {"K", "kelvin"}:
        return value - 273.15
    return np.nan


def _wind_speed_ms(measurement):
    value, unit = _measurement(measurement)
    if value is None:
        return np.nan
    if unit == "m_s-1":
        return value
    if unit == "km_h-1":
        return value / 3.6
    if unit in {"kn", "kt"}:
        return value * 0.514444
    return np.nan


def _direction_degrees(measurement):
    value, unit = _measurement(measurement)
    if value is None or unit not in {"degree_(angle)", "degree"}:
        return np.nan
    return value if 0 <= value <= 360 else np.nan

def fetch_noaa_shore_data(lat, lon, position_label):
    try:
        logging.info(f"{get_current_utc_timestamp()} ðŸŒ Fetching NOAA shore data for Position {position_label}")

        stations_df = query_nearest_noaa_stations(lat, lon)

        if stations_df.empty:
            logging.warning(f"{get_current_utc_timestamp()} âš ï¸ No NOAA shore stations found within range for Position {position_label}")
            return pd.DataFrame()

        all_station_data = []
        for _, station in stations_df.iterrows():
            station_id = station['station_id']
            station_data = fetch_single_noaa_station(station_id)

            if station_data.empty:
                all_station_data.append(create_placeholder_row(station, position_label))
                continue

            for _, obs in station_data.iterrows():
                enriched_obs = enrich_observation_with_metadata(station, obs, position_label)
                all_station_data.append(enriched_obs)

        combined_df = pd.DataFrame(all_station_data)

        logging.debug(f"âœ… Shore DataFrame columns for Position {position_label}: {combined_df.columns.tolist()}")
        logging.debug(f"âœ… Shore DataFrame (first 5 rows) for Position {position_label}:\n{combined_df.head()}")

        # Ensure critical columns always exist
        required_columns = ['latitude', 'longitude', 'station_id']
        for col in required_columns:
            if col not in combined_df.columns:
                combined_df[col] = np.nan

        logging.info(f"{get_current_utc_timestamp()} âœ… Retrieved NOAA shore data for Position {position_label}: {len(combined_df)} rows.")
        return combined_df

    except Exception as e:
        log_error_and_continue(f"{get_current_utc_timestamp()} âŒ Error fetching NOAA shore data for Position {position_label}: {e}")
        return pd.DataFrame()

def query_nearest_noaa_stations(
    lat, lon, max_stations=5, request_timeout=NOAA_REQUEST_TIMEOUT_SECONDS
):
    """Return the nearest NWS observation stations for one valid point."""
    if isinstance(lat, (bool, np.bool_)) or isinstance(lon, (bool, np.bool_)):
        return pd.DataFrame()
    try:
        lat, lon = float(lat), float(lon)
        max_stations = int(max_stations)
    except (TypeError, ValueError, OverflowError):
        return pd.DataFrame()
    if (
        not math.isfinite(lat)
        or not math.isfinite(lon)
        or not -90 <= lat <= 90
        or not -180 <= lon <= 180
        or max_stations < 1
    ):
        return pd.DataFrame()

    url = f"https://api.weather.gov/points/{lat},{lon}/stations"
    try:
        response = requests.get(
            url,
            timeout=request_timeout,
            headers=NOAA_REQUEST_HEADERS,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        logging.warning(
            "%s Failed to discover NOAA shore stations: %s",
            get_current_utc_timestamp(),
            exc,
        )
        return pd.DataFrame()

    stations = []
    features = payload.get("features", []) if isinstance(payload, dict) else []
    for feature in features:
        if not isinstance(feature, dict):
            continue
        properties = feature.get("properties") or {}
        geometry = feature.get("geometry") or {}
        coordinates = geometry.get("coordinates") or []
        if len(coordinates) < 2:
            continue
        try:
            station_lon, station_lat = float(coordinates[0]), float(coordinates[1])
        except (TypeError, ValueError, OverflowError):
            continue
        station_id = properties.get("stationIdentifier")
        if not station_id or not -90 <= station_lat <= 90 or not -180 <= station_lon <= 180:
            continue
        stations.append(
            {
                "station_id": str(station_id),
                "station_name": properties.get("name", "N/A"),
                "latitude": station_lat,
                "longitude": station_lon,
                "station_type": "NWS observation station",
                "owner": "NOAA/NWS",
                "program": "NWS",
                "distance_nm": haversine_nm(lat, lon, station_lat, station_lon),
            }
        )
    if not stations:
        return pd.DataFrame()
    return (
        pd.DataFrame(stations)
        .sort_values("distance_nm")
        .head(min(max_stations, 20))
        .reset_index(drop=True)
    )

def fetch_single_noaa_station(
    station_id, request_timeout=NOAA_REQUEST_TIMEOUT_SECONDS
):
    url = f"https://api.weather.gov/stations/{station_id}/observations/latest"
    try:
        response = requests.get(
            url,
            timeout=request_timeout,
            headers=NOAA_REQUEST_HEADERS,
        )
        response.raise_for_status()
        data = response.json()
        properties = data.get("properties", {}) if isinstance(data, dict) else {}

        obs_time = pd.to_datetime(properties.get('timestamp'), utc=True, errors='coerce')
        temp_c = _temperature_c(properties.get('temperature'))
        wind_speed_mps = _wind_speed_ms(properties.get('windSpeed'))
        wind_dir = _direction_degrees(properties.get('windDirection'))

        return pd.DataFrame([{
            'observation_time': obs_time,
            # NWS values have been normalized from their explicit unitCode to
            # the Celsius/metres-per-second shared RDS weather schema.
            'temperature': temp_c,
            'wind_speed': wind_speed_mps,
            'wind_direction': wind_dir,
            'timelate': calculate_timelate(obs_time),
            'source': 'NOAA'
        }])

    except Exception as e:
        logging.warning(f"{get_current_utc_timestamp()} âš ï¸ Failed to fetch data for NOAA station {station_id}: {e}")
        return pd.DataFrame()

def enrich_observation_with_metadata(station, observation, position_label):
    return {
        'station_id': station['station_id'],
        'station_name': station.get('station_name', 'N/A'),
        'latitude': station['latitude'],
        'longitude': station['longitude'],
        'observation_time': observation['observation_time'],
        'temperature': observation['temperature'],
        'wind_speed': observation['wind_speed'],
        'wind_direction': observation['wind_direction'],
        'wave_height': np.nan,
        'sea_state': np.nan,
        'source': observation['source'],
        'timelate': observation['timelate'],
        'distance_nm': station.get('distance_nm', np.nan),
        'position_label': position_label
    }

def create_placeholder_row(station, position_label):
    return {
        'station_id': station['station_id'],
        'latitude': station['latitude'],
        'longitude': station['longitude'],
        'observation_time': pd.NaT,
        'temperature': np.nan,
        'wind_speed': np.nan,
        'wind_direction': np.nan,
        'wave_height': np.nan,
        'sea_state': np.nan,
        'source': 'metadata_only',
        'timelate': np.nan,
        'position_label': position_label
    }

