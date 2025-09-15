# fetcher_noaa_shore.py - Onshore NOAA Station Data Fetcher for Rescue Decision Systems
# 2025-03-06 Initial Draft

# fetcher_noaa_shore.py - Onshore NOAA Station Data Fetcher for Rescue Decision Systems
# Updated with debugging and column enforcement - 2025-03-06

from app.setup_imports import *
from flask_app.app.utils import log_error_and_continue, get_current_utc_timestamp
from flask_app.app.utils_geo import is_within_5nm
from flask_app.app.utils_weather import calculate_timelate, celsius_to_fahrenheit, meters_per_second_to_knots

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

# This is placeholder - you may already have better logic
def query_nearest_noaa_stations(lat, lon):
    stations = [
        {'station_id': 'KSEA', 'latitude': 47.4447, 'longitude': -122.3133, 'station_type': 'ASOS', 'owner': 'NOAA', 'program': 'NWS'}
    ]
    return pd.DataFrame(stations)

def fetch_single_noaa_station(station_id):
    url = f"https://api.weather.gov/stations/{station_id}/observations/latest"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        obs_time = pd.to_datetime(data['properties']['timestamp'])
        temp_c = data['properties']['temperature']['value']
        wind_speed_mps = data['properties']['windSpeed']['value']
        wind_dir = data['properties']['windDirection']['value']

        return pd.DataFrame([{
            'observation_time': obs_time,
            'temperature': celsius_to_fahrenheit(temp_c),
            'wind_speed': meters_per_second_to_knots(wind_speed_mps),
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

