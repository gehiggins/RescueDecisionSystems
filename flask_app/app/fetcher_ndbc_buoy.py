# fetcher_ndbc_buoy.py - Offshore Buoy Fetcher (2025-03-07 - Updated)

from flask_app.setup_imports import *
from flask_app.app.utils import log_error_and_continue, get_current_utc_timestamp
from flask_app.app.utils_geo import haversine_nm
from flask_app.app.utils_weather import calculate_timelate

METADATA_PATH = os.path.join(
    os.getenv('RDS_DATA_FOLDER', 'C:/Users/gehig/Projects/RescueDecisionSystems/data'),
    'reference',
    'ndbc_station_metadata_full.csv'
)

def load_buoy_metadata():
    try:
        logging.info(f"{get_current_utc_timestamp()} 📄 Loading buoy metadata from: {METADATA_PATH}")
        return pd.read_csv(METADATA_PATH)
    except Exception as e:
        log_error_and_continue(f"{get_current_utc_timestamp()} ❌ Failed to load buoy metadata: {e}")
        return pd.DataFrame()

def fetch_ndbc_buoy_data(lat, lon, position_label):
    try:
        metadata_df = load_buoy_metadata()
        if metadata_df.empty:
            logging.error(f"{get_current_utc_timestamp()} ⚠️ No buoy metadata available — returning empty DataFrame.")
            return pd.DataFrame()

        metadata_df['distance_nm'] = metadata_df.apply(
            lambda row: haversine_nm(lat, lon, row['latitude'], row['longitude']),
            axis=1
        )
        nearby_buoys = metadata_df.nsmallest(10, 'distance_nm').copy()

        if nearby_buoys.empty:
            logging.warning(f"{get_current_utc_timestamp()} ⚠️ No nearby buoys found — returning empty DataFrame.")
            return pd.DataFrame()

        all_buoy_data = []
        for _, buoy in nearby_buoys.iterrows():
            buoy_data = fetch_single_buoy(buoy['station_id'], buoy.get('preferred_data_source', 'none'))

            if buoy_data.empty:
                all_buoy_data.append(create_placeholder_row(buoy, position_label, "metadata_only"))
            else:
                for _, obs in buoy_data.iterrows():
                    all_buoy_data.append(enrich_observation_with_metadata(buoy, obs, position_label))

        if all_buoy_data:
            df = pd.DataFrame(all_buoy_data)
        else:
            logging.warning(f"{get_current_utc_timestamp()} ⚠️ No valid data from nearby buoys — returning empty DataFrame.")
            df = pd.DataFrame()

        logging.debug(f"✅ Buoy DataFrame columns for Position {position_label}: {df.columns.tolist()}")
        logging.debug(f"✅ Buoy DataFrame (first 5 rows) for Position {position_label}:\n{df.head()}")

        required_columns = ['latitude', 'longitude', 'station_id']
        for col in required_columns:
            if col not in df.columns:
                df[col] = np.nan

        return df

    except Exception as e:
        log_error_and_continue(f"{get_current_utc_timestamp()} ❌ Error fetching buoy data for Position {position_label}: {e}")
        return pd.DataFrame()

# fetch_single_buoy and enrich_observation_with_metadata remain unchanged
