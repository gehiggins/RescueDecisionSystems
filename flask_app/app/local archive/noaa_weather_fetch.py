# noaa_weather_fetch.py - Fetches weather data from NOAA/NDBC
# Location: flask_app/app/noaa_weather_fetch.py
# 2025-03-06 Updated to integrate new offshore buoy handling and improved maritime logic.

from app.setup_imports import *
from app.utils import log_error_and_continue, calculate_distance_nm
from app.fetch_NOAA_offshore_buoys import fetch_offshore_buoys
from app.noaa_weather_alerts_fetch import fetch_weather_alerts_zone

def fetch_nearest_weather_stations(lat, lon, position_label='A'):
    """
    Fetches both onshore and offshore weather stations near the provided coordinates.
    Applies maritime logic when within 5NM of shore.
    """
    try:
        # Placeholder for onshore fetch - assuming handled separately for now.
        onshore_stations = _fetch_onshore_weather_stations(lat, lon, position_label)

        # Determine if position is maritime (within 5NM of shore)
        is_maritime = _is_within_maritime_zone(lat, lon, threshold_nm=5)

        if is_maritime:
            logging.info(f"ðŸŒŠ Position {position_label} is maritime (within 5NM of shore). Fetching offshore buoys.")
            offshore_stations = fetch_offshore_buoys(lat, lon, max_buoys=10)
        else:
            offshore_stations = pd.DataFrame()  # No offshore buoys needed if fully inland.

        combined_stations = pd.concat([onshore_stations, offshore_stations], ignore_index=True)

        if combined_stations.empty:
            logging.warning(f"âš ï¸ No weather stations found for Position {position_label}")
            return combined_stations  # Empty DataFrame

        complete_stations = _filter_complete_weather_stations(combined_stations)

        if complete_stations.empty:
            logging.warning(f"âš ï¸ No complete weather stations found for Position {position_label}")

        # Fetch any relevant weather alerts for the zone (only onshore stations will have zones)
        weather_alerts_zone, zone_alerts = fetch_weather_alerts_zone(lat, lon)
        combined_stations['weather_alerts_zone'] = np.nan

        if weather_alerts_zone:
            logging.info(f"ðŸŒ Weather zone for Position {position_label}: {weather_alerts_zone}")
            combined_stations['weather_alerts_zone'] = combined_stations.apply(
                lambda row: weather_alerts_zone if pd.notna(row['zone']) and row['zone'] == weather_alerts_zone else np.nan,
                axis=1
            )

        return combined_stations

    except Exception as e:
        log_error_and_continue(f"âŒ Error fetching weather stations for Position {position_label}: {e}")
        return pd.DataFrame()

def _fetch_onshore_weather_stations(lat, lon, position_label):
    """
    Placeholder for actual NOAA/NWS onshore station fetch.
    Currently assumed handled separately.
    """
    log_error_and_continue(f"âŒ âŒ Error fetching onshore stations: Onshore station fetcher is assumed implemented elsewhere.")
    return pd.DataFrame()

def _is_within_maritime_zone(lat, lon, threshold_nm=5):
    """
    Placeholder for actual maritime zone check using coastline proximity logic.
    Defaulting to assume all positions are maritime for now.
    """
    # TODO: Implement actual coastline proximity check (using shapefile or preloaded grid)
    return True  # Placeholder - assumes all locations are maritime for testing.

def _filter_complete_weather_stations(stations_df):
    """
    Filters stations to only those with at least one valid data point (temperature, wind, wave).
    """
    if stations_df.empty:
        return stations_df

    stations_df['has_data'] = stations_df.apply(
        lambda row: not all(pd.isna(row[col]) for col in ['temperature', 'wind_speed', 'wave_height']),
        axis=1
    )

    return stations_df[stations_df['has_data']].drop(columns=['has_data'])



