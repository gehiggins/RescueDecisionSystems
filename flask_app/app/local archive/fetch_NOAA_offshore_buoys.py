# fetch_NOAA_offshore_buoys.py
from flask_app.setup_imports import *
from app.utils import log_error_and_continue, calculate_distance_nm, parse_realtime2_data, parse_5day2_data

def fetch_offshore_buoys(lat, lon, max_buoys=10):
    """
    Fetch nearest offshore buoys with recent data for a given position.
    Combines location metadata from NDBC station list with observations from Realtime2 and 5day2.
    """

    try:
        buoys = _fetch_nearest_buoys(lat, lon, max_buoys)

        buoy_data = []
        for buoy in buoys:
            buoy_id = buoy['station_id']

            # Try Realtime2 first
            source = "realtime2"
            data = parse_realtime2_data(buoy_id)

            if data is None:
                # Fall back to 5day2
                source = "5day2"
                data = parse_5day2_data(buoy_id)

            if data is None:
                logging.warning(f"⚠️ No valid data found for buoy {buoy_id}")
                continue

            # Add source column
            data['source'] = source

            # Merge with metadata
            data.update({
                'station_id': buoy_id,
                'name': buoy.get('name', 'Unknown'),
                'latitude': buoy.get('latitude', None),
                'longitude': buoy.get('longitude', None),
                'owner': buoy.get('owner', 'Unknown'),
                'pgm': buoy.get('pgm', 'Unknown'),
                'deployment_notes': buoy.get('deployment_notes', 'No notes')
            })

            # Log row completeness (count non-null fields)
            non_null_count = sum(1 for value in data.values() if pd.notna(value))
            logging.info(f"📊 Buoy {buoy_id} ({source}) data completeness: {non_null_count} fields populated.")

            buoy_data.append(data)

        if not buoy_data:
            logging.warning(f"⚠️ No valid observations retrieved for buoys near ({lat}, {lon})")
            return pd.DataFrame()

        return pd.DataFrame(buoy_data)

    except Exception as e:
        log_error_and_continue(f"❌ Error fetching offshore buoys: {e}")
        return pd.DataFrame()

def _fetch_nearest_buoys(lat, lon, max_buoys):
    """
    Fetch and sort nearest offshore buoys from the station metadata.
    This function uses pre-loaded station metadata from ndbc_station_metadata_full.csv.
    """

    metadata_path = 'C:/Users/gehig/Projects/RescueDecisionSystems/data/reference/ndbc_station_metadata_full.csv'
    try:
        stations = pd.read_csv(metadata_path)
        stations = stations[stations['latitude'].notna() & stations['longitude'].notna()]

        stations['distance_nm'] = stations.apply(lambda row: calculate_distance_nm(
            (lat, lon), (row['latitude'], row['longitude'])), axis=1)

        nearest_stations = stations.sort_values(by='distance_nm').head(max_buoys).to_dict(orient='records')
        logging.info(f"✅ Found {len(nearest_stations)} nearest offshore buoys.")
        return nearest_stations

    except Exception as e:
        log_error_and_continue(f"❌ Error loading station metadata or calculating distances: {e}")
        return []
