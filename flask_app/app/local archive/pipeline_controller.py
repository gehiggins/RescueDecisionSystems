# pipeline_controller.py - Updated to Include Pre-Scan Phase (2025-03-06)

from app.setup_imports import *
from app.sarsat_parser import parse_sarsat_message
from app.database import save_alert_to_db, save_weather_to_db
from app.gis_mapping import generate_gis_map
from app.noaa_weather_fetch import fetch_nearest_weather_stations
from app.utils import log_error_and_continue, load_sample_message
from app.utils_coordinates import pre_scan_for_coordinates  # New Import

def process_sarsat_alert(raw_alert_message):
    """
    Core processing pipeline for a SARSAT alert.
    Parses the alert, pre-scans coordinates, fetches weather data, and generates GIS map.
    """
    try:
        logging.info("ðŸš€ Starting SARSAT alert processing pipeline")

        # NEW: Pre-scan coordinates before parsing
        pre_scan_results = preparse_coordinate_mapper(raw_alert_message)
        logging.info(f"ðŸ” Pre-scan detected {len(pre_scan_results)} complete coordinate pairs.")

        # Parse message with pre-scan assistance
        parsed_data = parse_sarsat_message(raw_alert_message, pre_scan_results=pre_scan_results)

        if parsed_data is None:
            logging.error("âŒ SARSAT message parsing failed â€” skipping further processing.")
            return

        alert_df = pd.DataFrame([parsed_data])
        logging.info(f"ðŸ“ Parsed Positions - A: {parsed_data.get('latitude_a')}, "
                     f"B: {parsed_data.get('latitude_b')}, "
                     f"Status A: {parsed_data.get('position_status_a')}, "
                     f"Status B: {parsed_data.get('position_status_b')}")

        try:
            save_alert_to_db(alert_df)
            logging.info(f"âœ… Alert saved with ID: {parsed_data.get('site_id')}")
        except Exception as e:
            log_error_and_continue(f"âŒ Failed to save alert to DB: {e}")

        for position in ['A', 'B']:
            lat = parsed_data.get(f'latitude_{position.lower()}')
            lon = parsed_data.get(f'longitude_{position.lower()}')

            if pd.isna(lat) or pd.isna(lon):
                logging.info(f"ðŸ“ Skipping weather fetch for Position {position} (No valid coordinates)")
                continue

            logging.info(f"ðŸŒ¦ï¸ Fetching weather for Position {position} ({lat}, {lon})")
            stations_df = fetch_nearest_weather_stations(lat, lon, position_label=position)

            if stations_df is not None and not stations_df.empty:
                save_weather_to_db(stations_df, parsed_data.get('site_id'), position)
            else:
                logging.warning(f"âš ï¸ No complete weather stations found for Position {position}")

        map_path = generate_gis_map(alert_df.iloc[0], os.path.join(
            os.getenv('RDS_DATA_FOLDER', 'C:/Users/gehig/Projects/RescueDecisionSystems/data'),
            'maps',
            f"alert_{parsed_data.get('site_id')}_map.html"
        ))

        if map_path:
            logging.info(f"âœ… Map generated and saved at: {map_path}")

        logging.info("âœ… Completed SARSAT alert processing pipeline")

    except Exception as e:
        log_error_and_continue(f"âŒ Pipeline processing error: {e}")

if __name__ == "__main__":
    sample_message_path = "C:/Users/gehig/Projects/RescueDecisionSystems/sample_sarsat_message.txt"
    example_alert_message = load_sample_message(sample_message_path)
    process_sarsat_alert(example_alert_message)

