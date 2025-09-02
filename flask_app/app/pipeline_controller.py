# pipeline_controller.py - SARSAT Alert Processing Pipeline
# 2025-03-07 (Fully Integrated with finalize_alert_df.py)
#
# Description:
# Manages the end-to-end processing of SARSAT alert messages, including:
# - Pre-parsing coordinates
# - Parsing SARSAT messages
# - Validating and finalizing alert data
# - Fetching weather data
# - Storing alerts in the database
# - Generating GIS maps
#
# External Data Sources:
# - SARSAT Messages (raw distress alerts)
# - NOAA Shore Weather Data (fetcher_noaa_shore.py)
# - NDBC Offshore Buoy Data (fetcher_ndbc_buoy.py)
#
# Internal Variables:
# - `alert_df`: Structured DataFrame containing parsed SARSAT alert data.
# - `pre_scan_results`: Coordinates extracted via `preparse_coordinate_mapper.py`.
# - `combined_weather_df`: Merged shore and buoy weather data.
#
# Produced DataFrames:
# - `alert_df`: Fully structured SARSAT alert data, validated before storage.
# - `weather_df`: Processed weather observations linked to SARSAT alert positions.
#
# Data Handling Notes:
# - Ensures SARSAT messages are parsed **before** weather fetching.
# - Uses **pre-parsed coordinates** for structured extraction.
# - **Finalizes alert_df** using `finalize_alert_df.py` before committing to the database.
# - Logs **missing or malformed fields** for debugging.
#
# Map Step (Final Output):
# - Role: Orchestrates parse → finalize → (call GIS) → [DB/weather etc.]. It calls the mapping function; it does not render maps itself.
# - Map Step Inputs: one finalized alert row (alert_df.iloc[0]) passed to GIS. (No new ground-truth DataFrame created here in this step.)
# - Map Step Output: HTML at data/maps/<site_id>/gis_map_<site_id>.html and prints:
#   ✅ Interactive map saved: data/maps/<site_id>/gis_map_<site_id>.html (current implementation uses generate_gis_map_html(...)).
# - Notes: DB connectivity is not required for the map smoke; map step should proceed even if DB is unavailable (as seen in prior runs).
# [RDS-ANCHOR: PREAMBLE_END]

import sys
import os

# Ensure the `flask_app` directory is added to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from flask_app.setup_imports import *
from flask_app.app.database import save_alert_to_db, save_weather_to_db, get_existing_alerts
from flask_app.app.gis_mapping import generate_gis_map, generate_gis_map_html_from_dfs
from flask_app.app.preparse_coordinate_mapper import pre_scan_for_coordinates
from flask_app.app.utils import log_error_and_continue, load_sample_message, get_current_utc_timestamp

from flask_app.app.fetcher_noaa_shore import fetch_noaa_shore_data
from flask_app.app.fetcher_ndbc_buoy import fetch_ndbc_buoy_data
from flask_app.app.fetcher_noaa_weather_alerts import fetch_weather_alerts_zone

from flask_app.app.finalize_alert_df import finalize_alert_df  # Fixed import
from flask_app.app.parser_sarsat_msg import parse_sarsat_message  # Standardized import


def process_sarsat_alert(raw_alert_message):
    try:
        logging.info(f"{get_current_utc_timestamp()} 🚀 Starting SARSAT alert processing pipeline")

        # Step 1: Pre-Parse Coordinates
        pre_scan_results = pre_scan_for_coordinates(raw_alert_message)
        logging.info(f"{get_current_utc_timestamp()} 🔍 Pre-scan detected {len(pre_scan_results)} coordinate pairs.")

        # Step 2: Parse SARSAT Message
        parsed_data = parse_sarsat_message(raw_alert_message, pre_scan_results=pre_scan_results)
        if parsed_data is None:
            logging.error(f"{get_current_utc_timestamp()} ❌ SARSAT message parsing failed — skipping further processing.")
            return

        alert_df = pd.DataFrame([parsed_data])

        logging.info(f"{get_current_utc_timestamp()} 📍 Parsed Positions - A: {parsed_data.get('latitude_a')}, "
                     f"B: {parsed_data.get('latitude_b')}, Status A: {parsed_data.get('position_status_a')}, "
                     f"Status B: {parsed_data.get('position_status_b')}")

        # Step 3: Finalize Alert Data
        # Fetch existing alerts from the database for sequence number assignment
        existing_alerts_db = get_existing_alerts()  # Fetch past alerts from the database

        alert_df = finalize_alert_df(alert_df, existing_alerts_db)  # Pass it to finalize_alert_df()
        
        logging.info(f"{get_current_utc_timestamp()} ✅ Finalized alert data.")


        # 🚨 DEBUG STOP: Print alert_df and exit 🚨
        print("✅ Finalized Alert DataFrame:")
        print(alert_df.to_string(index=False))  # Displays all columns and rows without truncation



        
        """
        # Step 4: Save Alert to Database
        try:
            save_alert_to_db(alert_df)
            logging.info(f"{get_current_utc_timestamp()} ✅ Alert saved with ID: {parsed_data.get('site_id')}")
        except Exception as e:
            log_error_and_continue(f"{get_current_utc_timestamp()} ❌ Failed to save alert to DB: {e}")

        # Step 5: Fetch Weather Data
        for position in ['A', 'B']:
            lat = parsed_data.get(f'latitude_{position.lower()}')
            lon = parsed_data.get(f'longitude_{position.lower()}')

            if pd.isna(lat) or pd.isna(lon):
                logging.info(f"{get_current_utc_timestamp()} 📍 Skipping weather fetch for Position {position} (No valid coordinates)")
                continue

            logging.info(f"{get_current_utc_timestamp()} 🌦️ Fetching weather for Position {position} ({lat}, {lon})")

            weather_shore_df = fetch_noaa_shore_data(lat, lon, position_label=position)
            weather_buoy_df = fetch_ndbc_buoy_data(lat, lon, position_label=position)

            combined_weather_df = pd.concat([weather_shore_df, weather_buoy_df], ignore_index=True)

            logging.debug(f"✅ Combined weather DataFrame columns for Position {position}: {combined_weather_df.columns.tolist()}")
            logging.debug(f"✅ Combined weather DataFrame for Position {position} (first 5 rows):\n{combined_weather_df.head()}")

            if not isinstance(combined_weather_df, pd.DataFrame):
                raise TypeError(f"Unexpected type for combined_weather_df at Position {position}: {type(combined_weather_df)}")

            if not combined_weather_df.empty:
                save_weather_to_db(combined_weather_df, parsed_data.get('site_id'), position)
                logging.info(f"{get_current_utc_timestamp()} ✅ Combined weather data saved for Position {position}")
            else:
                logging.warning(f"{get_current_utc_timestamp()} ⚠️ No complete weather data available for Position {position}")

            # Step 6: Fetch Weather Alerts
            try:
                alerts_df = fetch_weather_alerts_zone(lat, lon)
                if not alerts_df.empty:
                    logging.info(f"{get_current_utc_timestamp()} 🚨 {len(alerts_df)} weather alerts retrieved for Position {position}")
                else:
                    logging.info(f"{get_current_utc_timestamp()} ✅ No active weather alerts for Position {position}")
            except Exception as e:
                logging.error(f"{get_current_utc_timestamp()} ❌ Weather alerts fetch failed for Position {position}: {e}")
        """
                


        # Step 7: Generate GIS Map (HTML, online tiles)
        alert_row = alert_df.iloc[0]
        site_id_raw = alert_row.get('site_id')
        site_id_safe = None
        fallback_reason = None
        # Priority logic for site_id_safe
        if site_id_raw and str(site_id_raw).lower() not in ["none", "nan", ""]:
            site_id_safe = str(site_id_raw)
        elif alert_row.get('beacon_id'):
            site_id_safe = f"BEACON_{str(alert_row.get('beacon_id'))}"
            fallback_reason = "site_id missing; using beacon_id"
        elif alert_row.get('alert_sequence_number'):
            try:
                site_id_safe = f"ALERT_{int(alert_row.get('alert_sequence_number'))}"
                fallback_reason = "site_id missing; using alert_sequence_number"
            except Exception:
                pass
        if not site_id_safe:
            from datetime import datetime
            site_id_safe = f"SMOKE_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
            fallback_reason = "site_id missing; using UTC fallback"
        if fallback_reason:
            logging.warning(f"[RDS] GIS map step fallback: {fallback_reason} (site_id_safe={site_id_safe})")

        out_dir = os.path.join('data', 'maps', site_id_safe)
        os.makedirs(out_dir, exist_ok=True)

        # Build ephemeral positions_df (A row, and B row if present)
        rows = []
        # A
        lat_a = alert_row.get('position_lat_dd_a', alert_row.get('latitude_a'))
        lon_a = alert_row.get('position_lon_dd_a', alert_row.get('longitude_a'))
        rr_a  = alert_row.get('range_ring_meters_a')
        if pd.notna(lat_a) and pd.notna(lon_a):
            rows.append({
                'site_id': site_id_safe, 'role': 'A',
                'lat_dd': float(lat_a), 'lon_dd': float(lon_a),
                'range_ring_meters': float(rr_a) if pd.notna(rr_a) else 0.0,
                'position_status': alert_row.get('position_status_a'),
                'method': alert_row.get('position_method') or alert_row.get('position_method_a')
            })

        # B (optional)
        lat_b = alert_row.get('position_lat_dd_b', alert_row.get('latitude_b'))
        lon_b = alert_row.get('position_lon_dd_b', alert_row.get('longitude_b'))
        rr_b  = alert_row.get('range_ring_meters_b')
        if pd.notna(lat_b) and pd.notna(lon_b):
            rows.append({
                'site_id': site_id_safe, 'role': 'B',
                'lat_dd': float(lat_b), 'lon_dd': float(lon_b),
                'range_ring_meters': float(rr_b) if pd.notna(rr_b) else 0.0,
                'position_status': alert_row.get('position_status_b'),
                'method': alert_row.get('position_method') or alert_row.get('position_method_b')
            })

        positions_df = pd.DataFrame(rows)

        info = generate_gis_map_html_from_dfs(
            positions_df,
            out_dir,
            site_id=site_id_safe,
            tiles_mode="online"
        )
        print(f"✅ Interactive map saved: data/maps/{site_id_safe}/gis_map_{site_id_safe}.html")
                
    except Exception as e:
        log_error_and_continue(f"{get_current_utc_timestamp()} ❌ Pipeline processing error: {e}")
    
if __name__ == "__main__":
    sample_message_path = "C:/Users/gehig/Projects/RescueDecisionSystems/sample_sarsat_message.txt"
    example_alert_message = load_sample_message(sample_message_path)
    process_sarsat_alert(example_alert_message)
