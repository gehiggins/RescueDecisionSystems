# parser_sarsat_msg.py - Updated for Full Pre-Scan Integration & Robust Coordinate Handling (2025-03-06)

from flask_app.setup_imports import *
from flask_app.app.utils_coordinates import coordinate_pair_to_dd, is_valid_coordinate, clean_and_standardize_coordinate
from flask_app.app.utils import log_error_and_continue

print("📦 LOADED: parser_sarsat_msg.py from flask_app/app")

def parse_sarsat_message(message_text, pre_scan_results=None):
    """
    Parses a SARSAT message and extracts key data fields.
    Pre-scan coordinate pairs are passed in as a DataFrame for validation against extracted coordinates.
    """

    try:
        logging.info("🛰️ Parsing SARSAT message")

        parsed_data = {
            'beacon_id': None,
            'site_id': None,
            'latitude_a': None,
            'longitude_a': None,
            'latitude_b': None,
            'longitude_b': None,
            'position_status_a': None,
            'position_status_b': None,
            'position_method': None,
            'position_resolution': None,
            'expected_error_nm': None,
            'detect_time': None,  # Added detect_time placeholder
        }

        # Convert pre-scan results to tuples for easy matching
        pre_scan_pairs = []
        if pre_scan_results is not None and not pre_scan_results.empty:
            pre_scan_pairs = list(zip(pre_scan_results['lat_dd'], pre_scan_results['lon_dd']))

        lines = message_text.split('\n')
        position_count = 0

        for line in lines:
            line = line.strip()

            if "BEACON ID" in line:
                parts = line.split("SITE ID:")
                parsed_data['beacon_id'] = parts[0].split(":")[1].strip().split()[0]
                parsed_data['site_id'] = parts[1].strip()

            elif "TIME OF DETECTION" in line:  # New - capture detect_time
                parsed_data['detect_time'] = pd.to_datetime(line.split(":")[1].strip(), errors='coerce')

            elif "PROB EE SOL LATITUDE LONGITUDE" in line:
                position_count += 1
                position_line = lines[lines.index(line) + 1].strip()
                position_data = position_line.split()

                if position_count == 1:
                    position_key = 'a'
                elif position_count == 2:
                    position_key = 'b'
                else:
                    continue  # Ignore positions beyond A/B

                lat_str = f"{position_data[3]} {position_data[4]}"
                lon_str = f"{position_data[5]} {position_data[6]}"

                # Standardize and parse to decimal degrees
                coord_string = f"{lat_str} {lon_str}"
                coord_string = clean_and_standardize_coordinate(coord_string)
                lat_dd, lon_dd = coordinate_pair_to_dd(coord_string)

                logging.debug(f"🧪 Position {position_key.upper()} raw string: {coord_string} → lat: {lat_dd}, lon: {lon_dd}")


                if lat_dd is None or lon_dd is None:
                    logging.warning(f"⚠️ Failed to parse coordinate pair for Position {position_key.upper()}: {lat_str} / {lon_str}")
                    continue

                parsed_data[f'latitude_{position_key}'] = lat_dd
                parsed_data[f'longitude_{position_key}'] = lon_dd
                parsed_data[f'position_status_{position_key}'] = 'C'  # Default to Confirmed

                # Cross-check with pre-scan
                if pre_scan_pairs:
                    match_found = any(
                        abs(pre_lat - lat_dd) < 0.0001 and abs(pre_lon - lon_dd) < 0.0001
                        for pre_lat, pre_lon in pre_scan_pairs
                    )
                    if not match_found:
                        logging.warning(f"⚠️ Parsed Position {position_key.upper()} ({lat_dd}, {lon_dd}) does not match any pre-scanned coordinates.")

            elif "POSITION DEVICE" in line:
                parsed_data['position_method'] = line.split(":")[1].strip()

            elif "POSITION RESOLUTION" in line:
                parsed_data['position_resolution'] = line.split(":")[1].strip()

            elif "EXPECTED HORIZONTAL ERROR" in line:
                parsed_data['expected_error_nm'] = float(line.split(":")[1].strip().split()[0])

            elif "N/A N/A U N/A" in line:  # Handle unlocated position status
                position_count += 1
                if position_count == 1:
                    parsed_data['position_status_a'] = 'U'
                elif position_count == 2:
                    parsed_data['position_status_b'] = 'U'

        # Final required fields check
        if not parsed_data['beacon_id'] or not parsed_data['site_id']:
            raise ValueError("Missing required beacon or site ID.")

        # Apply range ring logic (can be refined later if resolution data is available)
        def calculate_range_ring(position_method):
            if position_method == 'GNSS':
                return 5  # meters (GNSS default accuracy)
            elif position_method == 'Doppler':
                return 5000  # meters (Doppler default)
            else:
                return 5000  # Safe fallback for unknown methods

        parsed_data['range_ring_meters_a'] = calculate_range_ring(parsed_data['position_method'])

        # If Position B exists, it should use the same method
        if pd.notna(parsed_data['latitude_b']) and pd.notna(parsed_data['longitude_b']):
            parsed_data['range_ring_meters_b'] = calculate_range_ring(parsed_data['position_method'])
        else:
            parsed_data['range_ring_meters_b'] = None  # Explicitly handle missing Position B

        logging.info(f"✅ SARSAT message parsed successfully: Beacon ID {parsed_data['beacon_id']}")

        return parsed_data

    except Exception as e:
        log_error_and_continue(f"❌ Error parsing SARSAT message: {e}")
        return None
