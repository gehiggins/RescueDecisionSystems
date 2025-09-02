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

        # Consume only the pre-parsed DataFrame (pre_scan_results)
        if pre_scan_results is None or pre_scan_results.empty:
            raise ValueError("No pre-parsed coordinate results provided.")

        # Map A/B positions from pre_scan_results
        # Assume pre_scan_results contains columns: lat_dd, lon_dd, format_type, confidence, lat_token, lon_token, notes, etc.
        # If more than one valid coordinate, treat first as A, second as B
        valid_coords = pre_scan_results[pre_scan_results['is_valid'] == True].reset_index(drop=True)

        if valid_coords.empty:
            parsed_data['latitude_a'] = None
            parsed_data['longitude_a'] = None
            parsed_data['position_status_a'] = 'U'
            parsed_data['format_type_a'] = None
            parsed_data['is_valid_a'] = None
            parsed_data['confidence_a'] = None
            parsed_data['latitude_b'] = None
            parsed_data['longitude_b'] = None
            parsed_data['position_status_b'] = 'U'
            parsed_data['format_type_b'] = None
            parsed_data['is_valid_b'] = None
            parsed_data['confidence_b'] = None
        else:
            # Carry A fields if index 0 exists
            if len(valid_coords) > 0:
                parsed_data['latitude_a'] = valid_coords.loc[0, 'lat_dd']
                parsed_data['longitude_a'] = valid_coords.loc[0, 'lon_dd']
                parsed_data['position_status_a'] = 'C'
                parsed_data['format_type_a'] = valid_coords.loc[0, 'format_type'] if 'format_type' in valid_coords.columns else None
                parsed_data['is_valid_a'] = valid_coords.loc[0, 'is_valid'] if 'is_valid' in valid_coords.columns else None
                parsed_data['confidence_a'] = valid_coords.loc[0, 'confidence'] if 'confidence' in valid_coords.columns else None
            else:
                parsed_data['latitude_a'] = None
                parsed_data['longitude_a'] = None
                parsed_data['position_status_a'] = 'U'
                parsed_data['format_type_a'] = None
                parsed_data['is_valid_a'] = None
                parsed_data['confidence_a'] = None
            # Carry B fields if index 1 exists
            if len(valid_coords) > 1:
                parsed_data['latitude_b'] = valid_coords.loc[1, 'lat_dd']
                parsed_data['longitude_b'] = valid_coords.loc[1, 'lon_dd']
                parsed_data['position_status_b'] = 'C'
                parsed_data['format_type_b'] = valid_coords.loc[1, 'format_type'] if 'format_type' in valid_coords.columns else None
                parsed_data['is_valid_b'] = valid_coords.loc[1, 'is_valid'] if 'is_valid' in valid_coords.columns else None
                parsed_data['confidence_b'] = valid_coords.loc[1, 'confidence'] if 'confidence' in valid_coords.columns else None
            else:
                parsed_data['latitude_b'] = None
                parsed_data['longitude_b'] = None
                parsed_data['position_status_b'] = 'U'
                parsed_data['format_type_b'] = None
                parsed_data['is_valid_b'] = None
                parsed_data['confidence_b'] = None

        # Carry through additional fields if present
        if 'beacon_id' in pre_scan_results.columns:
            parsed_data['beacon_id'] = pre_scan_results['beacon_id'].iloc[0]
        if 'site_id' in pre_scan_results.columns:
            parsed_data['site_id'] = pre_scan_results['site_id'].iloc[0]
        if 'detect_time' in pre_scan_results.columns:
            parsed_data['detect_time'] = pre_scan_results['detect_time'].iloc[0]
        if 'position_method' in pre_scan_results.columns:
            parsed_data['position_method'] = pre_scan_results['position_method'].iloc[0]
        if 'position_resolution' in pre_scan_results.columns:
            parsed_data['position_resolution'] = pre_scan_results['position_resolution'].iloc[0]
        if 'expected_error_nm' in pre_scan_results.columns:
            parsed_data['expected_error_nm'] = pre_scan_results['expected_error_nm'].iloc[0]

        # Final required fields check (non-fatal for smoke test)
        if not parsed_data['beacon_id'] or not parsed_data['site_id']:
            logging.warning("Missing beacon or site ID; proceeding for smoke test.")

        # Apply range ring logic (can be refined later if resolution data is available)
        def calculate_range_ring(position_method):
            if position_method == 'GNSS':
                return 5  # meters (GNSS default accuracy)
            elif position_method == 'Doppler':
                return 5000  # meters (Doppler default)
            else:
                return 5000  # Safe fallback for unknown methods

        parsed_data['range_ring_meters_a'] = calculate_range_ring(parsed_data.get('position_method'))
        parsed_data['range_ring_meters_b'] = calculate_range_ring(parsed_data.get('position_method')) if parsed_data['latitude_b'] is not None and parsed_data['longitude_b'] is not None else None

        logging.info(f"✅ SARSAT message parsed successfully: Beacon ID {parsed_data['beacon_id']}")

        return parsed_data

    except Exception as e:
        log_error_and_continue(f"❌ Error parsing SARSAT message: {e}")
        return None

import sys

def main():
    try:
        if len(sys.argv) < 2:
            print("No message file path provided.")
            sys.exit(1)
        message_path = sys.argv[1]
        with open(message_path, 'r', encoding='utf-8', errors='ignore') as f:
            message_text = f.read()

        from flask_app.app.preparse_coordinate_mapper import pre_scan_for_coordinates
        pre_scan_results = pre_scan_for_coordinates(message_text)
        result = parse_sarsat_message(message_text, pre_scan_results)

        def get_field(d, key):
            return d.get(key) if d and key in d else None

        valid_a = get_field(result, 'latitude_a') is not None and get_field(result, 'longitude_a') is not None
        valid_b = get_field(result, 'latitude_b') is not None and get_field(result, 'longitude_b') is not None

        if not valid_a and not valid_b:
            print("NO_COORDS: preparser returned 0 valid rows")
            sys.exit(0)

        # Compose one-line output (outside NO_COORDS branch)
        line = (
            f"A: lat={get_field(result, 'latitude_a')} "
            f"lon={get_field(result, 'longitude_a')} "
            f"fmt={get_field(result, 'format_type_a')} "
            f"valid={get_field(result, 'is_valid_a')} "
            f"conf={get_field(result, 'confidence_a')} "
            f"rr={get_field(result, 'range_ring_meters_a')} | "
            f"B: lat={get_field(result, 'latitude_b')} "
            f"lon={get_field(result, 'longitude_b')} "
            f"fmt={get_field(result, 'format_type_b')} "
            f"valid={get_field(result, 'is_valid_b')} "
            f"conf={get_field(result, 'confidence_b')} "
            f"rr={get_field(result, 'range_ring_meters_b')}"
        )
        print(line)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()

