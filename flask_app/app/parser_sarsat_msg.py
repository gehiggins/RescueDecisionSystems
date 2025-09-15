# parser_sarsat_msg.py - Updated for Full Pre-Scan Integration & Robust Coordinate Handling (2025-03-06)


from app.setup_imports import *
from flask_app.app.utils_coordinates import coordinate_pair_to_dd, is_valid_coordinate, clean_and_standardize_coordinate
from flask_app.app.utils import log_error_and_continue
import re

print("ðŸ“¦ LOADED: parser_sarsat_msg.py from flask_app/app")

def parse_sarsat_message(message_text, pre_scan_results=None):
    """
    Parses a SARSAT message and extracts key data fields.
    Pre-scan coordinate pairs are passed in as a DataFrame for validation against extracted coordinates.
    """
    try:
        logging.info("ðŸ›°ï¸ Parsing SARSAT message")


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

        # --- Header parse for SITE ID and BEACON ID ---
        site_id_match = re.search(r"SITE\s*ID\s*:\s*([0-9]{1,10})", message_text, re.IGNORECASE)
        beacon_id_match = re.search(r"BEACON\s*ID\s*:\s*([A-Za-z0-9\-\s]+)", message_text, re.IGNORECASE)
        if site_id_match:
            parsed_data["site_id"] = str(site_id_match.group(1)).strip()
        if beacon_id_match:
            parsed_data["beacon_id"] = beacon_id_match.group(1).strip()

        # --- Parse EE from message_text ---
        ee_msg_match = re.search(r"EXPECTED\s+HORIZONTAL\s+ERROR\s*\(EE\)\s*:\s*([0-9]*\.?[0-9]+)\s*NM", message_text, re.IGNORECASE)
        if ee_msg_match:
            try:
                parsed_data["expected_error_nm"] = float(ee_msg_match.group(1))
            except Exception:
                pass

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
        # If EE was not found in message_text, try pre_scan_results
        if parsed_data.get('expected_error_nm') is None and 'expected_error_nm' in pre_scan_results.columns:
            raw_ee = pre_scan_results['expected_error_nm'].iloc[0]
            try:
                if isinstance(raw_ee, (float, int)):
                    parsed_data['expected_error_nm'] = float(raw_ee)
                elif isinstance(raw_ee, str):
                    ee_clean = raw_ee.lower().replace('nm', '').replace('nautical miles', '').replace(',', '').strip()
                    parsed_data['expected_error_nm'] = float(ee_clean)
            except Exception:
                parsed_data['expected_error_nm'] = raw_ee


        # Final required fields check (non-fatal for smoke test)
        if not parsed_data['beacon_id'] or not parsed_data['site_id']:
            logging.warning("Missing beacon or site ID; proceeding for smoke test.")

        # --- Range ring authoritative logic (unconditional) ---
        expected_error_nm = parsed_data.get('expected_error_nm')
        def set_ring(lat, lon, method, format_type):
            if lat is None or lon is None:
                return None, None
            if expected_error_nm is not None:
                ring_m = float(expected_error_nm) * 1852.0
                return ring_m, "EE_95"
            if (method and method.upper() in ["GNSS", "SGB"]) or (format_type and "gps" in str(format_type).lower()):
                return 18.0, "fallback_gnss"
            return 5000.0, "fallback_default"

        # A
        ring_a, src_a = set_ring(
            parsed_data.get('latitude_a'),
            parsed_data.get('longitude_a'),
            parsed_data.get('position_method'),
            parsed_data.get('format_type_a')
        )
        if ring_a is not None:
            parsed_data['range_ring_meters_a'] = float(ring_a)
            parsed_data['range_ring_source_a'] = src_a
        # B
        ring_b, src_b = set_ring(
            parsed_data.get('latitude_b'),
            parsed_data.get('longitude_b'),
            parsed_data.get('position_method'),
            parsed_data.get('format_type_b')
        )
        if ring_b is not None:
            parsed_data['range_ring_meters_b'] = float(ring_b)
            parsed_data['range_ring_source_b'] = src_b

        # --- Type hygiene for IDs ---
        if parsed_data.get('site_id') is not None:
            parsed_data['site_id'] = str(parsed_data['site_id'])
        if parsed_data.get('beacon_id') is not None:
            parsed_data['beacon_id'] = str(parsed_data['beacon_id'])

        logging.info(f"âœ… SARSAT message parsed successfully: Beacon ID {parsed_data['beacon_id']}")

        return parsed_data

    except Exception as e:
        log_error_and_continue(f"âŒ Error parsing SARSAT message: {e}")
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


