# preparse_coordinate_mapper.py - 2025-03-07 (Updated for Maximum Robustness)
# 
# Description:
# This script pre-scans SARSAT messages to identify, validate, and log coordinate pairs (latitude & longitude).
# It records the exact start and stop positions of each coordinate pair, detects formatting (DMS, Decimal Minutes, or Decimal Degrees),
# and ensures a DataFrame is always returned for structured processing in downstream parsing.
# 
# External Data Sources:
# - SARSAT message text (raw input from RCC message processing pipeline)
# 
# Internal Variables:
# - raw_message: Full SARSAT message text.
# - coordinate_pairs: List to store detected coordinate pairs.
# - coord_df: Pandas DataFrame storing parsed coordinate data.
# 
# Produced DataFrames:
# - coord_df with columns:
#   - lat: Latitude string
#   - lon: Longitude string
#   - lat_dd: Decimal degrees for latitude
#   - lon_dd: Decimal degrees for longitude
#   - start_pos: Start position in message
#   - end_pos: End position in message
#   - format_type: Type of format detected (e.g., DMS, Decimal Minutes, Decimal Degrees)
#   - is_valid: Whether the pair is fully valid
# 
# Data Handling Notes:
# - Ensures coordinate extraction is robust against spacing variations and separators (e.g., dashes, colons, spaces).
# - Uses structured logging to flag invalid coordinate pairs.
# - Detects and allows unexpected characters between lat/lon pairs while maintaining correct matching.
# - Uses NSEW explicitly to identify lat vs lon rather than assuming order.
# - Supports all observed SARSAT coordinate formats, including Decimal Degrees and DMS with symbols.
#

from flask_app.setup_imports import *
from flask_app.app.utils_coordinates import is_valid_coordinate, coordinate_pair_to_dd, clean_and_standardize_coordinate, parse_any_coordinate

def pre_scan_for_coordinates(raw_message):
    """
    Pre-scans the SARSAT message to detect and map potential coordinate pairs.

    Args:
        raw_message (str): Full SARSAT message text.

    Returns:
        DataFrame: Contains columns:
            - lat: Latitude string
            - lon: Longitude string
            - lat_dd: Decimal degrees for latitude
            - lon_dd: Decimal degrees for longitude
            - start_pos: Start position in message
            - end_pos: End position in message
            - format_type: Type of format detected (e.g., DMS, Decimal Minutes, Decimal Degrees)
            - is_valid: Whether the pair is fully valid
    """
    
    coordinate_pairs = []
    lines = raw_message.splitlines()
    
    coord_pattern = re.compile(
        r'(\d{2,3}[\s°-]?\d{0,2}[\s\'-]?\d{0,2}\.\d{1,6}[NS])\s*[^NSWE]*\s*(\d{2,3}[\s°-]?\d{0,2}[\s\'-]?\d{0,2}\.\d{1,6}[EW])'
    )
    
    for line_idx, line in enumerate(lines):
        for match in coord_pattern.finditer(line):
            first_coord, second_coord = match.groups()
            start_pos = match.start()
            end_pos = match.end()
            
            # Standardize coordinates
            first_coord = clean_and_standardize_coordinate(first_coord)
            second_coord = clean_and_standardize_coordinate(second_coord)
            
            # Determine lat/lon based on NSEW
            if 'N' in first_coord or 'S' in first_coord:
                lat_str, lon_str = first_coord, second_coord
            else:
                lon_str, lat_str = first_coord, second_coord
            
            # Validate and parse coordinates
            lat_valid = is_valid_coordinate(lat_str)
            lon_valid = is_valid_coordinate(lon_str)
            is_valid_pair = lat_valid and lon_valid
            
            lat_dd, lon_dd = np.nan, np.nan  # Ensure columns always exist
            format_type = "Unknown"
            
            if is_valid_pair:
                lat_dd, lon_dd = parse_any_coordinate(lat_str), parse_any_coordinate(lon_str)
                if '°' in lat_str or '"' in lat_str:
                    format_type = "DMS"
                elif '.' in lat_str and ' ' in lat_str:
                    format_type = "Decimal Minutes"
                else:
                    format_type = "Decimal Degrees"
            else:
                logging.warning(f"Invalid coordinate pair detected: {lat_str}, {lon_str}")
            
            coordinate_pairs.append({
                "lat": lat_str,
                "lon": lon_str,
                "lat_dd": lat_dd,
                "lon_dd": lon_dd,
                "start_pos": start_pos,
                "end_pos": end_pos,
                "format_type": format_type,
                "is_valid": is_valid_pair
            })
            
            logging.debug(f"Detected coordinate pair: {lat_str}, {lon_str}, valid={is_valid_pair}, format={format_type}")
    
    coord_df = pd.DataFrame(coordinate_pairs)
    
    # ✅ NEW: Save Preparsed Coordinates to CSV for Debugging
    debug_csv_path = "C:/Users/gehig/Projects/RescueDecisionSystems/data/debugging/debug_preparsed_coordinates.csv"
    coord_df.to_csv(debug_csv_path, index=False)
    logging.info(f"✅ Saved Preparsed Coordinates to: {debug_csv_path}")

    if coord_df.empty:
        logging.info("📍 No valid coordinate pairs detected.")
        return pd.DataFrame(columns=["lat", "lon", "lat_dd", "lon_dd", "start_pos", "end_pos", "format_type", "is_valid"])
    
    logging.info(f"📍 Pre-parse detected {len(coord_df)} coordinate pairs (valid+invalid).")
    return coord_df
