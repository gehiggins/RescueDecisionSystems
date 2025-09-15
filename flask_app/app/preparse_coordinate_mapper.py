
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

from app.setup_imports import *

from app.utils_coordinates import clean_and_standardize_coordinate
from app.field_validator import (
    validate_and_extract_coordinate_token,
    validate_and_extract_coordinate_pair,
)

def _short(s, n=200):
    return (s or "")[:n]

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


    import os
    coordinate_pairs = []
    lines = raw_message.splitlines()
    offset = 0  # Running character offset for global spans

    for line_idx, line in enumerate(lines):
        result = validate_and_extract_coordinate_pair(
            field_name="coord_pair",
            raw_text=line,
            config={},
            context={}
        )
        if result.get("is_valid"):
            lat_token = result.get("lat_token")
            lon_token = result.get("lon_token")
            # Use validator tokens if present, else fallback to slice
            lat_val = lat_token if lat_token else line[result.get("start_pos"):result.get("end_pos")] if result.get("start_pos") is not None and result.get("end_pos") is not None else line
            lon_val = lon_token if lon_token else line[result.get("start_pos"):result.get("end_pos")] if result.get("start_pos") is not None and result.get("end_pos") is not None else line
            # Compute global spans
            start_pos = offset + (result.get("start_pos") if result.get("start_pos") is not None else 0)
            end_pos = offset + (result.get("end_pos") if result.get("end_pos") is not None else len(line))
            coordinate_pairs.append({
                "lat": lat_val,
                "lon": lon_val,
                "lat_dd": result.get("lat_dd"),
                "lon_dd": result.get("lon_dd"),
                "start_pos": start_pos,
                "end_pos": end_pos,
                "format_type": result.get("format_type", "Unknown"),
                "is_valid": result.get("is_valid"),
                "confidence": result.get("confidence", 0.0),
                "notes": _short("; ".join(result.get("notes", [])), 120)
            })
            # Deterministic: stop on first valid pair per line
        offset += len(line) + 1  # +1 for newline

    # Build DataFrame with fixed column order
    columns = ["lat", "lon", "lat_dd", "lon_dd", "start_pos", "end_pos", "format_type", "is_valid", "confidence", "notes"]
    coord_df = pd.DataFrame(coordinate_pairs, columns=columns)

    # Ensure output directory exists
    debug_csv_path = os.path.abspath("data/debugging/debug_preparsed_coordinates.csv")
    os.makedirs(os.path.dirname(debug_csv_path), exist_ok=True)
    coord_df.to_csv(debug_csv_path, index=False)
    print(f"âœ… Saved Preparsed Coordinates to: {debug_csv_path}")

    return coord_df

