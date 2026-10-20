
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

import re

from app.debug_outputs import dump_dataframe
from app.utils_coordinates import clean_and_standardize_coordinate
from app.field_validator import (
    validate_and_extract_coordinate_token,
    validate_and_extract_coordinate_pair,
)

def _short(s, n=200):
    return (s or "")[:n]


def _token_span(raw_text, token):
    """Locate a normalized coordinate token in the original source text."""
    if not isinstance(token, str):
        return None
    parts = token.strip().split()
    if not parts:
        return None
    # Validators collapse whitespace before matching. Rebuild a whitespace-
    # tolerant expression so the stored offsets still reference the raw alert.
    pattern = r"\s+".join(re.escape(part) for part in parts)
    match = re.search(pattern, raw_text, re.IGNORECASE)
    return match.span() if match else None


def _coordinate_pair_span(raw_text, lat_token, lon_token):
    """Return the raw span covering both validated coordinate tokens."""
    lat_span = _token_span(raw_text, lat_token)
    lon_span = _token_span(raw_text, lon_token)
    if lat_span is None or lon_span is None:
        return None
    return min(lat_span[0], lon_span[0]), max(lat_span[1], lon_span[1])

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
    offset = 0  # Running character offset for global spans

    # Keep each original line ending so global spans remain exact for LF, CRLF,
    # and CR messages. Validation receives only the line content.
    for raw_line in raw_message.splitlines(keepends=True):
        line = raw_line.rstrip("\r\n")
        cursor = 0
        while cursor < len(line):
            segment = line[cursor:]
            result = validate_and_extract_coordinate_pair(
                field_name="coord_pair",
                raw_text=segment,
                config={},
                context={}
            )
            if not result.get("is_valid"):
                break
            lat_token = result.get("lat_token")
            lon_token = result.get("lon_token")
            pair_span = _coordinate_pair_span(segment, lat_token, lon_token)
            if pair_span is None:
                pair_start = result.get("start_pos")
                pair_end = result.get("end_pos")
                if not isinstance(pair_start, int) or not isinstance(pair_end, int):
                    break
            else:
                pair_start, pair_end = pair_span
            if pair_end <= pair_start:
                break
            lat_val = lat_token if lat_token else segment[pair_start:pair_end]
            lon_val = lon_token if lon_token else segment[pair_start:pair_end]
            start_pos = offset + cursor + pair_start
            end_pos = offset + cursor + pair_end
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
            # Continue after the raw match so compact messages containing A
            # and B on the same line retain both positions without re-matching.
            cursor += pair_end
        offset += len(raw_line)

    # Build DataFrame with fixed column order
    columns = ["lat", "lon", "lat_dd", "lon_dd", "start_pos", "end_pos", "format_type", "is_valid", "confidence", "notes"]
    coord_df = pd.DataFrame(coordinate_pairs, columns=columns)

    dump_dataframe(coord_df, "debug_preparsed_coordinates.csv")

    return coord_df

