# preparse_coordinate_mapper.py - 2025-03-06 (Corrected Column Names Version)
# Pre-parse module for scanning SARSAT messages and mapping coordinate locations (A/B pairs)

from app.setup_imports import *
from app.utils_coordinates import is_valid_coordinate, coordinate_pair_to_dd

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
            - is_valid: Whether the pair is fully valid
    """

    coordinate_pairs = []
    lines = raw_message.splitlines()

    coord_pattern = re.compile(
        r'(\d{2,3}\s?\d{1,2}\.\d{1,3}[NS])\s+(\d{2,3}\s?\d{1,2}\.\d{1,3}[EW])'
    )

    for line_idx, line in enumerate(lines):
        for match in coord_pattern.finditer(line):
            lat_str, lon_str = match.groups()
            start_pos = match.start()
            end_pos = match.end()

            lat_valid = is_valid_coordinate(lat_str)
            lon_valid = is_valid_coordinate(lon_str)
            is_valid_pair = lat_valid and lon_valid

            lat_dd, lon_dd = None, None
            if is_valid_pair:
                lat_dd, lon_dd = coordinate_pair_to_dd(lat_str, lon_str)

            coordinate_pairs.append({
                "lat": lat_str,
                "lon": lon_str,
                "lat_dd": lat_dd,            # âœ… Corrected column name
                "lon_dd": lon_dd,            # âœ… Corrected column name
                "start_pos": start_pos,
                "end_pos": end_pos,
                "is_valid": is_valid_pair
            })

            logging.debug(f"Detected coordinate pair: {lat_str}, {lon_str}, valid={is_valid_pair}")

    coord_df = pd.DataFrame(coordinate_pairs)

    logging.info(f"ðŸ“ Pre-parse detected {len(coord_df)} coordinate pairs (valid+invalid).")
    return coord_df

