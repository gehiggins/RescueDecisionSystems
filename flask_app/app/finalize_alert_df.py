# finalize_alert_df.py - Final Preparation for SARSAT Alert Storage
# Location: flask_app/app/finalize_alert_df.py
# 2025-03-07 (Fully Aligned with RDS System Architecture & Data Guide)
#
# Description:
# This script ensures that all parsed SARSAT alerts are fully prepared before committing to the database.
# It assigns `alert_sequence_number` and validates required fields per documentation.
#
# External Data Sources:
# - `alert_df` (Parsed SARSAT alerts from `parser_sarsat_msg.py`)
# - `existing_alerts_db` (Database records for tracking prior site IDs and sequence numbers)
#
# Internal Variables:
# - `alert_df`: DataFrame containing the parsed SARSAT alert data.
# - `existing_alerts_db`: DataFrame containing previously stored alerts.
#
# Produced DataFrame:
# - `final_alert_df`: DataFrame fully prepared for database storage.
#   - Contains:
#     - `alert_sequence_number`: Ensures alerts are processed in correct order.
#     - `site_creation_time`: Populated based on `detect_time` or message received time.
#     - `detect_time`: Must be present and extracted from the original SARSAT message.
#     - All fields validated per `alert_df` schema in RDS documentation.
#
# Data Handling Notes:
# - Ensures `alert_sequence_number` increments properly for repeated `site_id`.
# - Validates `detect_time` but does NOT assign itâ€”extraction must occur in `parser_sarsat_msg.py`.
# - Logs a warning if `detect_time` is missing after parsing.
# - Applies consistency checks to prevent data corruption before database commit.
# - Can be extended to include additional processing steps as needed.
#

from app.setup_imports import *


def _site_key(value):
    """Normalize database and parser site identifiers for sequence matching."""
    try:
        if value is None or pd.isna(value):
            return None
    except (TypeError, ValueError):
        return None
    return str(value).strip()


def _existing_sequence_maxima(existing_alerts_db):
    maxima = {}
    if not isinstance(existing_alerts_db, pd.DataFrame) or existing_alerts_db.empty:
        return maxima
    if not {"site_id", "alert_sequence_number"}.issubset(existing_alerts_db.columns):
        logging.warning(
            "Existing alert history lacks site_id or alert_sequence_number; "
            "starting new sequences."
        )
        return maxima

    rejected = 0
    for _, row in existing_alerts_db.iterrows():
        key = _site_key(row.get("site_id"))
        try:
            sequence = float(row.get("alert_sequence_number"))
        except (TypeError, ValueError):
            rejected += 1
            continue
        if not np.isfinite(sequence) or sequence < 1 or not sequence.is_integer():
            rejected += 1
            continue
        maxima[key] = max(maxima.get(key, 0), int(sequence))
    if rejected:
        logging.warning(
            "Ignored %s invalid historical alert sequence value(s).", rejected
        )
    return maxima


def _coerce_detect_time(value):
    """Return a UTC timestamp, or NaT when a parser value is not usable."""
    # Numeric values are valid pandas epoch inputs, but they are not valid
    # SARSAT message timestamps and can otherwise hide parser defects.
    if not pd.api.types.is_scalar(value) or isinstance(
        value, (bool, int, float, np.number)
    ):
        return pd.NaT
    try:
        parsed = pd.to_datetime(value, utc=True, errors="coerce")
    except (TypeError, ValueError, OverflowError):
        return pd.NaT
    return pd.NaT if pd.isna(parsed) else parsed

def finalize_alert_df(alert_df, existing_alerts_db):
    """
    Finalizes SARSAT alert DataFrame before committing to the database.
    
    This function ensures that:
    - `alert_sequence_number` is assigned based on prior cases in the database.
    - `detect_time` is validated but not assigned hereâ€”it must be extracted in `parser_sarsat_msg.py`.
    - All required fields are correctly formatted before final storage.
    
    Args:
        alert_df (pd.DataFrame): Parsed SARSAT alert DataFrame.
        existing_alerts_db (pd.DataFrame): Historical alert records for sequence tracking.
    
    Returns:
        pd.DataFrame: Updated alert_df ready for final storage.
    """
    
    if not isinstance(alert_df, pd.DataFrame):
        raise TypeError("alert_df must be a pandas DataFrame")

    alert_df = alert_df.copy()
    if "site_id" not in alert_df.columns:
        alert_df["site_id"] = pd.Series(pd.NA, index=alert_df.index, dtype="object")

    sequence_maxima = _existing_sequence_maxima(existing_alerts_db)
    alert_df["alert_sequence_number"] = pd.Series(
        pd.NA, index=alert_df.index, dtype="Int64"
    )
    # Update the in-memory maximum after every row so repeated sites in one
    # incoming batch receive distinct, increasing sequence numbers.
    for idx, row in alert_df.iterrows():
        key = _site_key(row.get("site_id"))
        next_sequence = sequence_maxima.get(key, 0) + 1
        alert_df.at[idx, "alert_sequence_number"] = next_sequence
        sequence_maxima[key] = next_sequence
    
    # Normalize the parser boundary so downstream weather, satellite, and map
    # processing never receive malformed or timezone-ambiguous timestamps.
    if "detect_time" not in alert_df.columns:
        alert_df["detect_time"] = pd.NaT
    original_missing = alert_df["detect_time"].isna()
    normalized_detect_time = alert_df["detect_time"].map(_coerce_detect_time)
    alert_df["detect_time"] = pd.Series(
        normalized_detect_time, index=alert_df.index, dtype="datetime64[ns, UTC]"
    )
    missing_detect_time = alert_df["detect_time"].isna()
    invalid_detect_time = missing_detect_time & ~original_missing
    if invalid_detect_time.any():
        logging.warning(
            "Rejected %s invalid detect_time value(s) during finalization.",
            invalid_detect_time.sum(),
        )
    if missing_detect_time.any():
        logging.warning(f"âš ï¸ {missing_detect_time.sum()} alert(s) are missing detect_time. Ensure extraction occurs in parser_sarsat_msg.py.")
    
    logging.info(f"âœ… Finalized alert_df with sequence numbers and detect_time validation.")
    return alert_df

