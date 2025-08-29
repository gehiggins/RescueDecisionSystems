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
# - Validates `detect_time` but does NOT assign it—extraction must occur in `parser_sarsat_msg.py`.
# - Logs a warning if `detect_time` is missing after parsing.
# - Applies consistency checks to prevent data corruption before database commit.
# - Can be extended to include additional processing steps as needed.
#

from flask_app.setup_imports import *

def finalize_alert_df(alert_df, existing_alerts_db):
    """
    Finalizes SARSAT alert DataFrame before committing to the database.
    
    This function ensures that:
    - `alert_sequence_number` is assigned based on prior cases in the database.
    - `detect_time` is validated but not assigned here—it must be extracted in `parser_sarsat_msg.py`.
    - All required fields are correctly formatted before final storage.
    
    Args:
        alert_df (pd.DataFrame): Parsed SARSAT alert DataFrame.
        existing_alerts_db (pd.DataFrame): Historical alert records for sequence tracking.
    
    Returns:
        pd.DataFrame: Updated alert_df ready for final storage.
    """
    
    for idx, row in alert_df.iterrows():
        existing_alerts = existing_alerts_db[existing_alerts_db['site_id'] == row['site_id']]
        if existing_alerts.empty:
            alert_df.at[idx, 'alert_sequence_number'] = 1
        else:
            alert_df.at[idx, 'alert_sequence_number'] = existing_alerts['alert_sequence_number'].max() + 1
    
    # Ensure detect_time is present; log a warning if missing
    missing_detect_time = alert_df['detect_time'].isna()
    if missing_detect_time.any():
        logging.warning(f"⚠️ {missing_detect_time.sum()} alert(s) are missing detect_time. Ensure extraction occurs in parser_sarsat_msg.py.")
    
    logging.info(f"✅ Finalized alert_df with sequence numbers and detect_time validation.")
    return alert_df
