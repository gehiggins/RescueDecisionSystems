# ============================== RDS STANDARD HEADER ==============================
# Script Name: sat_compute_groundtrack.py
# Last Updated (UTC): 2025-09-04
# Update Summary:
# - New module for subpoint and ground-track computation (TLE-driven). MVP stubs only.
# Description:
# - Purpose: Given TLEs and a timestamp, compute sub-satellite points "now" and short past/future tracks.
# - Primary Inputs:
#   * tle_df with columns ['sat_id'|'name','tle_line1','tle_line2','epoch_utc']
#   * when_utc (snapshot time)
# - Primary Outputs:
#   * subpoints_df: ['sat_id','lat_dd','lon_dd']
#   * sat_tracks_df: ['sat_id','when_utc','lat_dd','lon_dd','segment'] where segment âˆˆ {'past','future'}
# - External Data Sources:
#   * None. Uses provided TLEs.
# - Data Handling Notes:
#   * MVP returns empty stubs. Future will use SGP4/Skyfield.
# ===============================================================================

from app.setup_imports import *
from datetime import datetime
from typing import Optional

import pandas as pd

# Project-wide rule: internal imports must use `from app.` prefix
from app import __init__ as app_root  # noqa: F401


def compute_subpoint_now(tle_df: pd.DataFrame, when_utc: datetime) -> pd.DataFrame:
    """
    MVP stub: return empty DataFrame until TLE propagation is enabled.
    Future: compute sub-satellite (lat_dd, lon_dd) for each row in tle_df at when_utc.
    """
    logging.info("[sat_compute_groundtrack] Subpoint computation not implemented in MVP.")
    return pd.DataFrame(columns=["sat_id","lat_dd","lon_dd"])


def compute_tracks(
    tle_df: pd.DataFrame,
    when_utc: datetime,
    minutes_past: int = 10,
    minutes_future: int = 10,
    step_s: int = 60
) -> pd.DataFrame:
    """
    MVP stub: return empty DataFrame until TLE propagation is enabled.
    Future: generate per-satellite linestring points for dashed (past) and dotted (future) tracks.
    """
    logging.info("[sat_compute_groundtrack] Track computation not implemented in MVP.")
    return pd.DataFrame(columns=["sat_id","when_utc","lat_dd","lon_dd","segment"])

