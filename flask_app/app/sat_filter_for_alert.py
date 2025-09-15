# ============================== RDS STANDARD HEADER ==============================
# Script Name: sat_filter_for_alert.py
# Last Updated (UTC): 2025-09-04
# Update Summary:
# - New module for scoping satellites to an alert and annotating visibility.
# - MVP annotates visibility only when sat subpoints are available (future). No elevation calc yet.
# Description:
# - Purpose: Provide helpers to select reporting-only or visible-now subsets and annotate 'visible_for'.
# - Primary Inputs:
#   * sat_overlay_df: per-satellite overlay rows (requires lat_dd/lon_dd for visibility tests).
#   * alert_df: authoritative alert with positions A/B (lat/lon).
# - Primary Outputs:
#   * sat_overlay_df with 'visible_for' âˆˆ {'A','B','AB', NaN}
# - External Data Sources:
#   * None.
# - Data Handling Notes:
#   * MVP: if sat lat/lon are NaN, visibility cannot be computed; leaves visible_for as NaN.
#   * Distance method: great-circle (Haversine) vs. footprint_radius_km.
# ===============================================================================

from app.setup_imports import *
import pandas as pd
import numpy as np
from math import radians, sin, cos, asin, sqrt
from typing import Optional


def annotate_visibility_distance(sat_overlay_df: pd.DataFrame, alert_df: pd.DataFrame) -> pd.DataFrame:
    """
    Annotate 'visible_for' based on simple distance check:
      distance(subpoint, A/B) <= footprint_radius_km  -> visible to that side.
    Requirements:
      - sat_overlay_df: ['lat_dd','lon_dd','footprint_radius_km']
      - alert_df: columns for A/B lat/lon:
          ['position_lat_dd_a','position_lon_dd_a','position_lat_dd_b','position_lon_dd_b']
    MVP caveat:
      - If sat lat/lon are NaN (no TLE), leaves visible_for as NaN.
    """
    required_sat_cols = {"lat_dd","lon_dd","footprint_radius_km"}
    if not required_sat_cols.issubset(sat_overlay_df.columns):
        logging.warning("[sat_filter_for_alert] Missing required sat columns; skip visibility.")
        return sat_overlay_df

    # Extract A/B (handles missing gracefully)
    A = _first_pair(alert_df, "position_lat_dd_a", "position_lon_dd_a")
    B = _first_pair(alert_df, "position_lat_dd_b", "position_lon_dd_b")

    def haversine_km(lat1, lon1, lat2, lon2):
        if not all(np.isfinite([lat1, lon1, lat2, lon2])):
            return float("nan")
        dlat = radians(lat2 - lat1)
        dlon = radians(lon2 - lon1)
        a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        return 6371.0 * c

    visible = []
    for _, r in sat_overlay_df.iterrows():
        lat = r.get("lat_dd", np.nan)
        lon = r.get("lon_dd", np.nan)
        R = r.get("footprint_radius_km", np.nan)
        if not np.isfinite(lat) or not np.isfinite(lon) or not np.isfinite(R):
            visible.append(np.nan)
            continue

        vis_a = False
        vis_b = False
        if A:
            da = haversine_km(lat, lon, A[0], A[1])
            vis_a = np.isfinite(da) and da <= R
        if B:
            db = haversine_km(lat, lon, B[0], B[1])
            vis_b = np.isfinite(db) and db <= R

        if vis_a and vis_b:
            visible.append("AB")
        elif vis_a:
            visible.append("A")
        elif vis_b:
            visible.append("B")
        else:
            visible.append(np.nan)

    sat_overlay_df = sat_overlay_df.copy()
    sat_overlay_df["visible_for"] = visible
    return sat_overlay_df


# ----------------------- Helpers -----------------------

def _first_pair(df: pd.DataFrame, lat_col: str, lon_col: str) -> Optional[tuple[float,float]]:
    if lat_col in df.columns and lon_col in df.columns:
        lat_series = df[lat_col].dropna()
        lon_series = df[lon_col].dropna()
        if len(lat_series) > 0 and len(lon_series) > 0:
            try:
                return float(lat_series.iloc[0]), float(lon_series.iloc[0])
            except Exception:
                return None
    return None

