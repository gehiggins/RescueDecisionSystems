# ============================== RDS STANDARD HEADER ==============================
# Script Name: sat_compute_footprint.py
# Last Updated (UTC): 2025-09-04
# Update Summary:
# - New module for footprint (viewing horizon) radius and (optional) geodesic circle generation.
# Description:
# - Purpose: Ensure each satellite overlay row has a valid footprint_radius_km. If missing,
#   compute from altitude using the horizon-angle formula. Optionally provide a geodesic circle API.
# - Primary Inputs:
#   * sat_overlay_df with ['altitude_km','footprint_radius_km'] (may be NaN)
# - Primary Outputs:
#   * sat_overlay_df with 'footprint_radius_km' filled where possible
# - External Data Sources:
#   * None.
# - Data Handling Notes:
#   * Uses Earth mean radius Re=6371 km. Returns radii in km.
# ===============================================================================

from flask_app.setup_imports import *
from math import acos

import pandas as pd
import numpy as np

# Project-wide rule: internal imports must use `from app.` prefix
from app import __init__ as app_root  # noqa: F401

RE_KM = 6371.0


def horizon_radius_km(altitude_km: float) -> float:
    """
    Ground-radius of the viewing horizon on a spherical Earth:
      r = Re * arccos(Re / (Re + h))
    Returns NaN if altitude is not finite or <= 0.
    """
    try:
        h = float(altitude_km)
        if not np.isfinite(h) or h <= 0:
            return float("nan")
        return RE_KM * acos(RE_KM / (RE_KM + h))
    except Exception:
        return float("nan")


def annotate_footprint_radius(sat_overlay_df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure 'footprint_radius_km' is present; if NaN, compute from altitude_km.
    If both are missing, leave as NaN (map can still draw other features).
    """
    if "footprint_radius_km" not in sat_overlay_df.columns:
        sat_overlay_df["footprint_radius_km"] = np.nan
    if "altitude_km" not in sat_overlay_df.columns:
        sat_overlay_df["altitude_km"] = np.nan

    mask_missing = sat_overlay_df["footprint_radius_km"].isna()
    if mask_missing.any():
        sat_overlay_df.loc[mask_missing, "footprint_radius_km"] = sat_overlay_df.loc[
            mask_missing, "altitude_km"
        ].apply(horizon_radius_km)
    return sat_overlay_df
