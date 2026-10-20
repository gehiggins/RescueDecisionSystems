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

from app.setup_imports import *
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
    if not isinstance(sat_overlay_df, pd.DataFrame):
        raise TypeError("sat_overlay_df must be a pandas DataFrame")

    sat_overlay_df = sat_overlay_df.copy()
    if "footprint_radius_km" not in sat_overlay_df.columns:
        sat_overlay_df["footprint_radius_km"] = np.nan
    if "altitude_km" not in sat_overlay_df.columns:
        sat_overlay_df["altitude_km"] = np.nan

    # Provider radii must be positive and finite. Normalize valid numeric text
    # and mark every other value missing so a valid altitude can repair it.
    def normalize_radius(value):
        try:
            radius = float(value)
        except (TypeError, ValueError):
            return float("nan")
        return radius if np.isfinite(radius) and radius > 0 else float("nan")

    sat_overlay_df["footprint_radius_km"] = sat_overlay_df[
        "footprint_radius_km"
    ].map(normalize_radius)

    mask_missing = sat_overlay_df["footprint_radius_km"].isna()
    if mask_missing.any():
        sat_overlay_df.loc[mask_missing, "footprint_radius_km"] = sat_overlay_df.loc[
            mask_missing, "altitude_km"
        ].apply(horizon_radius_km)
    return sat_overlay_df

