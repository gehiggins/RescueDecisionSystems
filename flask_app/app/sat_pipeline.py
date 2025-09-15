# ============================== RDS STANDARD HEADER ==============================
# Script Name: sat_pipeline.py
# Last Updated (UTC): 2025-09-04
# Update Summary:
# - New orchestrator for satellite overlays. MVP returns sat_overlay_df for reporting sat.
# - Uses sat_fetch_tle for reference/resolve; sat_compute_footprint for footprint radius.
# - Optional TLE hooks present but disabled by default (use_tle=False).
# Description:
# - Purpose: One-shot builder for satellite overlays per alert (no background jobs).
#   Produces operator-facing DataFrames:
#     1) sat_overlay_df (authoritative per-satellite overlay rows)
#     2) sat_tracks_df (optional; empty in MVP)
# - Primary Inputs:
#   * alert_df (authoritative alert data incl. SAT hint and A/B positions)
#   * SARSAT satellite reference CSV via sat_fetch_tle.load_sat_reference()
# - Primary Outputs:
#   * sat_overlay_df columns:
#       ['sat_id','name','type','owner','constellation','altitude_km',
#        'lat_dd','lon_dd','footprint_radius_km','snapshot_utc','visible_for']
#   * sat_tracks_df columns (not populated in MVP):
#       ['sat_id','when_utc','lat_dd','lon_dd','segment']
# - External Data Sources:
#   * Local CSVs (reference; optional designator map). No network I/O in this module.
# - Internal Variables:
#   * snapshot_utc (datetime UTC at start of run)
#   * scope: 'reporting' (MVP) | 'visible' | 'incoming' (future)
#   * types: ('LEO',) by default; can include 'MEO','GEO' later
# - Produced DataFrames:
#   * sat_overlay_df (authoritative), sat_tracks_df (optional)
# - Data Handling Notes:
#   * DataFrame-first; NaN for missing; no exceptions for empty results (warn and return empty).
#   * Distances in km internally; map handles display conversions.
# ===============================================================================

from app.setup_imports import *
from datetime import datetime, timezone
from typing import Optional, Tuple

import pandas as pd
import numpy as np

# Project-wide rule: internal imports must use `from app.` prefix
from app import __init__ as app_root  # noqa: F401
from app.sat_fetch_tle import (
    load_sat_reference,
    load_designator_map,
    resolve_reporting_sat,
    load_tle_snapshot,  # stubbed for later use
)
from app.sat_compute_footprint import annotate_footprint_radius


# ----------------------- Public API -----------------------

def build_sat_overlay_df(
    alert_df: pd.DataFrame,
    scope: str = "reporting",
    types: Tuple[str, ...] = ("LEO",),
    catalog_manifest_id: Optional[str] = None,
    use_tle: bool = False,
) -> pd.DataFrame:
    """
    Orchestrate the one-shot build of sat_overlay_df for the current alert.

    MVP behavior:
      - scope='reporting': return the single reporting satellite (if resolvable),
        or fallback to first active LEO in the reference.
      - use_tle=False: lat/lon left NaN; footprint uses benchmark radius from reference.

    Future:
      - use_tle=True -> compute subpoints (and tracks) from TLEs.
      - scope='visible' -> filter to sats that can see A/B now (requires subpoints).
      - scope='incoming' -> sats that will see A/B within X minutes.

    :param alert_df: authoritative alert DataFrame for the current case.
    :param scope: 'reporting' (MVP) | 'visible' | 'incoming'
    :param types: satellite classes to include (('LEO',), ('LEO','MEO'), etc.)
    :param catalog_manifest_id: optional manifest id for reference CSV path resolution.
    :param use_tle: if True (future), will compute subpoints and tracks (not in MVP).
    :return: sat_overlay_df (authoritative overlay rows)
    """
    if alert_df is None or len(alert_df) == 0:
        logging.warning("[sat_pipeline] alert_df empty; returning empty sat_overlay_df.")
        return _empty_overlay_df()

    snapshot_utc = datetime.now(timezone.utc).replace(tzinfo=timezone.utc)

    # Load references and resolve the reporting satellite (or fallback).
    sat_ref = load_sat_reference(manifest_id=catalog_manifest_id)
    if len(sat_ref) == 0:
        logging.warning("[sat_pipeline] sat_reference_df empty; returning empty sat_overlay_df.")
        return _empty_overlay_df()

    sat_ref = sat_ref[sat_ref["type"].isin([t.upper() for t in types])]

    design_map = load_designator_map()  # optional ({} if missing)
    row = resolve_reporting_sat(alert_df, sat_ref, design_map)

    if row is None:
        # Fallback: first active LEO (or first active in filtered types)
        fallback = sat_ref.query("is_active == True")
        if len(fallback) == 0:
            logging.warning("[sat_pipeline] No active rows in sat_reference_df; returning empty.")
            return _empty_overlay_df()
        row = fallback.iloc[0]
        logging.warning(
            "[sat_pipeline] Could not resolve reporting satellite from alert_df; "
            f"fallback to '{row.get('name','<unknown>')}'."
        )

    # Assemble sat_overlay_df (single row, MVP: lat/lon NaN)
    overlay = pd.DataFrame([{
        "sat_id": str(row["sat_id"]),
        "name": str(row["name"]),
        "type": str(row["type"]),
        "owner": str(row["owner"]),
        "constellation": str(row["constellation"]),
        "altitude_km": _to_float(row["altitude_km"]),
        "lat_dd": np.nan,   # subpoint will be filled when use_tle=True (future)
        "lon_dd": np.nan,
        "footprint_radius_km": _to_float(row["footprint_radius_km"]),  # may be NaN, annotated below
        "snapshot_utc": snapshot_utc,
        "visible_for": np.nan,  # annotated later when visibility logic enabled
    }])

    # Ensure footprint radius present (benchmark from altitude if missing)
    overlay = annotate_footprint_radius(overlay)

    logging.info(
        "[sat_pipeline] âœ… sat_overlay_df built (MVP). "
        f"name={overlay.loc[0,'name']} type={overlay.loc[0,'type']} "
        f"radius_km={overlay.loc[0,'footprint_radius_km']}"
    )
    return overlay


def build_sat_tracks_df(
    overlay_df: pd.DataFrame,
    minutes_past: int = 10,
    minutes_future: int = 10,
    step_s: int = 60,
    use_tle: bool = False,
) -> pd.DataFrame:
    """
    Optional. Build dashed/dotted ground tracks for satellites in overlay_df.
    MVP: returns empty until use_tle=True path is implemented.

    :return: sat_tracks_df with columns ['sat_id','when_utc','lat_dd','lon_dd','segment']
    """
    if not use_tle:
        logging.info("[sat_pipeline] Tracks disabled in MVP (use_tle=False). Returning empty DF.")
        return _empty_tracks_df()

    # Future: call sat_compute_groundtrack.compute_tracks(...)
    return _empty_tracks_df()


# Backward-compat alias for anything already wired to prior naming:
build_satellites_df = build_sat_overlay_df


# ----------------------- Helpers -----------------------

def _to_float(x) -> float:
    try:
        return float(x)
    except Exception:
        return float("nan")


def _empty_overlay_df() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "sat_id","name","type","owner","constellation","altitude_km",
        "lat_dd","lon_dd","footprint_radius_km","snapshot_utc","visible_for"
    ])


def _empty_tracks_df() -> pd.DataFrame:
    return pd.DataFrame(columns=["sat_id","when_utc","lat_dd","lon_dd","segment"])

