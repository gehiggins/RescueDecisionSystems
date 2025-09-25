# ============================== RDS STANDARD HEADER ==============================
# Script Name: sat_pipeline.py
# Last Updated (UTC): 2025-09-04
# Update Summary:
# - New orchestrator for satellite overlays. MVP returns sat_overlay_df for reporting sat.
# - Uses sat_fetch_tle for reference/resolve; sat_compute_footprint for footprint radius.
# - TLE path enabled by default (use_tle=True).
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
import logging
import math
import os
from pathlib import Path

from app.sat_fetch_tle import load_sat_reference, resolve_reporting_sat, load_tle_snapshot
from app.sat_compute_groundtrack import compute_subpoint_at, compute_short_track, find_next_pass_marker
from app.sat_compute_footprint import annotate_footprint_radius
import pandas as pd, numpy as np


# ----------------------- Public API -----------------------

def build_sat_overlay_df(
    alert_df: pd.DataFrame,
    types: Tuple[str, ...] = ("LEO",),
    use_tle: bool = True,
    scope: str = "reporting",
    fallback_to_nearest: bool = False
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
    COLS = [
        "sat_name","norad_id","at_time_utc","lat_dd","lon_dd","alt_km",
        "footprint_radius_km","tle_epoch_utc","tle_age_hours","source",
        "track_coords","track_start_utc","track_end_utc","next_pass_marker",
        "_variant","distance_km"
    ]
    if alert_df.empty:
        return pd.DataFrame(columns=COLS)

    sat_ref = load_sat_reference()
    if types is not None:
        TYPE_MAP = {"LEOSAR":"LEO","GEOSAR":"GEO","MEOSAR":"MEO","LEO":"LEO","GEO":"GEO","MEO":"MEO"}
        want = {TYPE_MAP.get(str(t).upper(), str(t).upper()) for t in types}
        sat_ref["type_norm"] = sat_ref["type"].str.upper().map(TYPE_MAP)
        sat_ref = sat_ref[sat_ref["type_norm"].isin(want)]

    row = resolve_reporting_sat(alert_df)

    # Prefer a NORAD provided in the alert row (test/ingest convenience)
    if pd.notna(alert_df.iloc[0].get("norad_id", np.nan)):
        row = (row or {})
        row["norad_id"] = int(alert_df.iloc[0]["norad_id"])
        row.setdefault("common_name", str(alert_df.iloc[0].get("sat_name") or alert_df.iloc[0].get("reporting_sat") or "SAT"))

    if row is None:
        row = {}  # defer return so fallback/overrides can run

    at_time_utc = pd.to_datetime(alert_df.iloc[0]["alert_time_utc"], utc=True)
    name = str(row.get("common_name") or row.get("designator"))
    norad_id = int(row["norad_id"]) if pd.notna(row.get("norad_id")) else np.nan
    source = "reference"
    lat = lon = alt_km = np.nan
    r_km = np.nan
    tle_epoch = pd.NaT
    tle_age_hours = np.nan
    track_coords = None
    next_pass_marker = None

    # TLE path
    tle_df = None
    if use_tle and pd.notna(row.get("norad_id")):
        tle_df = load_tle_snapshot([str(int(row["norad_id"]))]); source = "celestrak:catnr"
    elif use_tle:
        d0 = str(row.get("designator",""))[:1].upper()
        group = "galileo" if d0=="E" else ("glonass" if d0=="R" else ("beidou" if d0 in ("B","C") else None))
        if group: tle_df = load_tle_snapshot([group]); source = "celestrak:group"

    if use_tle and tle_df is not None and not tle_df.empty:
        t = tle_df.iloc[0]
        tle1, tle2 = t["tle_line1"], t["tle_line2"]
        tle_epoch = pd.to_datetime(t["epoch_utc"], utc=True, errors="coerce")
        lat, lon, alt_km = compute_subpoint_at(tle1, tle2, at_time_utc.to_pydatetime())
        if pd.notna(tle_epoch):
            tle_age_hours = float(abs((at_time_utc - tle_epoch).total_seconds())/3600.0)
        # forward track (+60 min)
        try:
            track_coords, track_start_utc, track_end_utc = compute_short_track(
                tle1, tle2, at_time_utc.to_pydatetime(), forward_min=30, step_s=60
            )
        except Exception:
            track_coords, track_start_utc, track_end_utc = None, None, None
        # next-pass marker (if alert lat/lon provided)
        if {"alert_lat_dd","alert_lon_dd"}.issubset(alert_df.columns):
            la, lo = alert_df.iloc[0].get("alert_lat_dd"), alert_df.iloc[0].get("alert_lon_dd")
            if pd.notna(la) and pd.notna(lo):
                try:
                    next_pass_marker = find_next_pass_marker(tle1, tle2, (float(la), float(lo)), at_time_utc.to_pydatetime(), max_hours=12)
                except Exception:
                    next_pass_marker = None
        # prefer provider name/CATNR when present
        name = str(t.get("name") or name)
        if pd.notna(t.get("norad_id")): norad_id = int(t["norad_id"])

    # assemble
    overlay = pd.DataFrame([{
        "sat_name": name, "norad_id": norad_id, "at_time_utc": at_time_utc,
        "lat_dd": lat, "lon_dd": lon, "alt_km": alt_km,
        "footprint_radius_km": r_km,
        "tle_epoch_utc": tle_epoch, "tle_age_hours": tle_age_hours,
        "source": source,
        "track_coords": track_coords,
        "track_start_utc": track_start_utc,
        "track_end_utc": track_end_utc,
        "next_pass_marker": next_pass_marker,
    }])
    # compute radius from alt_km -> annotate_footprint_radius expects 'altitude_km'
    overlay["altitude_km"] = overlay["alt_km"]
    overlay = annotate_footprint_radius(overlay).drop(columns=["altitude_km"])
    overlay = overlay.dropna(subset=["lat_dd","lon_dd","footprint_radius_km"])

    # If nothing to show and fallback enabled, try nearest LEO to alert location
    if overlay.dropna(subset=["lat_dd","lon_dd","footprint_radius_km"]).empty and fallback_to_nearest:
        if {"alert_lat_dd","alert_lon_dd"}.issubset(alert_df.columns):
            la, lo = alert_df.iloc[0].get("alert_lat_dd"), alert_df.iloc[0].get("alert_lon_dd")
            if pd.notna(la) and pd.notna(lo):
                nearest = _fallback_nearest_leo(float(la), float(lo), at_time_utc, max_candidates=16, top_n=3)
                if not nearest.empty:
                    overlay = pd.concat([overlay, nearest], ignore_index=True)

    import os, logging
    from pathlib import Path
    LOG = logging.getLogger(__name__)

    if os.getenv("RDS_SAT_DEBUG_DUMP") == "1" and not overlay.empty:
        try:
            outp = Path("data/overlays") / f"{alert_df.iloc[0]['alert_id']}_sat_overlay.parquet"
            outp.parent.mkdir(parents=True, exist_ok=True)
            overlay.to_parquet(outp, index=False)
        except Exception as e:
            LOG.warning(f"[SAT] parquet dump skipped: {e}")

    return overlay.reindex(columns=COLS)


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


def _empty_tracks_df() -> pd.DataFrame:
    return pd.DataFrame(columns=["sat_id","when_utc","lat_dd","lon_dd","segment"])

def _haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dlmb/2)**2
    return 2*r*math.asin(math.sqrt(a))

def _fallback_nearest_leo(alert_lat, alert_lon, at_time_utc, max_candidates=12, top_n=3):
    ref = load_sat_reference()
    cand = ref[ref["type"].str.upper().isin(["LEO","LEOSAR"])]
    cand = cand[pd.notna(cand["norad_id"])].copy()
    if cand.empty: 
        return pd.DataFrame()
    catnrs = [str(int(x)) for x in cand["norad_id"].head(max_candidates).tolist()]
    tles = load_tle_snapshot(catnrs)  # returns many rows
    rows = []
    for _, t in tles.iterrows():
        try:
            lat, lon, alt_km = compute_subpoint_at(t["tle_line1"], t["tle_line2"], at_time_utc.to_pydatetime())
            dist_km = _haversine_km(alert_lat, alert_lon, lat, lon)
            rows.append({
                "sat_name": t.get("name"),
                "norad_id": t.get("norad_id"),
                "at_time_utc": at_time_utc,
                "lat_dd": lat, "lon_dd": lon, "alt_km": alt_km,
                "footprint_radius_km": np.nan,  # filled later
                "tle_epoch_utc": pd.to_datetime(t.get("epoch_utc"), utc=True, errors="coerce"),
                "tle_age_hours": np.nan,
                "source": "fallback:nearest-LEO",
                "track_coords": None,
                "track_start_utc": None,
                "track_end_utc": None,
                "next_pass_marker": None,
                "_variant": "suggested",
                "distance_km": dist_km,
            })
        except Exception:
            continue
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).sort_values("distance_km").head(top_n)
    # compute footprint from alt
    df["altitude_km"] = df["alt_km"]
    out = annotate_footprint_radius(df).drop(columns=["altitude_km"])
    # compute tle_age_hours where epoch exists
    mask = pd.notna(out["tle_epoch_utc"])
    out.loc[mask, "tle_age_hours"] = (out.loc[mask, "at_time_utc"] - out.loc[mask, "tle_epoch_utc"]).dt.total_seconds()/3600.0
    return out

