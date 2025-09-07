# Script Name: wx_obs.py
# Last Updated (UTC): 2025-09-02
# Update Summary:
# • New: Aggregator to build wx_obs from multiple fetchers
# Description:
# • Given ref points (A/B coords) + time window, calls fetchers and returns unified wx_obs.
# External Data Sources: via sub-fetchers (Open-Meteo, Meteostat; NOAA later)
# Internal Variables:
# • Inputs: list[(lat, lon)], start_utc, end_utc, radius_km, max_stations, include_marine
# Produced DataFrames:
# • wx_obs: time-series of source observations/samples (schema agreed in chat)
# Data Handling Notes:
# • UTC timestamps; SI units; dedupe by (source_id, valid_utc).

from flask_app.setup_imports import *
from .wx_fetch_open_meteo import fetch_open_meteo_obs
from .wx_fetch_meteostat import fetch_meteostat_obs_near
LOG = logging.getLogger(__name__)

from datetime import datetime, timezone, timedelta

import pandas as pd
import numpy as np

def _coerce_utc(ts):
    """Make a pandas.Timestamp UTC-aware safely."""
    import pandas as pd
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        return t.tz_localize("UTC")
    return t.tz_convert("UTC")

def reduce_to_nearest_or_latest(df: pd.DataFrame, *, target_utc, max_age_hours: float = 24) -> pd.DataFrame:
    """
    Return exactly one row per source_id.

    If any rows for a given source_id are within max_age_hours of target_utc (absolute),
    choose the row with the minimum absolute age. Otherwise, choose the latest row for that source_id.
    """
    if df is None or len(df) == 0:
        return df

    df = df.copy()

    # Ensure tz-aware UTC
    df["valid_utc"] = pd.to_datetime(df["valid_utc"], utc=True)
    target = pd.to_datetime(target_utc, utc=True)

    # Age in hours (absolute)
    age = (df["valid_utc"] - target).abs()
    df["_age_hours"] = age.dt.total_seconds() / 3600.0

    def _pick(g: pd.DataFrame) -> pd.Series:
        near = g[g["_age_hours"] <= max_age_hours]
        if len(near) > 0:
            idx = near["_age_hours"].idxmin()
            return g.loc[idx]
        # fall back: latest by valid_utc
        return g.loc[g["valid_utc"].idxmax()]

    out = df.groupby("source_id", group_keys=False).apply(_pick)
    out = out.drop(columns=["_age_hours"], errors="ignore").reset_index(drop=True)
    return out

_WX_OBS_COLS = [
    "source_id","valid_utc","source_type","provider","lat","lon",
    "temp_c","dewpoint_c","rh_pct","wind_ms","wind_dir_deg","wind_gust_ms",
    "pressure_hpa","precip_mmhr","visibility_km","cloud_cover_pct",
    "wave_height_m","wave_period_s","wave_dir_deg",
    "swell_height_m","swell_period_s","swell_dir_deg",
    "wind_wave_height_m","wind_wave_period_s","wind_wave_dir_deg",
    "sst_c","name","owner","deployment_notes"
]

def _empty_wx_obs() -> pd.DataFrame:
    return pd.DataFrame(columns=_WX_OBS_COLS)

def build_wx_obs_for_area(ref_points: list[tuple[float,float]],
                          start_utc, end_utc,
                          radius_km: float = 50.0,
                          max_stations: int = 5,
                          include_marine: bool = True) -> pd.DataFrame:
    if not ref_points:
        return _empty_wx_obs()

    frames = []
    for (lat, lon) in ref_points:
        # Open-Meteo at point
        df_om = fetch_open_meteo_obs(lat, lon, start_utc, end_utc, include_marine=include_marine)
        if not df_om.empty: frames.append(df_om)

        # Meteostat nearby
        df_ms = fetch_meteostat_obs_near(lat, lon, start_utc, end_utc, radius_km=radius_km, max_stations=max_stations)
        if not df_ms.empty: frames.append(df_ms)

    if not frames:
        return _empty_wx_obs()

    df_all = pd.concat(frames, ignore_index=True)
    # Dedupe on (source_id, valid_utc)
    df_all = df_all.sort_values("valid_utc").drop_duplicates(subset=["source_id","valid_utc"], keep="last").reset_index(drop=True)

    # Enforce schema columns and dtypes
    for c in _WX_OBS_COLS:
        if c not in df_all.columns:
            df_all[c] = np.nan

    # Decide the target time:
    # Use alert time if you pass it; otherwise default to "end_utc" (i.e., the alert window end),
    # and if that’s not available, use "now".
    try:
        target_utc = end_utc if end_utc is not None else datetime.now(timezone.utc)
    except NameError:
        target_utc = datetime.now(timezone.utc)

    # Keep only nearest (or latest if nothing near) per source_id
    df_all = reduce_to_nearest_or_latest(df_all, target_utc=target_utc, max_age_hours=24)

    return df_all
