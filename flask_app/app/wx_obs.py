# Script Name: wx_obs.py
# Last Updated (UTC): 2025-09-02
# Update Summary:
# â€¢ New: Aggregator to build wx_obs from multiple fetchers
# Description:
# â€¢ Given ref points (A/B coords) + time window, calls fetchers and returns unified wx_obs.
# External Data Sources: via sub-fetchers (Open-Meteo, Meteostat; NOAA later)
# Internal Variables:
# â€¢ Inputs: list[(lat, lon)], start_utc, end_utc, radius_km, max_stations, include_marine
# Produced DataFrames:
# â€¢ wx_obs: time-series of source observations/samples (schema agreed in chat)
# Data Handling Notes:
# â€¢ UTC timestamps; SI units; dedupe by (source_id, valid_utc).

from app.setup_imports import *
from .wx_fetch_open_meteo import fetch_open_meteo_obs
from .wx_fetch_meteostat import fetch_meteostat_obs_near
from rds_engine.context import classify_zone_context
from rds_engine.policy_wx import make_wx_data_plan
LOG = logging.getLogger(__name__)

from datetime import datetime, timezone, timedelta
import math

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

    missing_columns = {"source_id", "valid_utc"} - set(df.columns)
    if missing_columns:
        raise ValueError(
            "weather observations require columns: "
            + ", ".join(sorted(missing_columns))
        )

    if target_utc is None or not pd.api.types.is_scalar(target_utc):
        raise ValueError("target_utc must be a valid timestamp")
    target = pd.to_datetime(target_utc, utc=True, errors="coerce")
    if pd.isna(target):
        raise ValueError("target_utc must be a valid timestamp")
    if isinstance(max_age_hours, bool):
        raise ValueError("max_age_hours must be a finite non-negative number")
    try:
        max_age_hours = float(max_age_hours)
    except (TypeError, ValueError) as exc:
        raise ValueError("max_age_hours must be a finite non-negative number") from exc
    if not math.isfinite(max_age_hours) or max_age_hours < 0:
        raise ValueError("max_age_hours must be a finite non-negative number")

    # Selection below uses index labels returned by idxmin/idxmax. Normalize
    # them first so duplicate caller indexes cannot expand one selected label
    # back into multiple observations.
    df = df.copy().reset_index(drop=True)

    # Ensure tz-aware UTC
    df["valid_utc"] = df["valid_utc"].map(
        lambda value: pd.to_datetime(value, utc=True, errors="coerce")
    )
    # The reducer is also a public utility, so enforce the same provider-ID
    # contract used by the higher-level aggregator before grouping rows.
    df["source_id"] = df["source_id"].map(_normalize_source_id)
    df = df.loc[
        df["valid_utc"].notna() & df["source_id"].notna()
    ].copy()
    if df.empty:
        return df

    # Age in hours (absolute)
    age = (df["valid_utc"] - target).abs()
    df["_age_hours"] = age.dt.total_seconds() / 3600.0

    selected_indices = []
    for _, group in df.groupby("source_id", dropna=False, sort=False):
        near = group[group["_age_hours"] <= max_age_hours]
        if len(near) > 0:
            selected_indices.append(near["_age_hours"].idxmin())
        else:
            selected_indices.append(group["valid_utc"].idxmax())

    # Selecting original rows keeps the source_id column; groupby.apply with
    # include_groups=False silently removed it from downstream station data.
    out = df.loc[selected_indices].copy()
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

_DEFAULT_RADIUS_KM = 50.0
_DEFAULT_MAX_STATIONS = 5


def _normalize_search_parameters(radius_km, max_stations):
    """Return safe provider-search values while preserving policy-driven ``None``."""
    normalized_radius = radius_km
    if radius_km is not None:
        try:
            normalized_radius = (
                float("nan") if isinstance(radius_km, bool) else float(radius_km)
            )
        except (TypeError, ValueError, OverflowError):
            normalized_radius = float("nan")
        if not math.isfinite(normalized_radius) or normalized_radius <= 0:
            LOG.warning(
                "Invalid weather radius_km=%r; using default %.1f",
                radius_km,
                _DEFAULT_RADIUS_KM,
            )
            normalized_radius = _DEFAULT_RADIUS_KM

    normalized_max_stations = max_stations
    if max_stations is not None:
        try:
            numeric_max_stations = (
                float("nan")
                if isinstance(max_stations, bool)
                else float(max_stations)
            )
        except (TypeError, ValueError, OverflowError):
            numeric_max_stations = float("nan")
        if (
            not math.isfinite(numeric_max_stations)
            or numeric_max_stations < 1
            or not numeric_max_stations.is_integer()
        ):
            LOG.warning(
                "Invalid weather max_stations=%r; using default %s",
                max_stations,
                _DEFAULT_MAX_STATIONS,
            )
            normalized_max_stations = _DEFAULT_MAX_STATIONS
        else:
            normalized_max_stations = int(numeric_max_stations)

    return normalized_radius, normalized_max_stations

def _empty_wx_obs() -> pd.DataFrame:
    return pd.DataFrame(columns=_WX_OBS_COLS)


def _normalize_source_id(value):
    """Return a stable provider identifier, or ``None`` when malformed."""
    if not pd.api.types.is_scalar(value) or isinstance(value, bool):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        return None
    if isinstance(value, (float, np.number)):
        try:
            if not math.isfinite(float(value)):
                return None
        except (TypeError, ValueError):
            return None
    normalized = str(value).strip()
    return normalized or None

def build_wx_obs_for_area(ref_points: list[tuple[float,float]],
                          start_utc, end_utc,
                          radius_km: float = 50.0,
                          max_stations: int = 5,
                          include_marine: bool = True) -> pd.DataFrame:
    if not ref_points:
        return _empty_wx_obs()

    radius_km, max_stations = _normalize_search_parameters(
        radius_km, max_stations
    )

    valid_ref_points = []
    rejected_points = 0
    for ref_point in ref_points:
        try:
            if isinstance(ref_point, dict):
                raw_lat = ref_point.get("lat")
                raw_lon = ref_point.get("lon")
            else:
                raw_lat, raw_lon = ref_point
            if isinstance(raw_lat, (bool, np.bool_)) or isinstance(
                raw_lon, (bool, np.bool_)
            ):
                raise ValueError("boolean coordinates are invalid")
            lat = float(raw_lat)
            lon = float(raw_lon)
        except (TypeError, ValueError):
            rejected_points += 1
            continue
        if (
            not math.isfinite(lat)
            or not math.isfinite(lon)
            or not -90.0 <= lat <= 90.0
            or not -180.0 <= lon <= 180.0
        ):
            rejected_points += 1
            continue
        if isinstance(ref_point, dict):
            normalized = dict(ref_point)
            normalized.update({"lat": lat, "lon": lon})
            valid_ref_points.append(normalized)
        else:
            valid_ref_points.append((lat, lon))
    if rejected_points:
        LOG.warning(
            "Discarded %s invalid weather provider reference point(s)",
            rejected_points,
        )
    if not valid_ref_points:
        return _empty_wx_obs()

    frames = []
    for r in valid_ref_points:
        # Accept tuple or dict for ref_points
        if isinstance(r, dict):
            lat, lon = r.get("lat"), r.get("lon")
        else:
            lat, lon = r

        # Centralized policy decision (per point)
        # error_radius_km/coast_distance_km: use row fields if available, else None
        err_km = r.get("error_radius_km") if isinstance(r, dict) else None
        coast_km = r.get("coast_distance_km") if isinstance(r, dict) else None
        _ctx = classify_zone_context(lat=lat, lon=lon, error_radius_km=err_km, coast_distance_km=coast_km)
        _plan = make_wx_data_plan(_ctx, mode=os.getenv("RDS_WX_POLICY_MODE", "auto"))

        # Respect plan for provider params
        # ``None`` delegates to policy; an explicit caller choice must survive
        # this per-point planning pass.
        _include_marine = (
            _plan.include_marine if include_marine is None else include_marine
        )
        _radius_km = radius_km if radius_km is not None else _plan.spatial_radius_km
        _max_stations = max_stations if max_stations is not None else _plan.station_limit

        LOG.info("wx policy per-point: include_marine=%s radius_km=%.1f stations=%s rationale=%s",
                 _include_marine, _radius_km, _max_stations, getattr(_plan, "rationale", None))

        # Keep providers isolated: one unexpected provider failure must not
        # discard observations already available from the other provider.
        try:
            df_om = fetch_open_meteo_obs(
                lat, lon, start_utc, end_utc, include_marine=_include_marine
            )
            if isinstance(df_om, pd.DataFrame) and not df_om.empty:
                frames.append(df_om)
        except Exception:
            LOG.exception("Open-Meteo weather retrieval failed")

        try:
            df_ms = fetch_meteostat_obs_near(
                lat,
                lon,
                start_utc,
                end_utc,
                radius_km=_radius_km,
                max_stations=_max_stations,
            )
            if isinstance(df_ms, pd.DataFrame) and not df_ms.empty:
                frames.append(df_ms)
        except Exception:
            LOG.exception("Meteostat weather retrieval failed")

    if not frames:
        return _empty_wx_obs()

    df_all = pd.concat(frames, ignore_index=True)
    # Enforce the shared schema before validating provider rows. A malformed
    # frame must not make valid observations from another provider unusable.
    for c in _WX_OBS_COLS:
        if c not in df_all.columns:
            df_all[c] = np.nan

    df_all["valid_utc"] = df_all["valid_utc"].map(
        lambda value: pd.to_datetime(value, utc=True, errors="coerce")
    )
    # Provider identifiers become grouping and deduplication keys below.
    # Normalize scalars first so malformed list/dict values cannot crash the
    # entire weather result after another provider already returned good data.
    df_all["source_id"] = df_all["source_id"].map(_normalize_source_id)
    usable = (
        df_all["valid_utc"].notna()
        & df_all["source_id"].notna()
    )
    rejected_rows = int((~usable).sum())
    if rejected_rows:
        LOG.warning("Discarded %s malformed weather observation row(s)", rejected_rows)
    df_all = df_all.loc[usable].copy()
    if df_all.empty:
        return _empty_wx_obs()

    # Dedupe on the validated identity and observation timestamp.
    df_all = (
        df_all.sort_values("valid_utc")
        .drop_duplicates(subset=["source_id", "valid_utc"], keep="last")
        .reset_index(drop=True)
    )

    # Decide the target time:
    # Use alert time if you pass it; otherwise default to "end_utc" (i.e., the alert window end),
    # and if thatâ€™s not available, use "now".
    try:
        target_utc = end_utc if end_utc is not None else datetime.now(timezone.utc)
    except NameError:
        target_utc = datetime.now(timezone.utc)

    # Keep only nearest (or latest if nothing near) per source_id
    df_all = reduce_to_nearest_or_latest(df_all, target_utc=target_utc, max_age_hours=24)

    return df_all

