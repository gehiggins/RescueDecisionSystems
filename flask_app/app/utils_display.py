# ============================== RDS STANDARD HEADER ==============================
# Script Name: utils_display.py
# Last Updated (UTC): 2025-09-15
# Update Summary:
#   - Initial implementation of display and conversion helpers for weather and GIS.
# Description:
#   - Provides pure helpers for timezone resolution, dual time formatting, US display conversions,
#     and maritime proximity checks (stub).
# External Data Sources:
#   - Optional: timezonefinder (if installed) for timezone lookup.
# Internal Variables:
#   - None (stateless utility functions).
# Produced DataFrames:
#   - None (all functions are pure helpers).
# Data Handling Notes:
#   - No file I/O, no network calls, all conversions are local and stateless.

from app.setup_imports import *
LOG = logging.getLogger(__name__)

from typing import Optional, Union, Tuple, Dict
from datetime import datetime

def _normalize_utc_like(tzname: str | None) -> str:
    if not tzname:
        return "UTC"
    return "UTC" if tzname in {"UTC", "Etc/UTC", "Etc/GMT", "GMT"} else tzname

def derive_local_tz(lat: float, lon: float, op_tz_env: str | None = None) -> str:
    """
    Derive IANA local timezone string from lat/lon, or use op_tz_env if provided.
    Falls back to UTC if lookup fails or timezonefinder is not installed.
    """
    # if an env override is given (non-empty), honor it
    if op_tz_env:
        return op_tz_env

    try:
        from timezonefinder import TimezoneFinder
        tf = TimezoneFinder()
        tz = tf.timezone_at(lat=lat, lng=lon) or tf.closest_timezone_at(lat=lat, lng=lon)
        return _normalize_utc_like(tz)
    except Exception:
        # hard fallback
        return "UTC"

def to_dual_time(ts_utc: Union[str, datetime], local_tz: str) -> Tuple[str, str]:
    """
    Convert a UTC timestamp to both UTC and local time ISO strings.
    Ensures input is tz-aware UTC, then converts to local_tz using zoneinfo (preferred) or dateutil (fallback).
    """
    ts = pd.to_datetime(ts_utc, utc=True)
    ts_utc_iso = ts.isoformat()
    try:
        try:
            from zoneinfo import ZoneInfo
            local_zone = ZoneInfo(local_tz)
            ts_local = ts.tz_convert(local_zone)
        except ImportError:
            from dateutil import tz
            local_zone = tz.gettz(local_tz)
            ts_local = ts.tz_convert(local_zone)
        ts_local_iso = ts_local.isoformat()
    except Exception as e:
        LOG.warning(f"to_dual_time: Failed to convert to local tz '{local_tz}': {e}; using UTC for both.")
        ts_local_iso = ts_utc_iso
    return ts_utc_iso, ts_local_iso

def m_to_ft(x: Optional[float]) -> Optional[float]:
    """
    Convert meters to feet. Returns None if input is None or NaN.
    """
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    return float(x) * 3.28084

def ms_to_kt(x: Optional[float]) -> Optional[float]:
    """
    Convert meters/second to knots. Returns None if input is None or NaN.
    """
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    return float(x) * 1.94384

def c_to_f(x: Optional[float]) -> Optional[float]:
    """
    Convert Celsius to Fahrenheit. Returns None if input is None or NaN.
    """
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    return float(x) * 9.0 / 5.0 + 32.0

def format_us_display(
    wave_height_m: Optional[float] = None,
    wind_ms: Optional[float] = None,
    temp_C: Optional[float] = None,
    temp_c: Optional[float] = None
) -> dict:
    """
    Format US display strings for wave height, wind, and temperature.
    Accepts either temp_c (preferred) or temp_C (legacy, for backward compatibility).
    Only includes keys for provided (non-None, non-NaN) inputs.
    """
    # Prefer lower-case temp_c; fall back to legacy temp_C for backward compat
    if temp_c is None and temp_C is not None:
        temp_c = temp_C

    def _is_num(x):
        try:
            return x is not None and not (isinstance(x, float) and (x != x))
        except Exception:
            return False

    feet = m_to_ft(wave_height_m) if wave_height_m is not None else None
    knots = ms_to_kt(wind_ms) if wind_ms is not None else None
    F = c_to_f(temp_c) if temp_c is not None else None

    has_wave = _is_num(feet)
    has_wind = _is_num(knots)
    has_tc  = _is_num(temp_c)
    has_tf  = _is_num(F)

    out = {}
    out["wave_height_display"] = f"{feet:.1f} ft" if has_wave else "None"
    out["wind_display"]        = f"{knots:.0f} kt" if has_wind else "None"
    if   has_tf and has_tc: out["temp_display"] = f"{F:.0f} °F / {temp_c:.1f} °C"
    elif has_tf:            out["temp_display"] = f"{F:.0f} °F"
    elif has_tc:            out["temp_display"] = f"{temp_c:.1f} °C"
    else:                   out["temp_display"] = "None"
    return out

def is_maritime(lat: float, lon: float, shore_nm: float = 5.0) -> bool:
    """
    Stub: Returns False. Intended to compute proximity to coastline.
    GIS will pass a precomputed flag or inject a coastline index in future.
    """
    LOG.debug("is_maritime: coastline proximity check not wired; returning False")
    return False