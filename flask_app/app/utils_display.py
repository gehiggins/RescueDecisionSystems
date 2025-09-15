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

def derive_local_tz(lat: float, lon: float, op_tz_env: Optional[str] = None) -> str:
    """
    Derive IANA local timezone string from lat/lon, or use op_tz_env if provided.
    Falls back to UTC if lookup fails or timezonefinder is not installed.
    """
    if op_tz_env and str(op_tz_env).strip():
        return str(op_tz_env).strip()
    try:
        from timezonefinder import TimezoneFinder
        tf = TimezoneFinder()
        tz = tf.timezone_at(lat=lat, lng=lon)
        if tz:
            return tz
        else:
            LOG.warning(f"derive_local_tz: No timezone found for ({lat},{lon}); falling back to UTC.")
            return "UTC"
    except ImportError:
        LOG.warning("derive_local_tz: timezonefinder not installed; falling back to UTC.")
        return "UTC"
    except Exception as e:
        LOG.warning(f"derive_local_tz: Failed to resolve timezone for ({lat},{lon}): {e}; falling back to UTC.")
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
    temp_C: Optional[float] = None
) -> Dict[str, str]:
    """
    Format US display strings for wave height, wind, and temperature.
    Only includes keys for provided (non-None, non-NaN) inputs.
    """
    out = {}
    if wave_height_m is not None and not (isinstance(wave_height_m, float) and np.isnan(wave_height_m)):
        feet = m_to_ft(wave_height_m)
        if feet is not None:
            out["wave_height_display"] = f"{feet:.1f} ft"
    if wind_ms is not None and not (isinstance(wind_ms, float) and np.isnan(wind_ms)):
        knots = ms_to_kt(wind_ms)
        if knots is not None:
            out["wind_display"] = f"{knots:.0f} kt"
    if temp_C is not None and not (isinstance(temp_C, float) and np.isnan(temp_C)):
        F = c_to_f(temp_C)
        if F is not None:
            out["temp_display"] = f"{F:.0f} °F / {temp_C:.1f} °C"
    return out

def is_maritime(lat: float, lon: float, shore_nm: float = 5.0) -> bool:
    """
    Stub: Returns False. Intended to compute proximity to coastline.
    GIS will pass a precomputed flag or inject a coastline index in future.
    """
    LOG.debug("is_maritime: coastline proximity check not wired; returning False")
    return False