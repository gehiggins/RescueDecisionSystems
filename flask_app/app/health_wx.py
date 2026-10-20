# health_wx.py â€” Weather providers health check (Open-Meteo + Meteostat)
# Last Updated (UTC): 2025-09-11
# Update Summary:
# â€¢ New: one-shot health check for external weather providers.
# â€¢ Prints row counts, time span, missing columns, and sample rows.
#
# Description:
# â€¢ Verifies that calls to Open-Meteo and (optionally) Meteostat return data
#   within a small time window at a given lat/lon, using shared utils.
#
# External Data Sources:
# â€¢ Open-Meteo API (model data)
# â€¢ Meteostat (station data; optional / may not be installed)
#
# Internal Variables:
# â€¢ None
#
# Produced DataFrames:
# â€¢ None (function returns a summary dict for pipeline logging/decisions)
#
# Data Handling Notes:
# â€¢ Uses UTC-normalized windows via utils_time.now_utc().

from app.setup_imports import *  # pandas as pd, numpy as np, logging, etc.

from app.utils_time import now_utc
from app.wx_fetch_open_meteo import fetch_open_meteo_obs
import math

# Meteostat optional: handle gracefully if lib/env not present
try:
    from app.wx_fetch_meteostat import fetch_meteostat_obs_near
    HAS_METEOSTAT = True
except Exception:
    HAS_METEOSTAT = False

LOG = logging.getLogger("health_wx")

REQUIRED_COLS = ["valid_utc", "lat", "lon", "provider", "source_type"]

_DEFAULT_LAT = 47.6062
_DEFAULT_LON = -122.3321
_DEFAULT_HOURS = 6.0
_DEFAULT_RADIUS_KM = 75.0
_DEFAULT_MAX_STATIONS = 5


def _bounded_float(value, *, default, minimum, maximum, name):
    try:
        normalized = (
            float("nan")
            if isinstance(value, (bool, np.bool_))
            else float(value)
        )
    except (TypeError, ValueError, OverflowError):
        normalized = float("nan")
    if not math.isfinite(normalized) or not minimum <= normalized <= maximum:
        LOG.warning("Invalid weather health %s=%r; using default %s", name, value, default)
        return default
    return normalized


def _positive_float(value, *, default, name):
    try:
        normalized = (
            float("nan")
            if isinstance(value, (bool, np.bool_))
            else float(value)
        )
    except (TypeError, ValueError, OverflowError):
        normalized = float("nan")
    if not math.isfinite(normalized) or normalized <= 0:
        LOG.warning("Invalid weather health %s=%r; using default %s", name, value, default)
        return default
    return normalized


def _positive_int(value, *, default, name):
    normalized = _positive_float(value, default=default, name=name)
    if not float(normalized).is_integer():
        LOG.warning("Invalid weather health %s=%r; using default %s", name, value, default)
        return default
    return int(normalized)


def _summarize_df(df: pd.DataFrame) -> dict:
    if df is None or df.empty:
        return {"rows": 0, "min_ts": None, "max_ts": None, "missing_cols": REQUIRED_COLS}
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    ts = (
        pd.to_datetime(df["valid_utc"], utc=True, errors="coerce")
        if "valid_utc" in df.columns
        else pd.Series([], dtype="datetime64[ns, UTC]")
    )
    valid_ts = ts.dropna() if isinstance(ts, pd.Series) else ts
    if not isinstance(valid_ts, pd.Series) or valid_ts.empty:
        min_ts = max_ts = None
    else:
        min_ts = valid_ts.min()
        max_ts = valid_ts.max()
    return {"rows": int(df.shape[0]), "min_ts": min_ts, "max_ts": max_ts, "missing_cols": missing}


def run_wx_health_check(lat: float, lon: float, hours: int = 6,
                        radius_km: float = 75.0, max_stations: int = 5,
                        include_marine: bool = True) -> dict:
    """Return a dict with per-provider row counts, timespan, and missing cols."""
    # Health checks often receive values from environment variables, so
    # normalize them before any timestamp math or provider request occurs.
    lat = _bounded_float(
        lat, default=_DEFAULT_LAT, minimum=-90.0, maximum=90.0, name="lat"
    )
    lon = _bounded_float(
        lon, default=_DEFAULT_LON, minimum=-180.0, maximum=180.0, name="lon"
    )
    hours = _positive_float(hours, default=_DEFAULT_HOURS, name="hours")
    radius_km = _positive_float(
        radius_km, default=_DEFAULT_RADIUS_KM, name="radius_km"
    )
    max_stations = _positive_int(
        max_stations, default=_DEFAULT_MAX_STATIONS, name="max_stations"
    )
    end_utc = now_utc().replace(minute=0, second=0, microsecond=0)
    start_utc = end_utc - pd.Timedelta(hours=hours)

    LOG.info("WX Health window: %s â†’ %s @ (%.4f, %.4f)", start_utc, end_utc, lat, lon)

    # Open-Meteo
    try:
        om = fetch_open_meteo_obs(lat, lon, start_utc, end_utc, include_marine=include_marine)
        om_sum = _summarize_df(om)
    except Exception as e:
        LOG.exception("Open-Meteo fetch failed: %s", e)
        om, om_sum = pd.DataFrame(), {"rows": 0, "min_ts": None, "max_ts": None, "missing_cols": REQUIRED_COLS}

    # Meteostat
    try:
        if HAS_METEOSTAT:
            ms = fetch_meteostat_obs_near(lat, lon, start_utc, end_utc, radius_km=radius_km, max_stations=max_stations)
            ms_sum = _summarize_df(ms)
        else:
            ms, ms_sum = pd.DataFrame(), {"rows": 0, "min_ts": None, "max_ts": None, "missing_cols": REQUIRED_COLS}
    except Exception as e:
        LOG.exception("Meteostat fetch failed: %s", e)
        ms, ms_sum = pd.DataFrame(), {"rows": 0, "min_ts": None, "max_ts": None, "missing_cols": REQUIRED_COLS}

    out = {
        "params": {"lat": lat, "lon": lon, "hours": hours, "radius_km": radius_km, "max_stations": max_stations},
        "open_meteo": om_sum,
        "meteostat": ms_sum,
        "has_meteostat": HAS_METEOSTAT,
    }

    # Log concise summary for pipeline logs
    LOG.info("Open-Meteo: rows=%s span=[%s, %s] missing=%s",
             om_sum["rows"], om_sum["min_ts"], om_sum["max_ts"], om_sum["missing_cols"])
    LOG.info("Meteostat:  rows=%s span=[%s, %s] missing=%s",
             ms_sum["rows"], ms_sum["min_ts"], ms_sum["max_ts"], ms_sum["missing_cols"])

    return out


if __name__ == "__main__":
    # Library imports must not replace application-wide logging settings.
    logging.basicConfig(level=os.environ.get("RDS_LOG_LEVEL", "INFO"))
    # Default: Seattle, 6-hour window
    summary = run_wx_health_check(47.6062, -122.3321, hours=6, radius_km=75, max_stations=5, include_marine=True)
    # Non-zero exit if neither provider returns rows (makes CLI usable in CI)
    if (summary["open_meteo"]["rows"] == 0) and (summary["meteostat"]["rows"] == 0):
        raise SystemExit(2)

