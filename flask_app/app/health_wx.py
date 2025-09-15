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

# Meteostat optional: handle gracefully if lib/env not present
try:
    from app.wx_fetch_meteostat import fetch_meteostat_obs_near
    HAS_METEOSTAT = True
except Exception:
    HAS_METEOSTAT = False

LOG = logging.getLogger("health_wx")
logging.basicConfig(level=os.environ.get("RDS_LOG_LEVEL", "INFO"))

REQUIRED_COLS = ["valid_utc", "lat", "lon", "provider", "source_type"]


def _summarize_df(df: pd.DataFrame) -> dict:
    if df is None or df.empty:
        return {"rows": 0, "min_ts": None, "max_ts": None, "missing_cols": REQUIRED_COLS}
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    ts = pd.to_datetime(df["valid_utc"], errors="coerce") if "valid_utc" in df.columns else pd.Series([], dtype="datetime64[ns, UTC]")
    if not isinstance(ts, pd.Series) or ts.empty:
        min_ts = max_ts = None
    else:
        min_ts = ts.min()
        max_ts = ts.max()
    return {"rows": int(df.shape[0]), "min_ts": min_ts, "max_ts": max_ts, "missing_cols": missing}


def run_wx_health_check(lat: float, lon: float, hours: int = 6,
                        radius_km: float = 75.0, max_stations: int = 5,
                        include_marine: bool = True) -> dict:
    """Return a dict with per-provider row counts, timespan, and missing cols."""
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
    # Default: Seattle, 6-hour window
    summary = run_wx_health_check(47.6062, -122.3321, hours=6, radius_km=75, max_stations=5, include_marine=True)
    # Non-zero exit if neither provider returns rows (makes CLI usable in CI)
    if (summary["open_meteo"]["rows"] == 0) and (summary["meteostat"]["rows"] == 0):
        raise SystemExit(2)

