# pipeline_health_entrypoint.py â€” Invoke the weather health check from CLI/CI
# Last Updated (UTC): 2025-09-11
# Update Summary:
# â€¢ New: simple entrypoint that calls run_wx_health_check with defaults or ENV.
#
# Description:
# â€¢ Convenience runner so pipeline/CI can call a single module and inspect exit code.
#
# External Data Sources: Open-Meteo, Meteostat (optional)
# Internal Variables: none
# Produced DataFrames: none

from app.setup_imports import *  # logging, pandas as pd, etc.
from app.health_wx import run_wx_health_check

def _get_env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except Exception:
        return default

if __name__ == "__main__":
    # Defaults: Seattle grid
    lat = _get_env_float("RDS_HEALTH_LAT", 47.6062)
    lon = _get_env_float("RDS_HEALTH_LON", -122.3321)
    hours = int(os.environ.get("RDS_HEALTH_HOURS", "6"))
    radius_km = float(os.environ.get("RDS_HEALTH_RADIUS_KM", "75"))
    max_stations = int(os.environ.get("RDS_HEALTH_MAX_STATIONS", "5"))
    include_marine = os.environ.get("RDS_HEALTH_INCLUDE_MARINE", "1") not in ("0", "false", "False")

    summary = run_wx_health_check(lat, lon, hours=hours, radius_km=radius_km,
                                  max_stations=max_stations, include_marine=include_marine)

    # Exit non-zero if both providers returned zero rows (useful for CI/pipeline gating)
    if (summary["open_meteo"]["rows"] == 0) and (summary["meteostat"]["rows"] == 0):
        raise SystemExit(2)

