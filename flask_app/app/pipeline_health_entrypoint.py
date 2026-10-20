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


def _health_parameters_from_env():
    """Read raw health settings; the health check performs safe normalization."""
    return {
        "lat": os.environ.get("RDS_HEALTH_LAT", "47.6062"),
        "lon": os.environ.get("RDS_HEALTH_LON", "-122.3321"),
        "hours": os.environ.get("RDS_HEALTH_HOURS", "6"),
        "radius_km": os.environ.get("RDS_HEALTH_RADIUS_KM", "75"),
        "max_stations": os.environ.get("RDS_HEALTH_MAX_STATIONS", "5"),
        "include_marine": os.environ.get("RDS_HEALTH_INCLUDE_MARINE", "1")
        .strip()
        .lower()
        not in ("0", "false", "no"),
    }

if __name__ == "__main__":
    summary = run_wx_health_check(**_health_parameters_from_env())

    # Exit non-zero if both providers returned zero rows (useful for CI/pipeline gating)
    if (summary["open_meteo"]["rows"] == 0) and (summary["meteostat"]["rows"] == 0):
        raise SystemExit(2)

