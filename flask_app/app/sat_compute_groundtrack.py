# ============================== RDS STANDARD HEADER ==============================
# Script Name: sat_compute_groundtrack.py
# Last Updated (UTC): 2025-09-04
# Update Summary:
# - New module for subpoint and ground-track computation (TLE-driven). MVP stubs only.
# Description:
# - Purpose: Given TLEs and a timestamp, compute sub-satellite points "now" and short past/future tracks.
# - Primary Inputs:
#   * tle_df with columns ['sat_id'|'name','tle_line1','tle_line2','epoch_utc']
#   * when_utc (snapshot time)
# - Primary Outputs:
#   * subpoints_df: ['sat_id','lat_dd','lon_dd']
#   * sat_tracks_df: ['sat_id','when_utc','lat_dd','lon_dd','segment'] where segment âˆˆ {'past','future'}
# - External Data Sources:
#   * None. Uses provided TLEs.
# - Data Handling Notes:
#   * MVP returns empty stubs. Future will use SGP4/Skyfield.
# ===============================================================================

from app.setup_imports import *
import logging
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Dict
import numpy as np

try:
    from skyfield.api import EarthSatellite, wgs84, load
except ImportError:
    raise ImportError("skyfield is required for SGP4 propagation")

# Project-wide rule: internal imports must use `from app.` prefix
from app import __init__ as app_root  # noqa: F401

def _make_sat(tle_line1: str, tle_line2: str) -> EarthSatellite:
    try:
        return EarthSatellite(tle_line1, tle_line2, "SAT", load.timescale())
    except Exception as e:
        raise ValueError(f"Malformed TLE: {e}")

def compute_subpoint_at(tle_line1: str, tle_line2: str, at_time_utc: datetime) -> Tuple[float, float, float]:
    """Return (lat_dd, lon_dd, alt_km) at at_time_utc. lon ∈ [-180,180]."""
    sat = _make_sat(tle_line1, tle_line2)
    ts = load.timescale()
    t = ts.from_datetime(at_time_utc)
    geoc = sat.at(t)
    subpoint = wgs84.subpoint(geoc)
    lat = subpoint.latitude.degrees
    lon = ((subpoint.longitude.degrees + 180) % 360) - 180  # wrap to [-180,180]
    alt = subpoint.elevation.km
    return (lat, lon, alt)

def compute_subpoint_now(tle_df: pd.DataFrame, when_utc: datetime) -> pd.DataFrame:
    """
    MVP stub: return empty DataFrame until TLE propagation is enabled.
    Future: compute sub-satellite (lat_dd, lon_dd) for each row in tle_df at when_utc.
    """
    logging.info("[sat_compute_groundtrack] Subpoint computation not implemented in MVP.")
    return pd.DataFrame(columns=["sat_id","lat_dd","lon_dd"])

def compute_short_track(
    tle_line1: str, tle_line2: str,
    center_utc: datetime,
    forward_min: int = 60,
    step_s: int = 60
) -> Tuple[List[List[float]], datetime, datetime]:
    """
    Return (coords, start_utc, end_utc) where coords is a list of [lon,lat] points
    sampling center_utc → +forward_min at step_s. Handle anti-meridian (unwrap then wrap).
    """
    sat = _make_sat(tle_line1, tle_line2)
    ts = load.timescale()
    n_steps = int(forward_min * 60 // step_s) + 1
    times = [center_utc + timedelta(seconds=k * step_s) for k in range(n_steps)]
    t_sf = ts.from_datetimes(times)
    geoc = sat.at(t_sf)
    lats = wgs84.subpoint(geoc).latitude.degrees
    lons = wgs84.subpoint(geoc).longitude.degrees
    # Unwrap longitudes for smooth tracks, then re-wrap to [-180,180]
    lons_unwrapped = np.unwrap(np.radians(lons))
    lons_deg = np.degrees(lons_unwrapped)
    lons_wrapped = ((lons_deg + 180) % 360) - 180
    start_utc = center_utc
    end_utc = times[-1]
    return ([[float(lon), float(lat)] for lon, lat in zip(lons_wrapped, lats)],
            start_utc, end_utc)

def compute_tracks(
    tle_df: pd.DataFrame,
    when_utc: datetime,
    minutes_past: int = 10,
    minutes_future: int = 10,
    step_s: int = 60
) -> pd.DataFrame:
    """
    MVP stub: return empty DataFrame until TLE propagation is enabled.
    Future: generate per-satellite linestring points for dashed (past) and dotted (future) tracks.
    """
    logging.info("[sat_compute_groundtrack] Track computation not implemented in MVP.")
    return pd.DataFrame(columns=["sat_id","when_utc","lat_dd","lon_dd","segment"])

def find_next_pass_marker(
    tle_line1: str, tle_line2: str,
    alert_latlon: Tuple[float, float],
    start_utc: datetime,
    max_hours: int = 12
) -> Optional[Dict[str, object]]:
    """Return {'time_utc': dt, 'lat_dd': lat, 'lon_dd': lon, 'elevation_max_deg': elev} for first LOS > 0° (within max_hours), else None."""
    sat = _make_sat(tle_line1, tle_line2)
    ts = load.timescale()
    step_s = 60
    n_steps = int(max_hours * 3600 // step_s)
    times = [start_utc + timedelta(seconds=k * step_s) for k in range(n_steps)]
    t_sf = ts.from_datetimes(times)
    # Observer at alert_latlon, sea level
    obs = wgs84.latlon(alert_latlon[0], alert_latlon[1], elevation_m=0)
    geoc = sat.at(t_sf)
    diff = geoc - obs
    el, az, dist = diff.altaz()
    elevations = el.degrees
    # Find first index where elevation crosses above 0
    above = np.where(elevations > 0)[0]
    if len(above) == 0:
        return None
    # Find contiguous block for the first pass
    first = above[0]
    last = first
    for idx in above[1:]:
        if idx == last + 1:
            last = idx
        else:
            break
    # Refine max elevation in this pass
    pass_idxs = range(first, last + 1)
    max_idx = first + np.argmax(elevations[pass_idxs])
    t_max = times[max_idx]
    geoc_max = sat.at(ts.from_datetime(t_max))
    subpoint = wgs84.subpoint(geoc_max)
    return {
        "time_utc": t_max,
        "lat_dd": float(subpoint.latitude.degrees),
        "lon_dd": float(((subpoint.longitude.degrees + 180) % 360) - 180),
        "elevation_max_deg": float(elevations[max_idx])
    }

