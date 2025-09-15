# Script Name: wx_fetch_open_meteo.py
# Last Updated (UTC): 2025-09-02
# Update Summary:
# â€¢ New: Open-Meteo model+marine hourly fetcher (point samples) â†’ wx_obs rows
# Description:
# â€¢ Fetches hourly weather + optional waves at (lat, lon) and normalizes to wx_obs schema.
# External Data Sources:
# â€¢ Open-Meteo Weather & Marine API (no key) â€“ UTC hourly.
# Internal Variables:
# â€¢ Inputs: lat, lon, start_utc, end_utc, include_marine
# Produced DataFrames:
# â€¢ wx_obs-slice: columns = source_id, valid_utc, source_type, provider, lat, lon,
#   temp_c, dewpoint_c, rh_pct, wind_ms, wind_dir_deg, wind_gust_ms, pressure_hpa,
#   precip_mmhr, visibility_km, cloud_cover_pct, wave_height_m, wave_period_s,
#   wave_dir_deg, swell_*, wind_wave_*, sst_c, name, owner, deployment_notes
# Data Handling Notes:
# â€¢ UTC timestamps; SI units; missing â†’ NaN; dewpoint via Magnus from T/RH.


from app.setup_imports import *  # pandas as pd, numpy as np, logging, etc.
import requests
from app.utils_time import ensure_utc as _to_utc
from app.utils_weather import dewpoint_magnus_c

LOG = logging.getLogger(__name__)

def fetch_open_meteo_obs(lat: float, lon: float, start_utc, end_utc, include_marine: bool = True) -> pd.DataFrame:
    start_utc = _to_utc(start_utc)
    end_utc = _to_utc(end_utc)

    # optional: if start/end may arrive swapped
    if start_utc and end_utc and start_utc > end_utc:
        start_utc, end_utc = end_utc, start_utc

    cols = [
        "source_id","valid_utc","source_type","provider","lat","lon",
        "temp_c","dewpoint_c","rh_pct","wind_ms","wind_dir_deg","wind_gust_ms",
        "pressure_hpa","precip_mmhr","visibility_km","cloud_cover_pct",
        "wave_height_m","wave_period_s","wave_dir_deg",
        "swell_height_m","swell_period_s","swell_dir_deg",
        "wind_wave_height_m","wind_wave_period_s","wind_wave_dir_deg",
        "sst_c","name","owner","deployment_notes"
    ]
    out = pd.DataFrame(columns=cols)

    wx_params = {
        "latitude": lat, "longitude": lon,
        "hourly": ",".join([
            "temperature_2m","relative_humidity_2m","pressure_msl",
            "wind_speed_10m","wind_direction_10m","wind_gusts_10m",
            "precipitation","cloud_cover"
        ]),
        "timezone": "UTC",
        "start_date": start_utc.strftime("%Y-%m-%d"),
        "end_date": end_utc.strftime("%Y-%m-%d")
    }
    try:
        r = requests.get("https://api.open-meteo.com/v1/forecast", params=wx_params, timeout=20)
        r.raise_for_status()
        j = r.json()
    except Exception as e:
        LOG.warning(f"Open-Meteo atmos fetch failed @({lat:.4f},{lon:.4f}): {e}")
        j = {}

    hourly = j.get("hourly", {})
    times = hourly.get("time") or []
    if not times:
        return out
    marine = {}
    m_map = {}
    if include_marine:
        try:
            rm = requests.get("https://marine-api.open-meteo.com/v1/marine", params={
                "latitude": lat, "longitude": lon,
                "hourly": "wave_height,wave_direction,wave_period",
                "timezone": "UTC",
                "start_date": start_utc.strftime("%Y-%m-%d"),
                "end_date": end_utc.strftime("%Y-%m-%d")
            }, timeout=20)
            rm.raise_for_status()
            marine = rm.json().get("hourly", {})
            m_times = [ _to_utc(t) for t in (marine.get("time") or []) ]
            m_h = marine.get("wave_height", [])
            m_p = marine.get("wave_period", [])
            m_d = marine.get("wave_direction", [])
            m_map = { m_times[i]: (m_h[i], m_p[i], m_d[i]) for i in range(len(m_times)) }
        except Exception as e:
            LOG.info(f"Open-Meteo marine unavailable @({lat:.4f},{lon:.4f}): {e}")
            marine = {}
            m_map = {}

    rows = []
    for i, t in enumerate(times):
        ts = _to_utc(t)
        if ts < start_utc or ts > end_utc:
            continue
        temp_c = hourly.get("temperature_2m",[np.nan]*len(times))[i]
        rh = hourly.get("relative_humidity_2m",[np.nan]*len(times))[i]
        wh, wp, wd = m_map.get(ts, (np.nan, np.nan, np.nan))
        rows.append({
            "source_id": f"om:{lat:.4f},{lon:.4f}",
            "valid_utc": ts,
            "source_type": "model",
            "provider": "Open-Meteo",
            "lat": float(lat),
            "lon": float(lon),
            "temp_c": temp_c,
            "dewpoint_c": dewpoint_magnus_c(temp_c, rh),
            "rh_pct": rh,
            "wind_ms": hourly.get("wind_speed_10m",[np.nan]*len(times))[i],
            "wind_dir_deg": hourly.get("wind_direction_10m",[np.nan]*len(times))[i],
            "wind_gust_ms": hourly.get("wind_gusts_10m",[np.nan]*len(times))[i],
            "pressure_hpa": hourly.get("pressure_msl",[np.nan]*len(times))[i],
            "precip_mmhr": hourly.get("precipitation",[np.nan]*len(times))[i],
            "visibility_km": np.nan,
            "cloud_cover_pct": hourly.get("cloud_cover",[np.nan]*len(times))[i],
            "wave_height_m": wh,
            "wave_period_s": wp,
            "wave_dir_deg": wd,
            "swell_height_m": np.nan,
            "swell_period_s": np.nan,
            "swell_dir_deg": np.nan,
            "wind_wave_height_m": np.nan,
            "wind_wave_period_s": np.nan,
            "wind_wave_dir_deg": np.nan,
            "sst_c": np.nan,
            "name": None,
            "owner": "Open-Meteo",
            "deployment_notes": "Model grid point at requested lat/lon"
        })

    if rows:
        out = pd.DataFrame(rows)
        out["valid_utc"] = pd.to_datetime(out["valid_utc"], utc=True).dt.tz_convert("UTC")
    return out

