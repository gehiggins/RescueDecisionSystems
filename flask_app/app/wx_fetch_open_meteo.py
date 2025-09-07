# Script Name: wx_fetch_open_meteo.py
# Last Updated (UTC): 2025-09-02
# Update Summary:
# • New: Open-Meteo model+marine hourly fetcher (point samples) → wx_obs rows
# Description:
# • Fetches hourly weather + optional waves at (lat, lon) and normalizes to wx_obs schema.
# External Data Sources:
# • Open-Meteo Weather & Marine API (no key) – UTC hourly.
# Internal Variables:
# • Inputs: lat, lon, start_utc, end_utc, include_marine
# Produced DataFrames:
# • wx_obs-slice: columns = source_id, valid_utc, source_type, provider, lat, lon,
#   temp_c, dewpoint_c, rh_pct, wind_ms, wind_dir_deg, wind_gust_ms, pressure_hpa,
#   precip_mmhr, visibility_km, cloud_cover_pct, wave_height_m, wave_period_s,
#   wave_dir_deg, swell_*, wind_wave_*, sst_c, name, owner, deployment_notes
# Data Handling Notes:
# • UTC timestamps; SI units; missing → NaN; dewpoint via Magnus from T/RH.


from flask_app.setup_imports import *  # pandas as pd, numpy as np, logging, etc.
import requests

LOG = logging.getLogger(__name__)

def _to_utc(ts):
    t = pd.to_datetime(ts, utc=True)
    return t if t.tzinfo else t.tz_localize("UTC")

def _magnus_dewpoint_c(temp_c, rh_pct):
    if pd.isna(temp_c) or pd.isna(rh_pct):
        return np.nan
    a, b = 17.625, 243.04
    gamma = (a * temp_c) / (b + temp_c) + np.log(max(min(rh_pct, 100.0), 0.1) / 100.0)
    return (b * gamma) / (a - gamma)

def fetch_open_meteo_obs(lat: float, lon: float, start_utc, end_utc, include_marine: bool = True) -> pd.DataFrame:
    start_utc = _to_utc(start_utc)
    end_utc = _to_utc(end_utc)

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
    marine = {}
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
        except Exception as e:
            LOG.info(f"Open-Meteo marine unavailable @({lat:.4f},{lon:.4f}): {e}")
            marine = {}

    rows = []
    for i, t in enumerate(times):
        ts = _to_utc(t)
        temp_c = hourly.get("temperature_2m",[np.nan]*len(times))[i]
        rh = hourly.get("relative_humidity_2m",[np.nan]*len(times))[i]
        rows.append({
            "source_id": f"om:{lat:.4f},{lon:.4f}",
            "valid_utc": ts,
            "source_type": "model",
            "provider": "Open-Meteo",
            "lat": float(lat),
            "lon": float(lon),
            "temp_c": temp_c,
            "dewpoint_c": _magnus_dewpoint_c(temp_c, rh),
            "rh_pct": rh,
            "wind_ms": hourly.get("wind_speed_10m",[np.nan]*len(times))[i],
            "wind_dir_deg": hourly.get("wind_direction_10m",[np.nan]*len(times))[i],
            "wind_gust_ms": hourly.get("wind_gusts_10m",[np.nan]*len(times))[i],
            "pressure_hpa": hourly.get("pressure_msl",[np.nan]*len(times))[i],
            "precip_mmhr": hourly.get("precipitation",[np.nan]*len(times))[i],
            "visibility_km": np.nan,
            "cloud_cover_pct": hourly.get("cloud_cover",[np.nan]*len(times))[i],
            "wave_height_m": (marine.get("wave_height",[np.nan]*len(times))[i]
                              if marine.get("time") else np.nan),
            "wave_period_s": (marine.get("wave_period",[np.nan]*len(times))[i]
                              if marine.get("time") else np.nan),
            "wave_dir_deg": (marine.get("wave_direction",[np.nan]*len(times))[i]
                              if marine.get("time") else np.nan),
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
