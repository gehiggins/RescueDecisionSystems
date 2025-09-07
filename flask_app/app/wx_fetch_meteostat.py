# Script Name: wx_fetch_meteostat.py
# APPLY TEST - selection scope
# Last Updated: 2025-09-07
```
# Update Summary: Fix timezone normalization to prevent TypeError, add debug breadcrumbs, enforce DataFrame return rules.
# Description: Fetches hourly weather data from Meteostat stations near a target site, normalizes timestamps, trims to time window, and returns DataFrame.
# External Data Sources: Meteostat API.
# Internal Variables: site_id (str), start_utc (datetime, tz-aware), end_utc (datetime, tz-aware).
# Produced DataFrames: weather_data_df (columns: station_id, observation_time, temp_c, dewpoint_c, rh_pct, wind_ms, wind_dir_deg, wind_gust_ms, pressure_hpa).
# Data Handling Notes: Always returns DataFrame (empty if failure), timestamps tz-aware UTC, NaN for missing values.
import pandas as pd
from datetime import timedelta

from flask_app.setup_imports import *
import numpy as np

LOG = logging.getLogger(__name__)

try:
    from meteostat import Stations, Hourly
    _HAS_METEOSTAT = True
except Exception:
    _HAS_METEOSTAT = False

def ensure_utc(ts):
    """
    Convert timestamp to UTC
    """
    t = pd.to_datetime(ts, utc=True)
    return t if t.tzinfo else t.tz_localize("UTC")

def fetch_meteostat_obs_near(lat: float, lon: float, start_utc, end_utc,
                             radius_km: float = 50.0, max_stations: int = 5) -> pd.DataFrame:
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

    if not _HAS_METEOSTAT:
        LOG.info("Meteostat not installed; skipping.")
        return out

    start_utc = ensure_utc(start_utc)
    end_utc = ensure_utc(end_utc)

    try:
        stations_df = Stations().nearby(lat, lon).fetch(max_stations * 3)
        LOG.debug("Meteostat stations: cols=%s index.name=%s shape=%s",
                  list(stations_df.columns), stations_df.index.name, stations_df.shape)
        if stations_df is None or len(stations_df) == 0:
            LOG.info("Meteostat: no nearby stations within radius_km=%s", radius_km)
            return pd.DataFrame(columns=cols)
        R = 6371.0
        def haversine_km(lat1, lon1, lat2, lon2):
            p = np.radians([lat1, lon1, lat2, lon2])
            dphi = p[2]-p[0]; dlmb = p[3]-p[1]
            a = np.sin(dphi/2)**2 + np.cos(p[0])*np.cos(p[2])*np.sin(dlmb/2)**2
            return 2*R*np.arcsin(np.sqrt(a))
        stations_df["dist_km"] = stations_df.apply(lambda r: haversine_km(lat, lon, r["latitude"], r["longitude"]), axis=1)
        stations_df = stations_df[stations_df["dist_km"] <= radius_km].head(max_stations)
    except Exception as e:
        LOG.warning(f"Meteostat station lookup failed: {e}")
        return out

    rows = []
    for _, s in stations_df.iterrows():
        try:
            if isinstance(s, pd.Series):
                sid = (
                    str(s["id"]) if "id" in s.index
                    else str(s["station"]) if "station" in s.index
                    else str(s.name)
                )
            else:
                sid = str(getattr(s, "id", getattr(s, "station", getattr(s, "Index", s))) )
        except Exception as e:
            LOG.debug("Meteostat station row keys=%s", list(getattr(s, "index", [])))
            LOG.warning("Skipping station; unable to determine id: %s", e)
            continue
        slat, slon = float(s["latitude"]), float(s["longitude"])
        sname = s.get("name") if "name" in s else None
        try:
            ms_start = (end_utc - pd.Timedelta(hours=24)).to_pydatetime()
            ms_end = end_utc.to_pydatetime()

            hourly = Hourly(sid, ms_start, ms_end)
            df_h = hourly.fetch()

            if df_h is not None and not df_h.empty:
                LOG.debug("[Meteostat] (station %s) index (pre) dtype=%s, tz=%s, first=%s, last=%s", sid, type(df_h.index), getattr(df_h.index, 'tz', None), df_h.index[0] if len(df_h.index)>0 else None, df_h.index[-1] if len(df_h.index)>0 else None)
            if df_h is not None and not df_h.empty:
                idx = pd.to_datetime(df_h.index, utc=False, errors="coerce")
                        if getattr(idx, "tz", None) is None:
                    idx = idx.tz_localize("UTC")
                else:
                    idx = idx.tz_convert("UTC")
                df_h.index = idx
                LOG.debug("[Meteostat] (station %s) index (norm) dtype=%s, tz=%s, first=%s, last=%s", sid, type(df_h.index), getattr(df_h.index, 'tz', None), df_h.index[0] if len(df_h.index)>0 else None, df_h.index[-1] if len(df_h.index)>0 else None)

            LOG.debug("[Meteostat] (station %s) filter window start_utc=%s (tzinfo=%s) end_utc=%s (tzinfo=%s)", sid, start_utc, getattr(start_utc, 'tzinfo', None), end_utc, getattr(end_utc, 'tzinfo', None))

            start_ts = pd.Timestamp(start_utc)
            end_ts   = pd.Timestamp(end_utc)
            if df_h is not None and not df_h.empty:
            df_h = df_h[(df_h.index >= start_ts) & (df_h.index <= end_ts)]

            if df_h is None or df_h.empty:
                LOG.info(f"Meteostat fallback triggered for station {sid}")
                try:
                    df_latest = Hourly(sid, None, None).fetch()
                    if df_latest is not None and not df_latest.empty:
                        idx = pd.to_datetime(df_latest.index, utc=False, errors="coerce")
                        if idx.tz is None:
                            idx = idx.tz_localize("UTC")
                        else:
                            idx = idx.tz_convert("UTC")
                        df_latest.index = idx
                        LOG.debug("[Meteostat-Fallback] (station %s) index (norm) dtype=%s, tz=%s, first=%s, last=%s", sid, type(df_latest.index), getattr(df_latest.index, 'tz', None), df_latest.index[0] if len(df_latest.index)>0 else None, df_latest.index[-1] if len(df_latest.index)>0 else None)
                        cutoff = pd.Timestamp(end_utc)
                        df_latest = df_latest[df_latest.index <= cutoff]
                        if not df_latest.empty:
                            df_h = df_latest.tail(1)
                            LOG.debug("[Meteostat-Fallback] (station %s) using row idx=%s", sid, df_h.index[-1])
        except Exception as e:
                    LOG.info(f"Meteostat latest probe failed for {sid}: {e}")

            if df_h is None or df_h.empty:
                continue

            for idx, r in df_h.iterrows():
                ts = idx
                rows.append({
                    "source_id": sid,
                    "valid_utc": ts,
                    "source_type": "station",
                    "provider": "Meteostat",
                    "lat": slat, "lon": slon,
                    "temp_c": r.get("temp", np.nan),
                    "dewpoint_c": r.get("dwpt", np.nan),
                    "rh_pct": r.get("rhum", np.nan),
                    "wind_ms": r.get("wspd", np.nan),
                    "wind_dir_deg": r.get("wdir", np.nan),
                    "wind_gust_ms": np.nan,
                    "pressure_hpa": r.get("pres", np.nan),
                    "precip_mmhr": r.get("prcp", np.nan),
                    "visibility_km": np.nan,
                    "cloud_cover_pct": np.nan,
                    "wave_height_m": np.nan, "wave_period_s": np.nan, "wave_dir_deg": np.nan,
                    "swell_height_m": np.nan, "swell_period_s": np.nan, "swell_dir_deg": np.nan,
                    "wind_wave_height_m": np.nan, "wind_wave_period_s": np.nan, "wind_wave_dir_deg": np.nan,
                    "sst_c": np.nan,
                    "name": sname if pd.notna(sname) else None,
                    "owner": "Meteostat",
                    "deployment_notes": None
                })
        except Exception as e:
            LOG.info(f"Meteostat hourly fetch failed for {sid}: {e}")

    if rows:
        out = pd.DataFrame(rows)
        out["valid_utc"] = pd.to_datetime(out["valid_utc"], utc=True).dt.tz_convert("UTC")
    else:
        out = pd.DataFrame(columns=cols)
    return out
# OS_SENTINEL_CHECK

