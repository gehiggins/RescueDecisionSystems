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

_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
_HISTORICAL_FORECAST_URL = (
    "https://historical-forecast-api.open-meteo.com/v1/forecast"
)
_HISTORICAL_WEATHER_URL = "https://archive-api.open-meteo.com/v1/archive"
_HISTORICAL_FORECAST_START = pd.Timestamp("2022-01-01T00:00:00Z")
_LIVE_FORECAST_PAST_DAYS = 92


def _normalize_include_marine(value):
    """Return a boolean without treating non-empty false strings as true."""
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    LOG.warning(
        "Invalid Open-Meteo include_marine=%r; using default True", value
    )
    return True


def _atmosphere_url(end_utc, current_utc=None):
    """Select the provider service whose documented range covers the date."""
    if end_utc < _HISTORICAL_FORECAST_START:
        return _HISTORICAL_WEATHER_URL
    current_utc = (
        _to_utc(current_utc)
        if current_utc is not None
        else pd.Timestamp.now(tz="UTC")
    )
    cutoff = current_utc - pd.Timedelta(days=_LIVE_FORECAST_PAST_DAYS)
    return _HISTORICAL_FORECAST_URL if end_utc < cutoff else _FORECAST_URL


def _numeric_at(mapping, key, index, minimum=None, maximum=None):
    """Return one finite in-range provider value, or NaN when unavailable."""
    if not isinstance(mapping, dict):
        return np.nan
    values = mapping.get(key)
    if not isinstance(values, (list, tuple, np.ndarray, pd.Series)):
        return np.nan
    try:
        raw_value = values[index]
        if isinstance(raw_value, (bool, np.bool_)):
            return np.nan
        value = float(raw_value)
    except (IndexError, KeyError, TypeError, ValueError, OverflowError):
        return np.nan
    if not np.isfinite(value):
        return np.nan
    if minimum is not None and value < minimum:
        return np.nan
    if maximum is not None and value > maximum:
        return np.nan
    return value

def fetch_open_meteo_obs(lat: float, lon: float, start_utc, end_utc, include_marine: bool = True) -> pd.DataFrame:
    start_utc = _to_utc(start_utc)
    end_utc = _to_utc(end_utc)
    include_marine = _normalize_include_marine(include_marine)

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

    if isinstance(lat, (bool, np.bool_)) or isinstance(
        lon, (bool, np.bool_)
    ):
        LOG.warning("Open-Meteo fetch skipped because coordinates are invalid")
        return out
    try:
        lat, lon = float(lat), float(lon)
    except (TypeError, ValueError, OverflowError):
        LOG.warning("Open-Meteo fetch skipped because coordinates are invalid")
        return out
    if (
        not np.isfinite(lat)
        or not np.isfinite(lon)
        or not -90.0 <= lat <= 90.0
        or not -180.0 <= lon <= 180.0
    ):
        LOG.warning("Open-Meteo fetch skipped because coordinates are invalid")
        return out

    if start_utc is None or end_utc is None:
        LOG.warning("Open-Meteo fetch skipped because the UTC window is invalid")
        return out

    wx_params = {
        "latitude": lat, "longitude": lon,
        "hourly": ",".join([
            "temperature_2m","relative_humidity_2m","pressure_msl",
            "wind_speed_10m","wind_direction_10m","wind_gusts_10m",
            "precipitation","cloud_cover"
        ]),
        "timezone": "UTC",
        # Open-Meteo defaults wind speed to km/h. The shared wx_obs contract
        # is metres/second, so request that unit explicitly for speeds/gusts.
        "wind_speed_unit": "ms",
        "start_date": start_utc.strftime("%Y-%m-%d"),
        "end_date": end_utc.strftime("%Y-%m-%d")
    }
    try:
        r = requests.get(_atmosphere_url(end_utc), params=wx_params, timeout=20)
        r.raise_for_status()
        j = r.json()
        if not isinstance(j, dict):
            j = {}
    except Exception as e:
        LOG.warning(f"Open-Meteo atmos fetch failed @({lat:.4f},{lon:.4f}): {e}")
        j = {}

    hourly = j.get("hourly", {})
    if not isinstance(hourly, dict):
        hourly = {}
    times = hourly.get("time") or []
    if not isinstance(times, (list, tuple)):
        times = []
    atmosphere_index = {}
    for i, raw_time in enumerate(times):
        atmosphere_time = _to_utc(raw_time)
        if atmosphere_time is not None:
            atmosphere_index.setdefault(atmosphere_time, i)

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
            marine_payload = rm.json()
            marine = (
                marine_payload.get("hourly", {})
                if isinstance(marine_payload, dict)
                else {}
            )
            if not isinstance(marine, dict):
                marine = {}
            raw_m_times = marine.get("time") or []
            if not isinstance(raw_m_times, (list, tuple)):
                raw_m_times = []
            for i, raw_time in enumerate(raw_m_times):
                marine_time = _to_utc(raw_time)
                if marine_time is None:
                    continue
                m_map[marine_time] = (
                    _numeric_at(marine, "wave_height", i, minimum=0),
                    _numeric_at(marine, "wave_period", i, minimum=0),
                    _numeric_at(
                        marine, "wave_direction", i, minimum=0, maximum=360
                    ),
                )
        except Exception as e:
            LOG.info(f"Open-Meteo marine unavailable @({lat:.4f},{lon:.4f}): {e}")
            m_map = {}

    sample_times = sorted(set(atmosphere_index) | set(m_map))
    if not sample_times:
        return out

    rows = []
    for ts in sample_times:
        if ts < start_utc or ts > end_utc:
            continue
        i = atmosphere_index.get(ts)
        temp_c = _numeric_at(hourly, "temperature_2m", i)
        rh = _numeric_at(
            hourly, "relative_humidity_2m", i, minimum=0, maximum=100
        )
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
            "wind_ms": _numeric_at(
                hourly, "wind_speed_10m", i, minimum=0
            ),
            "wind_dir_deg": _numeric_at(
                hourly, "wind_direction_10m", i, minimum=0, maximum=360
            ),
            "wind_gust_ms": _numeric_at(
                hourly, "wind_gusts_10m", i, minimum=0
            ),
            "pressure_hpa": _numeric_at(
                hourly, "pressure_msl", i, minimum=0
            ),
            "precip_mmhr": _numeric_at(
                hourly, "precipitation", i, minimum=0
            ),
            "visibility_km": np.nan,
            "cloud_cover_pct": _numeric_at(
                hourly, "cloud_cover", i, minimum=0, maximum=100
            ),
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

