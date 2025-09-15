# ============================== RDS STANDARD HEADER ==============================
# Script Name: gis_map_inputs_builder.py
# Last Updated (UTC): 2025-09-15
# Update Summary:
#   - Initial implementation: builds unified GIS map input DataFrame from positions, weather, and stations.
# Description:
#   - Pure transforms to produce a single DataFrame for GIS mapping, including alert positions, range rings, weather, and station layers.
# Data Handling Notes:
#   - No file I/O; all logic is in-memory and stateless. Handles missing columns gracefully.

from app.setup_imports import *
from app.utils_display import derive_local_tz, to_dual_time, format_us_display, is_maritime

from typing import Optional

import logging
LOG = logging.getLogger(__name__)

def build_gis_map_inputs_df(
    positions_df: pd.DataFrame,
    wx_df: Optional[pd.DataFrame] = None,
    stations_df: Optional[pd.DataFrame] = None,
    op_tz_env: Optional[str] = None,
    shore_nm: float = 5.0
) -> pd.DataFrame:
    """
    Return a single DataFrame (gis_map_inputs_df) with rows for:
      - alert_positions
      - range_rings (encode as Circle: geometry={'type':'Circle','center':[lon,lat],'radius_m':N})
      - weather (point)
      - stations (point)
    Schema columns (at minimum):
      ['site_id','layer','geom_type','geometry','ts_utc','ts_local','local_tz',
       'label','popup_html','style_hint','source_table','source_id','is_maritime',
       'range_ring_meters','wave_height_m','wind_ms','temp_C',
       'wave_height_display','wind_display','temp_display']
    """
    rows = []

    # Determine center from positions_df (A first, else B)
    center_lat, center_lon = None, None
    site_id = None
    if positions_df is not None and not positions_df.empty:
        for _, r in positions_df.iterrows():
            if pd.notna(r.get("lat_dd")) and pd.notna(r.get("lon_dd")):
                center_lat, center_lon = float(r["lat_dd"]), float(r["lon_dd"])
                site_id = r.get("site_id")
                break
    if center_lat is None or center_lon is None:
        LOG.warning("build_gis_map_inputs_df: No valid center found in positions_df; using (0,0)")
        center_lat, center_lon = 0.0, 0.0

    local_tz = derive_local_tz(center_lat, center_lon, op_tz_env)
    maritime_flag = is_maritime(center_lat, center_lon, shore_nm)

    # --- Alert positions layer ---
    if positions_df is not None and not positions_df.empty:
        for _, r in positions_df.iterrows():
            lat = r.get("lat_dd")
            lon = r.get("lon_dd")
            label = r.get("role", "Alert")
            ts = r.get("ts_utc", None)
            ts_utc, ts_local = to_dual_time(ts, local_tz) if ts is not None else (None, None)
            popup_fields = [f"<b>{label}</b>", f"Lat: {lat:.5f}", f"Lon: {lon:.5f}"]
            if ts_utc: popup_fields.append(f"UTC: {ts_utc}")
            if ts_local: popup_fields.append(f"Local: {ts_local}")
            popup_html = "<br>".join(popup_fields)
            rows.append({
                "site_id": r.get("site_id"),
                "layer": "alert_position",
                "geom_type": "Point",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "ts_utc": ts_utc,
                "ts_local": ts_local,
                "local_tz": local_tz,
                "label": label,
                "popup_html": popup_html,
                "style_hint": {"weight": 3, "opacity": 0.9},
                "source_table": "positions",
                "source_id": r.get("site_id"),
                "is_maritime": maritime_flag,
                "range_ring_meters": None,
                "wave_height_m": None,
                "wind_ms": None,
                "temp_C": None,
                "wave_height_display": None,
                "wind_display": None,
                "temp_display": None,
            })

    # --- Range rings layer (if present) ---
    if positions_df is not None and not positions_df.empty and "range_ring_meters" in positions_df.columns:
        for _, r in positions_df.iterrows():
            ring_m = r.get("range_ring_meters")
            if ring_m is not None and not (isinstance(ring_m, float) and np.isnan(ring_m)):
                lat = r.get("lat_dd")
                lon = r.get("lon_dd")
                label = f"Range Ring {int(ring_m)} m"
                popup_html = f"<b>{label}</b><br>Lat: {lat:.5f}<br>Lon: {lon:.5f}<br>Radius: {int(ring_m)} m"
                rows.append({
                    "site_id": r.get("site_id"),
                    "layer": "range_ring",
                    "geom_type": "Circle",
                    "geometry": {"type": "Circle", "center": [lon, lat], "radius_m": ring_m},
                    "ts_utc": None,
                    "ts_local": None,
                    "local_tz": local_tz,
                    "label": label,
                    "popup_html": popup_html,
                    "style_hint": {"weight": 2, "opacity": 0.5, "dash": "2,6"},
                    "source_table": "positions",
                    "source_id": r.get("site_id"),
                    "is_maritime": maritime_flag,
                    "range_ring_meters": ring_m,
                    "wave_height_m": None,
                    "wind_ms": None,
                    "temp_C": None,
                    "wave_height_display": None,
                    "wind_display": None,
                    "temp_display": None,
                })

    # --- Weather layer ---
    if wx_df is not None and not wx_df.empty:
        for _, w in wx_df.iterrows():
            lat = w.get("lat_dd")
            lon = w.get("lon_dd")
            ts = w.get("obs_time", None)
            ts_utc, ts_local = to_dual_time(ts, local_tz) if ts is not None else (None, None)
            wave_m = w.get("obs_value") if w.get("obs_type") == "wave_height_m" else None
            wind_ms = w.get("obs_value") if w.get("obs_type") == "wind_ms" else None
            temp_C = w.get("temp_C") if "temp_C" in w else None
            display = format_us_display(wave_height_m=wave_m, wind_ms=wind_ms, temp_C=temp_C)
            popup_fields = [f"<b>Weather</b>", f"Lat: {lat:.5f}", f"Lon: {lon:.5f}"]
            if ts_utc: popup_fields.append(f"UTC: {ts_utc}")
            if ts_local: popup_fields.append(f"Local: {ts_local}")
            for k, v in display.items():
                popup_fields.append(f"{k}: {v}")
            popup_html = "<br>".join(popup_fields)
            rows.append({
                "site_id": site_id,
                "layer": "weather",
                "geom_type": "Point",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "ts_utc": ts_utc,
                "ts_local": ts_local,
                "local_tz": local_tz,
                "label": "Weather",
                "popup_html": popup_html,
                "style_hint": {"weight": 2, "opacity": 0.8},
                "source_table": "weather",
                "source_id": w.get("station_id"),
                "is_maritime": maritime_flag,
                "range_ring_meters": None,
                "wave_height_m": wave_m,
                "wind_ms": wind_ms,
                "temp_C": temp_C,
                "wave_height_display": display.get("wave_height_display"),
                "wind_display": display.get("wind_display"),
                "temp_display": display.get("temp_display"),
            })

    # --- Stations layer ---
    if stations_df is not None and not stations_df.empty:
        for _, s in stations_df.iterrows():
            lat = s.get("lat_dd")
            lon = s.get("lon_dd")
            label = s.get("name", "Station")
            popup_html = f"<b>{label}</b><br>Lat: {lat:.5f}<br>Lon: {lon:.5f}<br>Type: {s.get('type')}"
            rows.append({
                "site_id": site_id,
                "layer": "station",
                "geom_type": "Point",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "ts_utc": None,
                "ts_local": None,
                "local_tz": local_tz,
                "label": label,
                "popup_html": popup_html,
                "style_hint": {"weight": 2, "opacity": 0.7},
                "source_table": "stations",
                "source_id": s.get("station_id"),
                "is_maritime": maritime_flag,
                "range_ring_meters": None,
                "wave_height_m": None,
                "wind_ms": None,
                "temp_C": None,
                "wave_height_display": None,
                "wind_display": None,
                "temp_display": None,
            })

    df_out = pd.DataFrame(rows)
    return df_out