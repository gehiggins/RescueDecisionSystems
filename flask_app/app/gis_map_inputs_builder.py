# ============================== RDS STANDARD HEADER ==============================
# Script Name: gis_map_inputs_builder.py
# Last Updated (UTC): 2025-09-15
# Update Summary:
#   - Initial implementation: builds unified GIS map input DataFrame from positions, weather, and stations.
# Description:
#   - Pure transforms to produce a single DataFrame for GIS mapping, including alert positions, range rings, weather, and station layers.
# Data Handling Notes:
#   - No file I/O; all logic is in-memory and stateless. Handles missing columns gracefully.

import math
import logging
import pandas as pd
import numpy as np
from typing import Optional
from app.utils_display import format_us_display, to_dual_time, derive_local_tz

LOG = logging.getLogger(__name__)

def _is_missing(v):
    return v is None or (isinstance(v, float) and np.isnan(v))

def _fmt(v, fmt=None, dash="—"):
    if _is_missing(v):
        return dash
    try:
        return format(v, fmt) if fmt else str(v)
    except Exception:
        return dash

def _fmt_num(v, decimals):
    try:
        if _is_missing(v):
            return "—"
        return f"{float(v):.{decimals}f}"
    except Exception:
        return "—"

def _latlon_str(lat, lon):
    return f"Lat: {_fmt_num(lat, 5)}, Lon: {_fmt_num(lon, 5)}"

def build_gis_map_inputs_df(
    positions_df: pd.DataFrame,
    wx_df: Optional[pd.DataFrame] = None,
    stations_df: Optional[pd.DataFrame] = None,
    op_tz_env: Optional[str] = None,
    shore_nm: float = 5.0,
    sat_overlays: Optional[list] = None
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
    maritime_flag = False
    try:
        from app.utils_display import is_maritime  # present stub
    except Exception:
        def is_maritime(lat, lon, shore_nm=5.0):  # safe fallback
            return False
    maritime_flag = is_maritime(center_lat, center_lon, shore_nm)

    # --- Alert positions layer ---
    if positions_df is not None and not positions_df.empty:
        for _, r in positions_df.iterrows():
            lat = r.get("lat_dd")
            lon = r.get("lon_dd")
            label = r.get("role", "Alert")
            ts = r.get("ts_utc", None)
            ts_utc, ts_local = to_dual_time(ts, local_tz) if ts is not None else (None, None)
            popup_fields = [f"<b>{label}</b>", _latlon_str(lat, lon)]
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
                "source_id": r.get("site_id") or "",
                "is_maritime": maritime_flag,
                "range_ring_meters": None,
                "wave_height_m": None,
                "wind_ms": None,
                "temp_C": None,
                "wave_height_display": "None",
                "wind_display": "None",
                "temp_display": "None",
            })

    # --- Range rings layer (if present) ---
    if positions_df is not None and not positions_df.empty and "range_ring_meters" in positions_df.columns:
        for _, r in positions_df.iterrows():
            ring_m = r.get("range_ring_meters")
            lat = r.get("lat_dd")
            lon = r.get("lon_dd")
            if ring_m is None or np.isnan(ring_m):
                continue
            label = f"Range Ring {_fmt_num(ring_m, 0)} m"
            radius_line = f"Radius: {int(ring_m)} m" if ring_m is not None else "Radius: —"
            popup_html = "<br>".join([f"<b>{label}</b>", _latlon_str(lat, lon), radius_line])
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
                "source_id": r.get("site_id") or "",
                "is_maritime": maritime_flag,
                "range_ring_meters": ring_m,
                "wave_height_m": None,
                "wind_ms": None,
                "temp_C": None,
                "wave_height_display": "None",
                "wind_display": "None",
                "temp_display": "None",
            })

    # --- Weather layer ---
    if wx_df is not None and not wx_df.empty:
        for _, w in wx_df.iterrows():
            lat = w.get("lat_dd")
            lon = w.get("lon_dd")
            ts = w.get("obs_time", None)
            ts_utc, ts_local = to_dual_time(ts, local_tz) if ts is not None else (None, None)
            wave_m = w.get("wave_height_m")
            wind_ms = w.get("wind_ms")
            temp_C = w.get("temp_C")
            display = format_us_display(
                wave_height_m=wave_m,
                wind_ms=wind_ms,
                temp_C=temp_C
            )
            wave_display = display.get("wave_height_display", "None")
            wind_display = display.get("wind_display", "None")
            temp_display = display.get("temp_display", "None")
            popup_fields = [f"<b>Weather</b>", _latlon_str(lat, lon)]
            if ts_utc: popup_fields.append(f"UTC: {ts_utc}")
            if ts_local: popup_fields.append(f"Local: {ts_local}")
            popup_fields += [f"Waves: {wave_display}", f"Wind: {wind_display}", f"Temp: {temp_display}"]
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
                "source_id": w.get("station_id") or "",
                "is_maritime": maritime_flag,
                "range_ring_meters": None,
                "wave_height_m": wave_m,
                "wind_ms": wind_ms,
                "temp_C": temp_C,
                "wave_height_display": wave_display,
                "wind_display": wind_display,
                "temp_display": temp_display,
            })

    # --- Stations layer ---
    if stations_df is not None and not stations_df.empty:
        for _, s in stations_df.iterrows():
            lat = s.get("lat_dd")
            lon = s.get("lon_dd")
            label = s.get("name", "Station")
            wave_display = s.get("wave_height_display", "None")
            wind_display = s.get("wind_display", "None")
            temp_display = s.get("temp_display", "None")
            popup_fields = [
                f"<b>{label}</b>",
                _latlon_str(lat, lon),
                f"Type: {s.get('type','N/A')}",
                f"Waves: {wave_display}",
                f"Wind: {wind_display}",
                f"Temp: {temp_display}"
            ]
            popup_html = "<br>".join(popup_fields)
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
                "source_id": s.get('station_id') or s.get('source_id') or s.get('id') or "",
                "is_maritime": maritime_flag,
                "range_ring_meters": None,
                "wave_height_m": None,
                "wind_ms": None,
                "temp_C": None,
                "wave_height_display": wave_display,
                "wind_display": wind_display,
                "temp_display": temp_display,
            })

    # --- Satellite overlays ---
    if sat_overlays:
        for o in sat_overlays:
            # Image overlay
            if "bounds" in o and o["bounds"]:
                rows.append({
                    "site_id": positions_df["site_id"].iloc[0] if positions_df is not None and not positions_df.empty else None,
                    "layer": "satellite_overlay",
                    "geom_type": "ImageOverlay",
                    "geometry": {
                        "type": "ImageOverlay",
                        "bounds": o["bounds"],
                        "image_path": o.get("image_path", ""),
                        "opacity": o.get("opacity", 0.6),
                        "name": o.get("name", "overlay")
                    },
                    "ts_utc": None,
                    "ts_local": None,
                    "local_tz": None,
                    "label": o.get("name", "Satellite Overlay"),
                    "popup_html": o.get("name", "Satellite Overlay"),
                    "style_hint": {},
                    "source_table": "satellite",
                    "source_id": o.get("name", ""),
                    "is_maritime": None,
                    "range_ring_meters": None,
                    "wave_height_m": None,
                    "wind_ms": None,
                    "temp_C": None,
                    "wave_height_display": "None",
                    "wind_display": "None",
                    "temp_display": "None",
                })
            # Line/poly overlay
            if "coordinates" in o and o["coordinates"]:
                rows.append({
                    "site_id": positions_df["site_id"].iloc[0] if positions_df is not None and not positions_df.empty else None,
                    "layer": "satellite_overlay",
                    "geom_type": "LineString",
                    "geometry": {
                        "type": "LineString",
                        "coordinates": o["coordinates"],
                        "name": o.get("name", "overlay")
                    },
                    "ts_utc": None,
                    "ts_local": None,
                    "local_tz": None,
                    "label": o.get("name", "Satellite Overlay"),
                    "popup_html": o.get("name", "Satellite Overlay"),
                    "style_hint": {},
                    "source_table": "satellite",
                    "source_id": o.get("name", ""),
                    "is_maritime": None,
                    "range_ring_meters": None,
                    "wave_height_m": None,
                    "wind_ms": None,
                    "temp_C": None,
                    "wave_height_display": "None",
                    "wind_display": "None",
                    "temp_display": "None",
                })

    # --- Satellite overlay (footprint + short track + optional next-pass) ---
    try:
        from app.sat_pipeline import build_sat_overlay_df

        # Build minimal alert_df for SAT pipeline from available context
        _lat = positions_df.iloc[0]['lat_dd'] if positions_df is not None and not positions_df.empty else None
        _lon = positions_df.iloc[0]['lon_dd'] if positions_df is not None and not positions_df.empty else None
        _alert_time = pd.Timestamp.now(tz="UTC")
        _alert = pd.DataFrame([{
            'alert_id': site_id,
            'alert_time_utc': _alert_time,
            'alert_lat_dd': _lat,
            'alert_lon_dd': _lon,
            # Optional: upstream may add 'norad_id' when testing old alerts
        }])

        import os
        _test_norad = os.getenv("RDS_SAT_TEST_NORAD")
        if _test_norad and _test_norad.isdigit():
            _alert.loc[0, 'norad_id'] = int(_test_norad)

        _sat_df = build_sat_overlay_df(_alert, types=("LEO",), use_tle=True, fallback_to_nearest=True)

        if _sat_df is not None and not _sat_df.empty:
            # Footprint circle (guardrail: require center + radius)
            for _, r in _sat_df.iterrows():
                # Footprint circle
                lat = r.get('lat_dd'); lon = r.get('lon_dd'); rad_km = r.get('footprint_radius_km')
                if pd.notna(lat) and pd.notna(lon) and pd.notna(rad_km):
                    rows.append({
                        "site_id": site_id,
                        "layer": "satellite_overlay",
                        "geom_type": "Circle",
                        "geometry": {
                            "type": "Circle",
                            "center": [float(lon), float(lat)],
                            "radius_m": float(rad_km) * 1000.0
                        },
                        "label": f"{r.get('sat_name', 'SAT')} (TLE age {r.get('tle_age_hours', 'NA')}h)",
                        "popup_html": f"TLE epoch: {r.get('tle_epoch_utc', 'NA')}",
                        "style_hint": {"weight": 1, "opacity": 0.6},
                        # fields the renderer expects:
                        "lat_dd": float(lat),
                        "lon_dd": float(lon),
                        "footprint_radius_km": float(rad_km),
                    })

                # Short forward track
                _track = r.get("track_coords")
                if isinstance(_track, (list, tuple)) and len(_track) > 1:
                    # Drop any vertices with NaN/None and coerce to float
                    _clean = []
                    for pt in _track:
                        try:
                            lon, lat = pt  # stored as [lon, lat]
                            if pd.notna(lon) and pd.notna(lat):
                                _clean.append((float(lon), float(lat)))
                        except Exception:
                            continue
                    if len(_clean) > 1:
                        first_lon, first_lat = _clean[0]
                        rows.append({
                            "site_id": site_id,
                            "layer": "satellite_overlay",
                            "geom_type": "LineString",
                            "geometry": {"type": "LineString", "coordinates": _clean},
                            "label": "Satellite track",
                            "popup_html": "Forward track",
                            "style_hint": {"dash": "4,6", "opacity": 0.6},
                            # keep columns non-null so Folium doesn't crash:
                            "lat_dd": float(first_lat),
                            "lon_dd": float(first_lon),
                            "footprint_radius_km": None,
                        })

                # Next-pass marker
                npm = r.get("next_pass_marker")
                if isinstance(npm, dict) and pd.notna(npm.get("lat_dd")) and pd.notna(npm.get("lon_dd")):
                    rows.append({
                        "site_id": site_id,
                        "layer": "satellite_overlay",
                        "geom_type": "Point",
                        "geometry": {"type": "Point", "coordinates": [float(npm["lon_dd"]), float(npm["lat_dd"])]},
                        "label": "Next pass",
                        "popup_html": f"Next pass {npm.get('time_utc')}",
                        "style_hint": {"icon": "pin"},
                        # keep columns present for renderer:
                        "lat_dd": float(npm["lat_dd"]),
                        "lon_dd": float(npm["lon_dd"]),
                        "footprint_radius_km": None,
                    })
    except Exception as _sat_e:
        LOG.warning(f"[SAT] overlay inject skipped: {_sat_e}")

    # -- drop invalid geometries (no NaNs) --
    def _valid_coords_pair(p):
        try:
            lon, lat = p
            return pd.notna(lon) and pd.notna(lat)
        except Exception:
            return False

    def _is_valid_row(row):
        g = row.get("geometry") or {}
        t = row.get("geom_type")
        if t == "Point":
            coords = g.get("coordinates")
            return isinstance(coords, (list, tuple)) and len(coords) == 2 and _valid_coords_pair(coords)
        if t == "Circle":
            center = g.get("center"); rad = g.get("radius_m")
            return (isinstance(center, (list, tuple)) and len(center) == 2 and _valid_coords_pair(center)
                    and pd.notna(rad))
        if t == "LineString":
            coords = g.get("coordinates")
            if not isinstance(coords, (list, tuple)) or len(coords) < 2:
                return False
            # all vertices valid
            return all(_valid_coords_pair(pt) for pt in coords)
        # other types pass through
        return True

    df_out = pd.DataFrame(rows)
    if not df_out.empty:
        df_out = df_out[df_out.apply(_is_valid_row, axis=1)].reset_index(drop=True)

    # Ensure contract columns exist
    contract_cols = [
        'site_id','layer','geom_type','geometry','ts_utc','ts_local','local_tz',
        'label','popup_html','style_hint','source_table','source_id','is_maritime',
        'range_ring_meters','wave_height_m','wind_ms','temp_C',
        'wave_height_display','wind_display','temp_display'
    ]
    for col in contract_cols:
        if col not in df_out.columns:
            df_out[col] = None

    # -- derive lat_dd/lon_dd from geometry for all rows --
    if 'lat_dd' not in df_out.columns:
        df_out['lat_dd'] = np.nan
    if 'lon_dd' not in df_out.columns:
        df_out['lon_dd'] = np.nan

    def _derive_latlon(row):
        g = row.get('geometry') or {}
        t = row.get('geom_type')
        try:
            if t == 'Point':
                lon, lat = g.get('coordinates', (None, None))
                return lat, lon
            if t == 'Circle':
                lon, lat = g.get('center', (None, None))
                return lat, lon
            if t == 'LineString':
                coords = g.get('coordinates', [])
                if isinstance(coords, (list, tuple)) and len(coords) > 0:
                    lon, lat = coords[0]
                    return lat, lon
        except Exception:
            pass
        return row.get('lat_dd'), row.get('lon_dd')

    _latlon = df_out.apply(_derive_latlon, axis=1, result_type='expand')
    df_out['lat_dd'] = df_out['lat_dd'].fillna(_latlon[0])
    df_out['lon_dd'] = df_out['lon_dd'].fillna(_latlon[1])

    # Drop rows that still have NaN location where a location is required
    need_loc = df_out['geom_type'].isin(['Point','Circle','LineString'])
    df_out = df_out[~(need_loc & (df_out['lat_dd'].isna() | df_out['lon_dd'].isna()))].reset_index(drop=True)

    # -- normalize geometry: coerce JSON/text -> dict --
    import json
    def _norm_geom(g):
        if isinstance(g, str):
            gs = g.strip()
            if (gs.startswith("{") and gs.endswith("}")) or (gs.startswith("[") and gs.endswith("]")):
                try:
                    return json.loads(gs)
                except Exception:
                    return None
            return None
        return g
    if 'geometry' in df_out.columns:
        df_out['geometry'] = df_out['geometry'].apply(_norm_geom)

    return df_out