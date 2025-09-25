"""
Script Name: gis_mapping.py
Last Updated (UTC): 2025-09-01
Update Summary:
- Static PNG map rendering (matplotlib, AEQD/degree fallback)
- Plots A/B positions, range rings, and saves GeoJSON if requested
- HTML map rendering (Folium, online tiles), accepts alert row today; next step adds DF-based API
Description:
- Folium HTML map rendering from SARSAT alert data (red A/B, range rings). PNG renderer exists for static exports.
Inputs (current step):
- For generate_gis_map_html(alert_row, out_dir, tiles_mode="online"): single alert row (supports legacy latitude_*/longitude_* and new position_lat_dd_* fields)
Outputs:
- HTML at data/maps/<site_id>/gis_map_<site_id>.html (current behavior)
- (Also available) PNG at data/maps/<site_id>/rds_map_<site_id>.png via generate_gis_png(...) (unchanged)
External Data Sources:
- OpenStreetMap tiles (online) for HTML map
Data Handling Notes:
- Accepts both legacy and new field names for A/B; skips writing if no valid coords.
- PNG uses AEQD via pyproj when available; otherwise degree-approx fallback.
Next step: Add generate_gis_map_html_from_dfs(positions_df, ...) (DF inputs, layered map).
"""
# [RDS-ANCHOR: PREAMBLE_END]

#gis_mapping.py


import os
import math
import numpy as np
from app.utils_display import format_us_display, to_dual_time, derive_local_tz
from app.setup_imports import *
from app.utils import log_error_and_continue
import folium
import geopandas as gpd
from folium import DivIcon
import traceback
import matplotlib.pyplot as plt
from app.utils_coordinates import to_latlon_polyline


try:
    from pyproj import CRS, Transformer
    HAS_PROJ = True
except Exception:
    HAS_PROJ = False

from shapely.geometry import Point

DEBUG_MARKERS = os.getenv("RDS_DEBUG_MARKERS", "0") == "1"

WEATHER_STYLE = {
    "radius": 10 if DEBUG_MARKERS else 5,
    "weight": 3 if DEBUG_MARKERS else 1,
    "opacity": 1.0,
    "fillOpacity": 0.9 if DEBUG_MARKERS else 0.6,
    "color": "#00E5FF" if DEBUG_MARKERS else "#1f77b4",
    "fillColor": "#00E5FF" if DEBUG_MARKERS else "#1f77b4",
}
STATION_STYLE = {
    "radius": 10 if DEBUG_MARKERS else 5,
    "weight": 3 if DEBUG_MARKERS else 1,
    "opacity": 1.0,
    "fillOpacity": 0.9 if DEBUG_MARKERS else 0.6,
    "color": "#FF2D95" if DEBUG_MARKERS else "#6a51a3",
    "fillColor": "#FF2D95" if DEBUG_MARKERS else "#6a51a3",
}
if DEBUG_MARKERS:
    WEATHER_STYLE["zIndexOffset"] = 500
    STATION_STYLE["zIndexOffset"] = 500

def first_notna(row, keys):
    for k in keys:
        v = row.get(k, np.nan)
        if pd.notna(v):
            return v
    return None

def get_lat_lon(row):
    lat = first_notna(row, ["lat", "latitude", "lat_dd", "latitude_dd"])
    lon = first_notna(row, ["lon", "longitude", "lon_dd", "longitude_dd"])
    return lat, lon

def fmt_num(x, fmt=".1f"):
    """Safe numeric formatting: returns 'N/A' if x is None/NaN or not castable."""
    try:
        if x is None:
            return "N/A"
        if hasattr(pd, "isna") and pd.isna(x):
            return "N/A"
        if not isinstance(x, (int, float)):
            x = float(x)
        if math.isnan(x):
            return "N/A"
        return format(x, fmt)
    except Exception:
        return "N/A"

def fmt_coord(x):
    """Latitude/longitude display, 4 dp, safe."""
    return fmt_num(x, ".4f")

def _fmt_num(v, prec=5, dash="—"):
    """Safe float formatting for UI: returns '—' if v is None/NaN; else formats to given precision."""
    try:
        if v is None:
            return dash
        f = float(v)
        if math.isnan(f):
            return dash
        return f"{f:.{prec}f}"
    except Exception:
        return dash

def generate_gis_map(alert_row, save_path):
    """
    Generates GIS map showing SARSAT alert locations (A/B), weather stations, range rings, and weather alerts.
    """
    logging.warning(f"ðŸ—ºï¸ generate_gis_map() called â€” context: {traceback.format_stack(limit=3)}")

    site_id = str(alert_row['site_id'])  # âœ… Force site_id to string to avoid int64 serialization issues

    base_data_path = os.getenv('RDS_DATA_FOLDER', 'C:/Users/gehig/Projects/RescueDecisionSystems/data')
    coastline_shapefile = os.path.join(base_data_path, 'shapefiles', 'coastline', 'ne_10m_coastline.shp')

    try:
        gdf_coastline = gpd.read_file(coastline_shapefile)
        logging.info(f"âœ… Loaded coastline shapefile: {coastline_shapefile}")
    except Exception as e:
        log_error_and_continue(f"âš ï¸ Failed to load coastline shapefile: {e}")
        gdf_coastline = None

    center_lat = alert_row['latitude_a'] if pd.notna(alert_row['latitude_a']) else alert_row['latitude_b']
    center_lon = alert_row['longitude_a'] if pd.notna(alert_row['longitude_a']) else alert_row['longitude_b']

    if pd.isna(center_lat) or pd.isna(center_lon):
        logging.warning("âš ï¸ No valid position available for map generation.")
        return None

    m = folium.Map(location=[center_lat, center_lon], zoom_start=6)

    def add_position_marker(lat, lon, range_ring, label):
        if pd.notna(lat) and pd.notna(lon):
            folium.Marker(
                location=[lat, lon],
                popup=f"{label} Location<br>{_fmt_num(lat, 5)}, {_fmt_num(lon, 5)}",
                icon=folium.Icon(color="red", icon="info-sign")
            ).add_to(m)

            folium.map.Marker(
                [lat, lon],
                icon=DivIcon(
                    icon_size=(150, 36),
                    icon_anchor=(0, 0),
                    html=f'<div style="font-size: 14pt; color: red; font-weight: bold">{label}</div>',
                )
            ).add_to(m)

            if range_ring and range_ring > 0:
                folium.Circle(
                    radius=range_ring,
                    location=[lat, lon],
                    color='red',
                    fill=True,
                    fill_opacity=0.2
                ).add_to(m)

    add_position_marker(alert_row['latitude_a'], alert_row['longitude_a'], alert_row['range_ring_meters_a'], "A")
    add_position_marker(alert_row['latitude_b'], alert_row['longitude_b'], alert_row['range_ring_meters_b'], "B")

    combined_weather_stations = []
    if 'nearest_weather_stations_a' in alert_row and alert_row['nearest_weather_stations_a']:
        combined_weather_stations.extend(alert_row['nearest_weather_stations_a'])
    if 'nearest_weather_stations_b' in alert_row and alert_row['nearest_weather_stations_b']:
        combined_weather_stations.extend(alert_row['nearest_weather_stations_b'])

    weather_stations_df = pd.DataFrame(combined_weather_stations)

    if not weather_stations_df.empty:
        for _, station in weather_stations_df.iterrows():
            lat, lon = get_lat_lon(station)
            if lat is None or lon is None:
                continue

            # Weather stations block
            raw_temp_C= first_notna(station, ["temp_C", "temperature", "temp_c"])
            raw_wind_ms = first_notna(station, ["wind_ms", "wind_speed"])
            raw_wave_height_m= first_notna(station, ["wave_m", "wave_height", "wave_height_m"])

            display = format_us_display(wave_height_m=raw_wave_height_m, wind_ms=raw_wind_ms, temp_C=raw_temp_C)
            wave_txt = display.get("wave_height_display", "N/A")
            wind_txt = display.get("wind_display", "N/A")
            temp_txt = display.get("temp_display", "N/A")

            obs_time = station.get("ts_utc") or station.get("obs_time")
            time_txt = ""
            if obs_time:
                tz = derive_local_tz(lat, lon, os.getenv("RDS_OPERATOR_TZ"))
                utc_iso, local_iso = to_dual_time(obs_time, tz)
                time_txt = f"{utc_iso} / {local_iso}"

            timelate = station.get("timelate")
            if pd.isna(timelate):
                timelate = ""

            station_id = str(station.get('station_id', 'Unknown'))
            station_name = str(station.get('station_name', 'N/A'))
            distance_nm = str(station.get('distance_nm', 'N/A'))
            source = str(station.get('source', 'N/A'))
            owner = str(station.get('owner', 'N/A'))
            notes = str(station.get('deployment_notes', 'N/A'))

            popup_content = (
                f"Station: {station_id} ({station_name})<br>"
                f"Temp: {temp_txt}<br>"
                f"Wind: {wind_txt}<br>"
                f"Waves: {wave_txt}<br>"
                f"Distance: {distance_nm} NM<br>"
                f"Timelate (hrs): {timelate}<br>"
                f"Source: {source}<br>"
                f"Owner: {owner}<br>"
                f"deployment_notes: {notes}<br>"
                f"Obs Time: {time_txt}"
            )

            color = 'green' if source == 'shore' else 'blue'

            folium.Marker(
                location=[lat, lon],
                popup=popup_content,
                icon=folium.Icon(color=color, icon="cloud")
            ).add_to(m)

    if 'weather_alerts' in alert_row and alert_row['weather_alerts']:
        weather_alerts_df = pd.DataFrame(alert_row['weather_alerts'])
        if not weather_alerts_df.empty:
            for _, alert in weather_alerts_df.iterrows():
                headline = str(alert.get('headline', 'N/A'))
                event = str(alert.get('event', 'N/A'))
                severity = str(alert.get('severity', 'N/A'))
                certainty = str(alert.get('certainty', 'N/A'))
                effective = str(alert.get("effective", "N/A"))
                expires   = str(alert.get("expires", "N/A"))

                effective_txt = effective
                expires_txt   = expires

                if effective and effective != "N/A":
                    utc_eff, local_eff = to_dual_time(effective, "UTC")
                    effective_txt = f"{utc_eff} / {local_eff}"

                if expires and expires != "N/A":
                    utc_exp, local_exp = to_dual_time(expires, "UTC")
                    expires_txt = f"{utc_exp} / {local_exp}"

                popup = (
                    f"Alert: {headline}<br>"
                    f"Event: {event}<br>"
                    f"Severity: {severity}<br>"
                    f"Certainty: {certainty}<br>"
                    f"Effective: {effective_txt}<br>"
                    f"Expires: {expires_txt}"
                )

                folium.Marker(
                    location=[center_lat, center_lon],
                    popup=popup,
                    icon=folium.Icon(color='orange', icon='exclamation-triangle')
                ).add_to(m)

    if gdf_coastline is not None:
        for _, row in gdf_coastline.iterrows():
            if row.geometry.geom_type == 'LineString':
                coords = [[pt[1], pt[0]] for pt in list(row.geometry.coords)]
                folium.PolyLine(coords, color='black', weight=1).add_to(m)

    m.save(save_path)
    logging.info(f"âœ… Saved GIS map: {save_path}")

    return save_path

def format_timelate(hours):
    if hours is None or (hasattr(pd, "isna") and pd.isna(hours)):
        return "N/A"
    try:
        h = float(hours)
    except Exception:
        return "N/A"
    if h < 1:
        return f"{int(h * 60)} mins"
    return f"{fmt_num(h, '.2f')} hours"

def format_hours(hours):
    if hours is None or (hasattr(pd, "isna") and pd.isna(hours)):
        return "—"
    return f"{fmt_num(hours, '.2f')} hours"

def generate_gis_png(alert_row: pd.Series, out_dir: str) -> dict:
    site_id = str(alert_row.get('site_id', 'unknown'))
    lat_a = alert_row.get('position_lat_dd_a')
    lon_a = alert_row.get('position_lon_dd_a')
    lat_b = alert_row.get('position_lat_dd_b')
    lon_b = alert_row.get('position_lon_dd_b')
    rr_a = alert_row.get('range_ring_meters_a', 0)
    rr_b = alert_row.get('range_ring_meters_b', 0)

    os.makedirs(out_dir, exist_ok=True)
    png_path = os.path.join(out_dir, f"rds_map_{site_id}.png")
    geojson_path = os.path.join(out_dir, f"positions_{site_id}.geojson")

    fig, ax = plt.subplots(figsize=(6, 6))
    points = []
    labels = []
    rings = []

    # Ensure PROJ is ready once per render
    _rds_ensure_proj_ready()

    # Helper for ring
    def plot_ring(lat, lon, radius_m, label):
        try:
            # Try pyproj/CRS logic
            crs_wgs = CRS.from_epsg(4326)
            crs_aeqd = CRS.from_proj4(f"+proj=aeqd +lat_0={lat} +lon_0={lon} +datum=WGS84")
            transformer = Transformer.from_crs(crs_wgs, crs_aeqd, always_xy=True)
            transformer_inv = Transformer.from_crs(crs_aeqd, crs_wgs, always_xy=True)
            x0, y0 = transformer.transform(lon, lat)
            circle = plt.Circle((x0, y0), radius_m, color='red', alpha=0.2, fill=True, lw=1, zorder=1)
            ax.add_patch(circle)
            bounds = [transformer_inv.transform(x0 + radius_m, y0), transformer_inv.transform(x0 - radius_m, y0),
                      transformer_inv.transform(x0, y0 + radius_m), transformer_inv.transform(x0, y0 - radius_m)]
            logging.info(f"[RDS] Range ring for {label} used PROJ/AEQD projection.")
            return bounds
        except Exception as e:
            logging.warning(f"[RDS] pyproj ring failed ({e}); using degree-approx fallback.")
            ring_pts = _rds_ring_lonlat_points(lat, lon, radius_m)
            from matplotlib.patches import Polygon
            poly = Polygon(ring_pts, closed=True, edgecolor='red', facecolor='red', alpha=0.2, lw=1, zorder=1)
            ax.add_patch(poly)
            logging.info(f"[RDS] Range ring for {label} used degree-approximation fallback.")
            return ring_pts

    # Plot Position A
    if pd.notna(lat_a) and pd.notna(lon_a):
        ax.plot(lon_a, lat_a, 'ro', markersize=8, zorder=2)
        ax.text(lon_a, lat_a, 'A', color='red', fontsize=12, fontweight='bold', ha='left', va='bottom', zorder=3)
        points.append(Point(lon_a, lat_a))
        labels.append('A')
        if rr_a and rr_a > 0:
            rings += plot_ring(lat_a, lon_a, rr_a, 'A')

    # Plot Position B
    if pd.notna(lat_b) and pd.notna(lon_b):
        ax.plot(lon_b, lat_b, 'ro', markersize=8, zorder=2)
        ax.text(lon_b, lat_b, 'B', color='red', fontsize=12, fontweight='bold', ha='left', va='bottom', zorder=3)
        points.append(Point(lon_b, lat_b))
        labels.append('B')
        if rr_b and rr_b > 0:
            rings += plot_ring(lat_b, lon_b, rr_b, 'B')

    # Set extent
    all_lats = [lat for lat in [lat_a, lat_b] if pd.notna(lat)]
    all_lons = [lon for lon in [lon_a, lon_b] if pd.notna(lon)]
    if all_lats and all_lons:
        min_lat, max_lat = min(all_lats), max(all_lats)
        min_lon, max_lon = min(all_lons), max(all_lons)
        pad_lat = max(0.01, (max_lat - min_lat) * 0.2)
        pad_lon = max(0.01, (max_lon - min_lon) * 0.2)
        ax.set_xlim(min_lon - pad_lon, max_lon + pad_lon)
        ax.set_ylim(min_lat - pad_lat, max_lat + pad_lat)
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.set_title(f"RDS Alert Map: {site_id}")
    plt.tight_layout()
    plt.savefig(png_path, dpi=150)
    plt.close(fig)
    if logging.getLogger().hasHandlers():
        logging.info(f"âœ… Map image saved: {png_path}")
    else:
        print(f"âœ… Map image saved: {png_path}")

    # Optionally save GeoJSON
    geojson_written = False
    geojson_path_out = None
    if points:
        try:
            if HAS_PROJ:
                gdf = gpd.GeoDataFrame({
                    'label': labels,
                    'range_ring_meters': [rr_a if l == 'A' else rr_b for l in labels]
                }, geometry=points, crs="EPSG:4326")
                gdf.to_file(geojson_path, driver='GeoJSON')
                geojson_written = True
                geojson_path_out = geojson_path
                if logging.getLogger().hasHandlers():
                    logging.info(f"âœ… Positions GeoJSON saved: {geojson_path}")
                else:
                    print(f"âœ… Positions GeoJSON saved: {geojson_path}")
        except Exception as e:
            if logging.getLogger().hasHandlers():
                logging.warning(f"GeoJSON write failed: {e}")
            else:
                print(f"GeoJSON write failed: {e}")

    return {
        "png_path": png_path,
        "geojson_path": geojson_path_out,
        "site_id": site_id
    }

def generate_gis_map_html(alert_row, out_dir, tiles_mode="online"):
    """
    Generates an interactive HTML map with online tiles using Folium.
    Plots A/B positions (supports both legacy and new field names), draws red markers and meter rings.
    Saves to data/maps/<site_id>/gis_map_<site_id>.html
    """
    import folium
    import os
    import pandas as pd
    from shapely.geometry import Point

    # Support both legacy and new field names
    site_id = str(alert_row.get('site_id', 'unknown'))
    lat_a = alert_row.get('position_lat_dd_a', alert_row.get('latitude_a'))
    lon_a = alert_row.get('position_lon_dd_a', alert_row.get('longitude_a'))
    lat_b = alert_row.get('position_lat_dd_b', alert_row.get('latitude_b'))
    lon_b = alert_row.get('position_lon_dd_b', alert_row.get('longitude_b'))
    rr_a = alert_row.get('range_ring_meters_a', 0)
    rr_b = alert_row.get('range_ring_meters_b', 0)

    os.makedirs(out_dir, exist_ok=True)
    html_path = os.path.join(out_dir, f"gis_map_{site_id}.html")

    # Center map on A if present, else B
    center_lat = lat_a if pd.notna(lat_a) else lat_b
    center_lon = lon_a if pd.notna(lon_a) else lon_b
    m = folium.Map(location=[center_lat, center_lon], zoom_start=8, tiles="OpenStreetMap" if tiles_mode=="online" else None)

    # Plot Position A
    if pd.notna(lat_a) and pd.notna(lon_a):
        folium.Marker([lat_a, lon_a], popup="A", icon=folium.Icon(color="red")).add_to(m)
        if rr_a and rr_a > 0:
            folium.Circle([lat_a, lon_a], radius=rr_a, color="red", fill=True, fill_opacity=0.2, weight=1, popup="A ring").add_to(m)

    # Plot Position B
    if pd.notna(lat_b) and pd.notna(lon_b):
        folium.Marker([lat_b, lon_b], popup="B", icon=folium.Icon(color="red")).add_to(m)
        if rr_b and rr_b > 0:
            folium.Circle([lat_b, lon_b], radius=rr_b, color="red", fill=True, fill_opacity=0.2, weight=1, popup="B ring").add_to(m)

    m.save(html_path)
    if logging.getLogger().hasHandlers():
        logging.info(f"âœ… Interactive map saved: {html_path}")
    else:
        print(f"âœ… Interactive map saved: {html_path}")
    return html_path

# --- RDS: PROJ datadir helper (Windows/conda) ---
import os, sys, logging, math
from typing import Optional

try:
    from pyproj import CRS, datadir, network
except Exception:
    pass

def _rds_ensure_proj_ready() -> Optional[str]:
    """
    Ensure pyproj has a valid PROJ database available.
    Returns the resolved PROJ data directory or None if unresolved.
    """
    try:
        cur = getattr(datadir, "get_data_dir", lambda: None)()
        if cur and os.path.exists(os.path.join(cur, "proj.db")):
            try:
                network.set_network_enabled(False)
            except Exception:
                pass
            return cur
        candidates = []
        conda_prefix = os.environ.get("CONDA_PREFIX")
        if conda_prefix:
            candidates.append(os.path.join(conda_prefix, "Library", "share", "proj"))
        candidates.extend([
            r"C:\Users\gehig\anaconda3\envs\unified_env\Library\share\proj",
            r"C:\Users\gehig\anaconda3\Library\share\proj",
            r"C:\ProgramData\Anaconda3\Library\share\proj",
        ])
        for c in candidates:
            if os.path.exists(os.path.join(c, "proj.db")):
                try:
                    datadir.set_data_dir(c)
                    try:
                        network.set_network_enabled(False)
                    except Exception:
                        pass
                    logging.info(f"[RDS] PROJ data dir set: {c}")
                    return c
                except Exception:
                    continue
    except Exception as e:
        logging.warning(f"[RDS] Failed to set PROJ data dir: {e}")
    logging.warning("[RDS] PROJ data dir not resolved; GIS will use degree-approximation fallback for rings.")
    return None

def _rds_ring_lonlat_points(lat_deg: float, lon_deg: float, radius_m: float, n: int = 180):
    """
    Degree-approximation ring (fallback when pyproj CRS fails).
    Returns list of (lon, lat) points.
    """
    lat_per_m = 1.0 / 111_320.0
    lon_per_m = 1.0 / (111_320.0 * max(0.1, math.cos(math.radians(lat_deg))))
    pts = []
    for k in range(n):
        ang = (2.0 * math.pi * k) / n
        dlat = radius_m * lat_per_m * math.sin(ang)
        dlon = radius_m * lon_per_m * math.cos(ang)
        pts.append((lon_deg + dlon, lat_deg + dlat))
    pts.append(pts[0])
    return pts
# --- RDS: end PROJ datadir helper ---

# [RDS-ANCHOR: GIS_EXPORTS]
def generate_gis_map_html_from_dfs(gis_map_inputs_df, alert_row, out_path, tiles_mode="online"):
    import folium, os, pandas as pd, logging
    from folium import LayerControl, DivIcon, FeatureGroup, PolyLine, Marker, Circle

    # --- Center/Meta ---
    site_id = str((alert_row or {}).get("site_id", "unknown"))
    lat0 = (alert_row or {}).get("position_lat_dd_a") or (alert_row or {}).get("alert_lat_dd")
    lon0 = (alert_row or {}).get("position_lon_dd_a") or (alert_row or {}).get("alert_lon_dd")
    if pd.isna(lat0) or pd.isna(lon0):
        lat0, lon0 = 38.255, -70.208333  # safe default

    m = folium.Map(location=[float(lat0), float(lon0)], zoom_start=7, tiles="OpenStreetMap" if tiles_mode=="online" else None)

    # --- Alert Positions (A/B) ---
    ab_positions = gis_map_inputs_df[gis_map_inputs_df["layer"] == "alert_position"]
    for _, row in ab_positions.iterrows():
        g = row.get("geometry", {})
        coords = g.get("coordinates", [None, None]) if isinstance(g, dict) else [None, None]
        lat_dd = coords[1]; lon_dd = coords[0]
        label = row.get("label", "")
        if lat_dd is None or lon_dd is None or pd.isna(lat_dd) or pd.isna(lon_dd):
            continue
        popup = f"{label} Location<br>{_fmt_num(lat_dd, 5)}, {_fmt_num(lon_dd, 5)}"
        folium.Marker([lat_dd, lon_dd], popup=popup, icon=folium.Icon(color="red", icon="info-sign")).add_to(m)
        folium.map.Marker([lat_dd, lon_dd], icon=DivIcon(icon_size=(150, 36), icon_anchor=(0, 0),
                              html=f'<div style="font-size: 14pt; color: red; font-weight: bold">{label}</div>')).add_to(m)

    # --- Range Rings ---
    rings = gis_map_inputs_df[gis_map_inputs_df["layer"] == "range_ring"]
    for _, row in rings.iterrows():
        g = row.get("geometry", {})
        center = g.get("center") if isinstance(g, dict) else None
        rad_m = g.get("radius_m") if isinstance(g, dict) else None
        if (isinstance(center, (list, tuple)) and len(center) == 2
                and center[0] is not None and center[1] is not None
                and rad_m and rad_m > 0):
            folium.Circle(location=[center[1], center[0]], radius=float(rad_m),
                          color="red", fill=False, weight=2,
                          tooltip=f"{row.get('label','')} — EE95 Ring").add_to(m)

    # --- Weather Layer ---
    wx_rows = gis_map_inputs_df[gis_map_inputs_df["layer"] == "weather"]
    if not wx_rows.empty:
        wx_group = folium.FeatureGroup(name="Weather", show=True)
        for _, row in wx_rows.iterrows():
            g = row.get("geometry", {})
            coords = g.get("coordinates", [None, None]) if isinstance(g, dict) else [None, None]
            lat, lon = coords[1], coords[0]
            if lat is None or lon is None or pd.isna(lat) or pd.isna(lon):
                continue
            wave = row.get('wave_height_display', 'None')
            wind = row.get('wind_display', 'None')
            temp = row.get('temp_display', 'None')
            popup_html = "<br>".join([
                f"<b>Weather</b>",
                f"Lat: {_fmt_num(lat, 5)}",
                f"Lon: {_fmt_num(lon, 5)}",
                f"Temp: {temp}",
                f"Wind: {wind}",
                f"Waves: {wave}",
            ])
            folium.CircleMarker(location=[lat, lon],
                                radius=WEATHER_STYLE["radius"],
                                color=WEATHER_STYLE["color"],
                                fill=True, fill_color=WEATHER_STYLE["fillColor"],
                                fill_opacity=WEATHER_STYLE["fillOpacity"],
                                weight=WEATHER_STYLE["weight"],
                                popup=popup_html).add_to(wx_group)
        wx_group.add_to(m)

    # --- Stations Layer ---
    st_rows = gis_map_inputs_df[gis_map_inputs_df["layer"] == "station"]
    if not st_rows.empty:
        st_group = folium.FeatureGroup(name="Stations", show=True)
        for _, st in st_rows.iterrows():
            g = st.get("geometry", {})
            coords = g.get("coordinates", [None, None]) if isinstance(g, dict) else [None, None]
            lat, lon = coords[1], coords[0]
            if lat is None or lon is None or pd.isna(lat) or pd.isna(lon):
                continue
            wave = st.get('wave_height_display', 'None')
            wind = st.get('wind_display', 'None')
            temp = st.get('temp_display', 'None')
            popup = "<br>".join([
                f"<b>Station</b> {st.get('source_id', '')} — {st.get('label','N/A')}",
                f"Temp: {temp}",
                f"Wind: {wind}",
                f"Waves: {wave}",
                f"Lat: {_fmt_num(lat, 5)}",
                f"Lon: {_fmt_num(lon, 5)}"
            ])
            folium.CircleMarker(location=[lat, lon],
                                radius=STATION_STYLE["radius"],
                                color=STATION_STYLE["color"],
                                fill=True, fill_color=STATION_STYLE["fillColor"],
                                fill_opacity=STATION_STYLE["fillOpacity"],
                                weight=STATION_STYLE["weight"],
                                popup=popup).add_to(st_group)
        st_group.add_to(m)

    # --- Satellite Overlays (footprints, tracks, next-pass) ---
    sat_rows = gis_map_inputs_df[gis_map_inputs_df["layer"] == "satellite_overlay"]
    if not sat_rows.empty:
        fg_foot = folium.FeatureGroup(name="Satellite footprints", show=True)
        fg_track = folium.FeatureGroup(name="Satellite tracks", show=True)
        fg_next  = folium.FeatureGroup(name="Next-pass markers", show=True)

        for _, sat in sat_rows.iterrows():
            geom = sat.get("geometry", {})
            if not isinstance(geom, dict):
                continue
            gtype = geom.get("type")

            # Circle (footprint)
            if gtype == "Circle":
                center = geom.get("center")
                rad_m  = geom.get("radius_m")
                if (isinstance(center, (list, tuple)) and len(center) == 2
                        and pd.notna(center[0]) and pd.notna(center[1]) and rad_m and rad_m > 0):
                    folium.Circle(location=[float(center[1]), float(center[0])],
                                  radius=float(rad_m),
                                  color="#0b84f3", weight=1, fill=True, fill_opacity=0.15,
                                  tooltip=sat.get("label","")).add_to(fg_foot)

            # LineString (track)
            elif gtype == "LineString":
                coords = geom.get("coordinates") or []
                clean = []
                for pt in coords:
                    try:
                        lon, lat = pt
                        if pd.notna(lon) and pd.notna(lat):
                            clean.append([float(lat), float(lon)])  # folium = [lat, lon]
                    except Exception:
                        continue
                if len(clean) > 1:
                    PolyLine(locations=clean, weight=2, opacity=0.6, dash_array="4,6").add_to(fg_track)

            # Point (next-pass)
            elif gtype == "Point":
                coords = geom.get("coordinates", [None, None])
                if len(coords) == 2 and pd.notna(coords[0]) and pd.notna(coords[1]):
                    Marker(location=[float(coords[1]), float(coords[0])],
                           tooltip=str(sat.get("label","Next pass"))).add_to(fg_next)

        fg_foot.add_to(m); fg_track.add_to(m); fg_next.add_to(m)

        from folium import LayerControl
        LayerControl(collapsed=True).add_to(m)

    # --- Title & Save ---
    title_html = f'''<h3 align="center" style="font-size:18px"><b>RDS Alert Map: {site_id}</b></h3>'''
    m.get_root().html.add_child(folium.Element(title_html))
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    m.save(out_path)
    logging.info(f"✅ DF-based HTML map saved: {out_path}")
    return {"site_id": site_id, "map_html_path": out_path, "status": "ok"}




