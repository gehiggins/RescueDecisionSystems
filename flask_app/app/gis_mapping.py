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


from flask_app.setup_imports import *
from flask_app.app.utils import log_error_and_continue
from flask_app.app.utils_weather import celsius_to_fahrenheit
import folium
import geopandas as gpd
import os
import pandas as pd
from folium import DivIcon
import traceback
import math
import matplotlib.pyplot as plt
from shapely.geometry import Point
from pyproj import CRS, Transformer

try:
    import geopandas as gpd
    from shapely.geometry import Point
    from pyproj import CRS, Transformer
    HAS_PROJ = True
except ImportError:
    HAS_PROJ = False
    logging.warning("pyproj/geopandas not available; using lat/lon approximation for range rings.")

def generate_gis_map(alert_row, save_path):
    """
    Generates GIS map showing SARSAT alert locations (A/B), weather stations, range rings, and weather alerts.
    """
    logging.warning(f"🗺️ generate_gis_map() called — context: {traceback.format_stack(limit=3)}")

    site_id = str(alert_row['site_id'])  # ✅ Force site_id to string to avoid int64 serialization issues

    base_data_path = os.getenv('RDS_DATA_FOLDER', 'C:/Users/gehig/Projects/RescueDecisionSystems/data')
    coastline_shapefile = os.path.join(base_data_path, 'shapefiles', 'coastline', 'ne_10m_coastline.shp')

    try:
        gdf_coastline = gpd.read_file(coastline_shapefile)
        logging.info(f"✅ Loaded coastline shapefile: {coastline_shapefile}")
    except Exception as e:
        log_error_and_continue(f"⚠️ Failed to load coastline shapefile: {e}")
        gdf_coastline = None

    center_lat = alert_row['latitude_a'] if pd.notna(alert_row['latitude_a']) else alert_row['latitude_b']
    center_lon = alert_row['longitude_a'] if pd.notna(alert_row['longitude_a']) else alert_row['longitude_b']

    if pd.isna(center_lat) or pd.isna(center_lon):
        logging.warning("⚠️ No valid position available for map generation.")
        return None

    m = folium.Map(location=[center_lat, center_lon], zoom_start=6)

    def add_position_marker(lat, lon, range_ring, label):
        if pd.notna(lat) and pd.notna(lon):
            folium.Marker(
                location=[lat, lon],
                popup=f"{label} Location<br>{lat:.5f}, {lon:.5f}",
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
            station_id = str(station.get('station_id', 'Unknown'))
            station_name = str(station.get('station_name', 'N/A'))

            raw_temp_c = station.get('temperature', np.nan)

            if isinstance(raw_temp_c, (int, float, np.number)) and pd.notna(raw_temp_c):
                temp_c = f"{raw_temp_c:.1f}"
                temp_f = f"{celsius_to_fahrenheit(raw_temp_c):.1f}"
            else:
                temp_c = "N/A"
                temp_f = "N/A"

            wind_speed = str(station.get('wind_speed', 'N/A'))
            wave_height = str(station.get('wave_height', 'N/A'))
            timelate = str(format_timelate(station.get('timelate', np.nan)))
            distance_nm = str(station.get('distance_nm', 'N/A'))
            source = str(station.get('source', 'N/A'))
            owner = str(station.get('owner', 'N/A'))
            notes = str(station.get('deployment_notes', 'N/A'))

            popup_content = (
                f"Station: {station_id} ({station_name})<br>"
                f"Temp: {temp_c}°C / {temp_f}°F<br>"
                f"Wind: {wind_speed} m/s<br>"
                f"Waves: {wave_height} m<br>"
                f"Distance: {distance_nm} NM<br>"
                f"Timelate (hrs): {timelate}<br>"
                f"Source: {source}<br>"
                f"Owner: {owner}<br>"
                f"Notes: {notes}"
            )

            color = 'green' if source == 'shore' else 'blue'

            folium.Marker(
                location=[station['latitude'], station['longitude']],
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
                effective = str(alert.get('effective', 'N/A'))
                expires = str(alert.get('expires', 'N/A'))

                popup = (
                    f"Alert: {headline}<br>"
                    f"Event: {event}<br>"
                    f"Severity: {severity}<br>"
                    f"Certainty: {certainty}<br>"
                    f"Effective: {effective}<br>"
                    f"Expires: {expires}"
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
    logging.info(f"✅ Saved GIS map: {save_path}")

    return save_path

def format_timelate(hours):
    if pd.isna(hours):
        return "N/A"
    elif hours < 1:
        return f"{int(hours * 60)} mins"
    else:
        return f"{hours:.2f} hours"

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
        logging.info(f"✅ Map image saved: {png_path}")
    else:
        print(f"✅ Map image saved: {png_path}")

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
                    logging.info(f"✅ Positions GeoJSON saved: {geojson_path}")
                else:
                    print(f"✅ Positions GeoJSON saved: {geojson_path}")
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
        logging.info(f"✅ Interactive map saved: {html_path}")
    else:
        print(f"✅ Interactive map saved: {html_path}")
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
def generate_gis_map_html_from_dfs(positions_df, out_dir, *, site_id=None, wx_df=None, stations_df=None, tiles_mode="online") -> dict:
    """
    Generates a Folium HTML map from DataFrames of alert positions, weather, and stations.
    Inputs:
        positions_df: DataFrame with required columns [site_id, role (A/B), lat_dd, lon_dd, range_ring_meters]
        out_dir: Output directory (created if missing)
        site_id: Optional; if None, derived from positions_df['site_id'] (must be unique)
        wx_df: Optional weather DataFrame
        stations_df: Optional stations DataFrame
        tiles_mode: "online" for OpenStreetMap tiles
    Returns:
        dict: {"site_id": str, "map_html_path": str, "layers": [layer names]}
    """
    import folium
    import os
    import pandas as pd
    import logging
    from datetime import datetime
    from folium import LayerControl, DivIcon

    layers = []
    # Validate positions_df
    if positions_df is None or positions_df.empty:
        logging.warning("[RDS] No positions_df provided; cannot generate map.")
        return {"site_id": None, "map_html_path": None, "layers": [], "status": "no-op: empty positions_df"}

    # Filter for valid A/B positions
    valid_roles = positions_df[positions_df["role"].isin(["A", "B"])]
    valid_positions = valid_roles[pd.notna(valid_roles["lat_dd"]) & pd.notna(valid_roles["lon_dd"])]

    # Site ID logic
    sid = site_id
    if not sid:
        unique_ids = positions_df["site_id"].dropna().unique()
        if len(unique_ids) == 1:
            sid = str(unique_ids[0])
        elif len(unique_ids) > 1:
            sid = str(unique_ids[0])
            logging.warning(f"[RDS] Multiple site_ids in positions_df; using first: {sid}")
        else:
            sid = f"SMOKE_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

    # Directory logic
    out_dir_final = os.path.join(out_dir, str(sid)) if out_dir and not out_dir.endswith(str(sid)) else out_dir
    os.makedirs(out_dir_final, exist_ok=True)
    html_path = os.path.join(out_dir_final, f"gis_map_{sid}.html")

    # Safety: must have at least one valid A or B
    ab_positions = valid_positions[valid_positions["role"].isin(["A", "B"])]
    if ab_positions.empty:
        logging.warning(f"[RDS] No valid A/B positions for site_id {sid}; map not written.")
        return {"site_id": sid, "map_html_path": None, "layers": [], "status": "no-op: no valid A/B"}

    # Center/zoom: use A if present, else B
    center_row = ab_positions[ab_positions["role"] == "A"]
    if not center_row.empty:
        center_lat = center_row.iloc[0]["lat_dd"]
        center_lon = center_row.iloc[0]["lon_dd"]
    else:
        center_lat = ab_positions.iloc[0]["lat_dd"]
        center_lon = ab_positions.iloc[0]["lon_dd"]

    m = folium.Map(location=[center_lat, center_lon], zoom_start=8, tiles="OpenStreetMap" if tiles_mode=="online" else None)

    # --- Alert Positions Layer ---
    alert_group = folium.FeatureGroup(name="Alert Positions", show=True)
    for _, row in ab_positions.iterrows():
            label = row["role"]
            lat = row["lat_dd"]
            lon = row["lon_dd"]
            ring = row.get("range_ring_meters", 0)
            # --- Format radius strings ---
            radius_m_str = f"{int(round(ring)):,} m" if ring and ring > 0 else "N/A"
            radius_nm = round(ring / 1852, 2) if ring and ring > 0 else None
            radius_nm_str = f"{radius_nm} NM" if radius_nm is not None else "N/A"
            # --- Provenance label ---
            ring_src = row.get("range_ring_source", None)
            if ring_src == "EE_95":
                provenance = "95% confidence (EE)"
            elif ring_src == "fallback_gnss":
                provenance = "default radius (GNSS resolution; not 95%)"
            else:
                provenance = "default radius (not 95%)"
            # --- Popup fields ---
            popup_fields = [
                f"Role: {label}",
                f"Lat/Lon: {lat:.5f}, {lon:.5f}",
                f"Range ring: {radius_m_str} (~{radius_nm_str})",
                provenance
            ]
            # If EE_95 and ee_nm present, add EE value
            if ring_src == "EE_95" and "ee_nm" in row and pd.notna(row["ee_nm"]):
                popup_fields.append(f"EE: {row['ee_nm']} NM (95%)")
            # Existing optional fields
            for opt in ["position_status", "method", "confidence", "expected_error_nm"]:
                if opt in row and pd.notna(row[opt]):
                    popup_fields.append(f"{opt}: {row[opt]}")
            popup = "<br>".join(popup_fields)
            folium.Marker(
                [lat, lon],
                popup=popup,
                icon=folium.Icon(color="red", icon="info-sign")
            ).add_to(alert_group)
            folium.map.Marker(
                [lat, lon],
                icon=DivIcon(
                    icon_size=(150, 36),
                    icon_anchor=(0, 0),
                    html=f'<div style="font-size: 14pt; color: red; font-weight: bold">{label}</div>',
                )
            ).add_to(alert_group)
    alert_group.add_to(m)
    layers.append("Alert Positions")

    # --- Range Rings Layer ---
    ring_group = folium.FeatureGroup(name="Range Rings", show=True)
    for _, row in ab_positions.iterrows():
            lat = row["lat_dd"]
            lon = row["lon_dd"]
            ring = row.get("range_ring_meters", 0)
            label = row["role"]
            ring_src = row.get("range_ring_source", None)
            if ring and ring > 0:
                # Tooltip: "A — 95% confidence (EE)" or "A — default radius (not 95%)"
                if ring_src == "EE_95":
                    tooltip = f"{label} — 95% confidence (EE)"
                elif ring_src == "fallback_gnss":
                    tooltip = f"{label} — default radius (GNSS resolution; not 95%)"
                else:
                    tooltip = f"{label} — default radius (not 95%)"
                folium.Circle(
                    location=[lat, lon],
                    radius=ring,
                    color="red",
                    fill=False,
                    weight=2,
                    tooltip=tooltip
                ).add_to(ring_group)
    ring_group.add_to(m)
    layers.append("Range Rings")

    # --- Weather Layer ---
    if wx_df is not None and not wx_df.empty:
        wx_group = folium.FeatureGroup(name="Weather", show=True)
        for _, wx in wx_df.iterrows():
            lat = wx.get("lat_dd")
            lon = wx.get("lon_dd")
            if pd.notna(lat) and pd.notna(lon):
                popup = "<br>".join([
                    f"Obs: {wx.get('obs_type', 'N/A')}",
                    f"Value: {wx.get('obs_value', 'N/A')} {wx.get('obs_unit', '')}",
                    f"Time: {wx.get('obs_time', 'N/A')}",
                    f"Station: {wx.get('station_id', 'N/A')}"
                ])
                folium.Marker(
                    [lat, lon],
                    popup=popup,
                    icon=folium.Icon(color="orange", icon="cloud")
                ).add_to(wx_group)
        wx_group.add_to(m)
        layers.append("Weather")

    # --- Stations Layer ---
    if stations_df is not None and not stations_df.empty:
        st_group = folium.FeatureGroup(name="Stations", show=True)
        for _, st in stations_df.iterrows():
            lat = st.get("lat_dd")
            lon = st.get("lon_dd")
            if pd.notna(lat) and pd.notna(lon):
                popup = "<br>".join([
                    f"ID: {st.get('station_id', 'N/A')}",
                    f"Name: {st.get('name', 'N/A')}",
                    f"Type: {st.get('type', 'N/A')}"
                ])
                folium.Marker(
                    [lat, lon],
                    popup=popup,
                    icon=folium.Icon(color="blue", icon="cloud")
                ).add_to(st_group)
        st_group.add_to(m)
        layers.append("Stations")

    # --- Layer Control ---
    LayerControl(collapsed=True).add_to(m)

    # --- Title ---
    title_html = f'''<h3 align="center" style="font-size:18px"><b>RDS Alert Map: {sid}</b></h3>'''
    m.get_root().html.add_child(folium.Element(title_html))

    # --- Fit map to largest ring ---
    max_ring = ab_positions["range_ring_meters"].max() if "range_ring_meters" in ab_positions else 0
    if max_ring and max_ring > 0:
        # Pad by 20%
        pad = max_ring * 1.2
        m.fit_bounds([
            [center_lat - pad/111320, center_lon - pad/(111320*max(0.1, math.cos(math.radians(center_lat))))],
            [center_lat + pad/111320, center_lon + pad/(111320*max(0.1, math.cos(math.radians(center_lat))))]
        ])

    # --- Save ---
    m.save(html_path)
    logging.info(f"✅ DF-based HTML map saved: {html_path}")
    return {"site_id": sid, "map_html_path": html_path, "layers": layers, "status": "ok"}
