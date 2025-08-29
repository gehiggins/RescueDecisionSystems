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
