# gis_plot.py

import folium
import logging
from app.weather_fetch import fetch_nearest_weather_stations, fetch_weather_data
from app.distance_calc import compute_distance_to_shore
from shapely.geometry import Point

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def generate_gis_map(df_alert):
    """Creates GIS map showing alert, weather stations, and nearest shore distances."""
    
    latitude = df_alert.iloc[0]["latitude"]
    longitude = df_alert.iloc[0]["longitude"]
    site_id = df_alert.iloc[0]["site_id"]
    
    # ✅ Use stored weather station data
    df_weather = pd.DataFrame(df_alert.iloc[0]["nearest_weather_stations"])

    # ✅ Create Map
    m = folium.Map(location=[latitude, longitude], zoom_start=6)

    # ✅ Add Alert Location
    folium.Marker(
        location=[latitude, longitude],
        popup="Alert Location",
        icon=folium.Icon(color="red", icon="info-sign")
    ).add_to(m)

    # ✅ Add Weather Stations
    for _, row in df_weather.iterrows():
        folium.Marker(
            location=[row["latitude"], row["longitude"]],
            popup=f"Station: {row['station_id']}<br>Wind: {row.get('wind_speed', 'N/A')} m/s<br>Temp: {row.get('temperature', 'N/A')}°C",
            icon=folium.Icon(color="blue", icon="cloud")
        ).add_to(m)

    # ✅ Save Map
    map_filename = f"maps/gis_map_{site_id}.html"
    m.save(map_filename)
    
    return map_filename
