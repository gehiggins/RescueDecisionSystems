#gis_plot.py

import os
import folium
import pandas as pd
from geopy.distance import geodesic  # ✅ Used for distance calculations
from app.weather_fetch import fetch_nearest_weather_stations
from app.distance_calc import compute_distance_to_shore
from app.utils import convert_lat_lon_to_decimal, convert_km_to_miles

def generate_gis_map(latitude, longitude, site_id):
    """Creates a GIS-based image showing alert location, nearest weather stations, and satellite ground tracks."""
    
    print(f"🔄 Generating GIS Map for {latitude}, {longitude}, Site ID: {site_id}")
    
    # ✅ Convert lat/lon to decimal degrees if needed
    latitude, longitude = convert_lat_lon_to_decimal(latitude, longitude)
    
    # ✅ Compute distance to shore
    distance_to_shore = compute_distance_to_shore(latitude, longitude)
    print(f"✅ Distance to shore: {distance_to_shore:.2f} km")
    
    # ✅ Step 1: Create a Base Map Centered on the Alert Location
    m = folium.Map(location=[latitude, longitude], zoom_start=6)
    
    # ✅ Step 2: Plot the Alert Location
    folium.Marker(
        location=[latitude, longitude],
        popup="Alert Location",
        icon=folium.Icon(color="red", icon="info-sign")
    ).add_to(m)
    
    # ✅ Step 3: Fetch Nearest Weather Stations
    df_weather = fetch_nearest_weather_stations(latitude, longitude, distance_to_shore)
    
    if not df_weather.empty:
        for _, station in df_weather.iterrows():
            distance_miles = convert_km_to_miles(station['distance_km'])
            folium.Marker(
                location=[station['latitude'], station['longitude']],
                popup=f"Weather Station: {station['station_id']}<br>"
                      f"🌡 Temp: {station.get('temperature', 'N/A')}°C<br>"
                      f"💨 Wind: {station.get('wind_speed', 'N/A')} m/s {station.get('wind_direction', 'N/A')}°<br>"
                      f"🌊 Waves: {station.get('wave_height', 'N/A')} m<br>"
                      f"☔ Rain: {station.get('precipitation', 'N/A')} mm<br>"
                      f"🌍 Distance: {distance_miles} miles", 
                icon=folium.Icon(color="blue", icon="cloud")
            ).add_to(m)
    
    # ✅ Step 4: Save the Map as an HTML File
    maps_dir = os.path.join(os.getcwd(), "maps")
    os.makedirs(maps_dir, exist_ok=True)
    map_filename = os.path.join(maps_dir, f"gis_map_{site_id}.html")
    m.save(map_filename)
    
    print(f"✅ GIS Map saved successfully: {map_filename}")
    
    return map_filename
