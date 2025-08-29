# weather_fetch.py

import requests
import pandas as pd
from geopy.distance import geodesic
from app.utils import convert_km_to_miles

def fetch_nearest_weather_stations(latitude, longitude, distance_to_shore):
    """Fetches the nearest NOAA/NDBC weather stations to the given coordinates."""
    print("🔍 Fetching nearest weather stations...")
    
    # ✅ NOAA API for nearest stations
    stations_url = f"https://api.weather.gov/points/{latitude},{longitude}"
    headers = {"User-Agent": "RescueDecisionSystems.com, RescueDecisionSystems@outlook.com"}
    
    try:
        response = requests.get(stations_url, headers=headers).json()
        
        # ✅ Extract observation stations list
        stations_list_url = response.get("properties", {}).get("observationStations")
        if not stations_list_url:
            print("🚨 No observation stations found in response!")
            return pd.DataFrame()
        
        # ✅ Fetch station details
        station_data = []
        stations = requests.get(stations_list_url, headers=headers).json().get("features", [])[:5]  # Limit to 5 stations
        
        for station in stations:
            station_name = station["properties"]["name"]
            station_lat = station["geometry"]["coordinates"][1]
            station_lon = station["geometry"]["coordinates"][0]
            station_dist_km = geodesic((latitude, longitude), (station_lat, station_lon)).km

            station_data.append({
                "station_id": station_name,
                "latitude": station_lat,
                "longitude": station_lon,
                "distance_km": station_dist_km,
                "type": "land" if distance_to_shore < 10 else "buoy"  # ✅ Classify as land/buoy
            })
        
        df_weather = pd.DataFrame(station_data)
        print(f"✅ Found {len(df_weather)} stations.")
        return df_weather

    except Exception as e:
        print(f"🚨 ERROR Fetching Weather Stations: {e}")
        return pd.DataFrame()


def fetch_weather_data(station_url):
    """Fetches the latest weather data from a given NOAA station URL."""
    print(f"🔍 Fetching weather data for {station_url}...")
    
    try:
        headers = {"User-Agent": "RescueDecisionSystems.com, RescueDecisionSystems@outlook.com"}
        response = requests.get(f"{station_url}/observations/latest", headers=headers).json()
        
        properties = response.get("properties", {})
        if not properties:
            print(f"🚨 No valid weather data found for {station_url}")
            return {}

        # ✅ Extract key weather data
        return {
            "station": station_url,
            "name": station_url,
            "temperature": properties.get("temperature", {}).get("value", "N/A"),
            "wind_speed": properties.get("windSpeed", {}).get("value", "N/A"),
            "wind_direction": properties.get("windDirection", {}).get("value", "N/A"),
            "wave_height": properties.get("waveHeight", {}).get("value", "N/A"),
            "precipitation": properties.get("precipitationLastHour", {}).get("value", "N/A"),
            "timestamp": properties.get("timestamp", "N/A")
        }
    
    except Exception as e:
        print(f"🚨 ERROR Fetching Weather Data: {e}")
        return {}
