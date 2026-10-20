import json

import requests

# NOAA API requires a User-Agent
HEADERS = {
    "User-Agent": "RescueDecisionSystems.com (RescueDecisionSystems@outlook.com)"
}

# Test Location (Offshore in the Gulf of Mexico)
latitude = 36.70  # Approx. location of NOAA buoy 42001
longitude = -75.67

def fetch_weather_stations(lat, lon):
    """Fetch nearest NOAA weather stations (land & marine)."""
    try:
        url = f"https://api.weather.gov/points/{lat},{lon}"
        response = requests.get(url, headers=HEADERS, timeout=10)

        if response.status_code != 200:
            print(f"🚨 ERROR: Received status code {response.status_code}")
            print(response.text)
            return

        data = response.json()
        station_url = data.get("properties", {}).get("observationStations", None)

        if not station_url:
            print("❌ ERROR: No observation stations found.")
            return

        # Fetch nearest stations
        station_response = requests.get(station_url, headers=HEADERS, timeout=10)
        if station_response.status_code != 200:
            print(f"🚨 ERROR: Could not retrieve stations. Code {station_response.status_code}")
            return

        stations = station_response.json().get("observationStations", [])
        if not stations:
            print("⚠️ No weather stations found.")
            return

        print(f"✅ Found {len(stations)} stations. Fetching weather data for first station...")

        # Fetch weather data for the first station (could be buoy or land-based)
        first_station_url = stations[0]
        weather_response = requests.get(first_station_url + "/observations/latest", headers=HEADERS, timeout=10)
        
        if weather_response.status_code != 200:
            print(f"🚨 ERROR: Could not retrieve weather data from {first_station_url}")
            return
        
        weather_data = weather_response.json().get("properties", {})
        
        # ✅ Print Full Weather Data
        print(json.dumps(weather_data, indent=2))

        # ✅ Print Key Marine Weather Data
        print(f"✅ Weather Data for {first_station_url}:")
        print(f"   🌡 Temperature: {weather_data.get('temperature', {}).get('value', 'N/A')} °C")
        print(f"   💨 Wind Speed: {weather_data.get('windSpeed', {}).get('value', 'N/A')} km/h")
        print(f"   💨 Wind Direction: {weather_data.get('windDirection', {}).get('value', 'N/A')}°")
        print(f"   🌊 Wave Height: {weather_data.get('waveHeight', {}).get('value', 'N/A')} m")
        print(f"   🌊 Wave Period: {weather_data.get('wavePeriod', {}).get('value', 'N/A')} sec")
        print(f"   🌊 Sea State: {weather_data.get('seaState', {}).get('value', 'N/A')}")
        print(f"   ☔ Precipitation: {weather_data.get('precipitationLastHour', {}).get('value', 'N/A')} mm")
        print(f"   📅 Observation Time: {weather_data.get('timestamp', 'N/A')}")
    
    except requests.exceptions.RequestException as e:
        print(f"❌ ERROR: Request failed - {e}")

def main():
    """Run the live NOAA diagnostic only when explicitly requested."""
    fetch_weather_stations(latitude, longitude)


if __name__ == "__main__":
    main()
