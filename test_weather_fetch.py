#test_weather_fetch.py

import sys
import os
import logging

logging.info("âœ… Loaded setup_imports for consistent imports across scripts.")

# âœ… Ensure Python recognizes the `flask_app` directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# âœ… Import `setup_imports.py` using a relative path
from app.setup_imports import *

# âœ… Import `noaa_weather_fetch` correctly (assumes it's in flask_app/app)
from flask_app.app.noaa_weather_fetch import fetch_nearest_weather_stations, fetch_weather_data

# âœ… Test coordinates (Position A from SARSAT message â€” Chesapeake Bay area)
test_latitude = 37.76
test_longitude = -75.503333

print(f"âœ… Fetching nearest NOAA weather stations for ({test_latitude}, {test_longitude})...")

# Step 1: Fetch nearest NOAA weather stations
nearest_stations = fetch_nearest_weather_stations(test_latitude, test_longitude)

# Step 2: Print and verify results
if nearest_stations is None:
    print("âŒ ERROR: Function returned None instead of a DataFrame.")
elif nearest_stations.empty:
    print("âš ï¸ WARNING: No weather stations found near the test location.")
else:
    print("âœ… Nearest Weather Stations Found:")
    print(nearest_stations)

    # Step 3: Fetch weather data from the first available station
    test_station_id = nearest_stations.iloc[0]["station_id"]
    print(f"\nFetching weather data for station: {test_station_id}...")

    weather_data = fetch_weather_data(test_station_id)

    if weather_data.empty:
        print("âš ï¸ WARNING: No weather data retrieved.")
    else:
        print("âœ… Weather Data Retrieved:")
        print(weather_data)
        

