import sys
import os

# Ensure flask_app is on the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "flask_app", "app")))

#from fetch_NOAA_offshore_buoys import fetch_offshore_buoys





def main():
    # Test location: Chesapeake offshore example
    test_lat = 37.5
    test_lon = -74.5

    print(f"🌊 Testing offshore buoy fetch for ({test_lat}, {test_lon}) using NDBC JSON API + realtime2 + station metadata fallback...")

    buoy_df = fetch_offshore_buoys(test_lat, test_lon, max_buoys=10)

    if buoy_df.empty:
        print("❌ No buoys found or no valid data available.")
    else:
        print("✅ Retrieved offshore buoy data:")

        # Expanded columns to display metadata alongside observations
        display_cols = [
            "station_id", "name", "latitude", "longitude", 
            "owner", "pgm", "deployment_notes",
            "source", "observation_time", "timelate",
            "temperature", "wind_speed", "wind_direction", "wave_height"
        ]

        print(buoy_df[display_cols].to_string(index=False))

if __name__ == "__main__":
    main()
