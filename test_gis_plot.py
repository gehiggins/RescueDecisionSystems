import sys
import os
import pandas as pd
from app.setup_imports import *
from flask_app.app.gis_mapping import generate_gis_map

def main():
    # Manually constructed DataFrame row simulating SARSAT alert after parsing & weather attachment.
    alert_row = pd.Series({
        'site_id': '98372',
        'latitude_a': 37.76,
        'longitude_a': -75.503333,
        'range_ring_meters_a': 5000,
        'latitude_b': 38.15,
        'longitude_b': -70.12,
        'range_ring_meters_b': 5000,
        'nearest_weather_stations_a': [
            {'station_id': 'KWAL', 'latitude': 37.9372, 'longitude': -75.46619,
             'temperature': 2.2, 'wind_speed': None, 'visibility': None,
             'station_name': 'Wallops Island'}
        ],
        'nearest_weather_stations_b': [
            {'station_id': 'KSBY', 'latitude': 38.34056, 'longitude': -75.51028,
             'temperature': 3.5, 'wind_speed': 5.0, 'visibility': 10,
             'station_name': 'Salisbury'}
        ]
    })

    save_path = os.path.join(os.getenv('RDS_DATA_FOLDER'), 'maps')

    print("ðŸ—ºï¸ Generating GIS map...")
    map_path = generate_gis_map(alert_row, save_path)
    print(f"âœ… GIS map generated: {map_path}")

if __name__ == '__main__':
    main()

