#tle_fetcher.py step 1 for satellite data and tracking

import os
import requests

# Target directory
TLE_DIR = 'data/tle'
os.makedirs(TLE_DIR, exist_ok=True)

# URLs for SARSAT and GNSS constellations
TLE_SOURCES = {
    'sarsat': 'https://celestrak.org/NORAD/elements/gp.php?GROUP=sarsat&FORMAT=tle',
    'gps': 'https://celestrak.org/NORAD/elements/gp.php?GROUP=gps-ops&FORMAT=tle',
    'galileo': 'https://celestrak.org/NORAD/elements/gp.php?GROUP=galileo&FORMAT=tle',
    'glonass': 'https://celestrak.org/NORAD/elements/gp.php?GROUP=glonass&FORMAT=tle'
}

def fetch_tle():
    for group, url in TLE_SOURCES.items():
        response = requests.get(url)
        if response.status_code == 200:
            file_path = os.path.join(TLE_DIR, f'{group}.tle')
            with open(file_path, 'w') as file:
                file.write(response.text)
            print(f"✅ TLE data for {group} saved to {file_path}")
        else:
            print(f"❌ Failed to fetch TLE data for {group} (HTTP {response.status_code})")

if __name__ == '__main__':
    fetch_tle()
