# generate_ndbc_station_metadata.py

import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import os
import logging

from flask_app.setup_imports import *

# --------------------------------------------
# Logging Setup
# --------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --------------------------------------------
# File Paths (Per RDS Data Schema Rules)
# --------------------------------------------
FULL_METADATA_FILE = 'C:/Users/gehig/Projects/RescueDecisionSystems/data/reference/ndbc_station_metadata_full.csv'
SUMMARY_FILE = 'C:/Users/gehig/Projects/RescueDecisionSystems/data/reference/ndbc_station_metadata_summary.csv'

# --------------------------------------------
# NOAA Data Source URLs
# --------------------------------------------
BASE_URL = "https://www.ndbc.noaa.gov/station_page.php?station="
REALTIME2_URL_TEMPLATE = "https://www.ndbc.noaa.gov/data/realtime2/{}.txt"
FIVEDAY2_URL_TEMPLATE = "https://www.ndbc.noaa.gov/data/5day2/{}_5day.txt"

# --------------------------------------------
# Fetch metadata for a single station
# --------------------------------------------
def fetch_station_metadata(station_id):
    url = BASE_URL + station_id
    response = requests.get(url)
    if response.status_code != 200:
        logging.warning(f"[WARN] Failed to fetch station page for {station_id}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')

    name = soup.find('h1').text.strip() if soup.find('h1') else 'Unknown'
    description = soup.find('p').text.strip() if soup.find('p') else 'No description'
    deployment_notes = 'No deployment notes found'
    owner = 'Unknown'

    for paragraph in soup.find_all('p'):
        text = paragraph.get_text()
        if 'Owned and maintained by' in text:
            deployment_notes = text.strip()
            owner = text.split('by ')[-1].strip()

    page_text = soup.get_text()
    no_recent_data = any(
        phrase in page_text for phrase in ["No Recent Reports", "No data available"]
    )

    has_realtime2 = requests.head(REALTIME2_URL_TEMPLATE.format(station_id)).status_code == 200
    has_fiveday2 = requests.head(FIVEDAY2_URL_TEMPLATE.format(station_id)).status_code == 200

    if has_realtime2 and not no_recent_data:
        preferred_data_source = 'realtime2'
    elif has_fiveday2 and not no_recent_data:
        preferred_data_source = '5day2'
    else:
        preferred_data_source = 'none'

    return {
        'station_id': station_id,
        'name': name,
        'owner': owner,
        'deployment_notes': deployment_notes,
        'has_realtime2': has_realtime2,
        'has_fiveday2': has_fiveday2,
        'no_recent_data': no_recent_data,
        'preferred_data_source': preferred_data_source
    }

# --------------------------------------------
# Main execution
# --------------------------------------------
def main():
    logging.info("[INFO] Starting full NDBC station metadata scan...")

    stations_url = "https://www.ndbc.noaa.gov/to_station.shtml"
    stations_page = requests.get(stations_url)
    soup = BeautifulSoup(stations_page.content, 'html.parser')

    station_links = soup.find_all('a', href=True)
    station_ids = set()

    for link in station_links:
        href = link['href']
        if 'station_page.php?station=' in href:
            station_id = href.split('station=')[-1]
            station_ids.add(station_id)

    logging.info(f"[INFO] Found {len(station_ids)} station IDs for processing.")

    metadata = []
    for station_id in sorted(station_ids):
        logging.info(f"[INFO] Fetching metadata for station {station_id}...")
        meta = fetch_station_metadata(station_id)
        if meta:
            metadata.append(meta)
        time.sleep(0.5)

    df = pd.DataFrame(metadata)

    logging.info(f"[INFO] Total stations parsed: {len(df)}")
    logging.info("[INFO] Saving full metadata file...")
    df.to_csv(FULL_METADATA_FILE, index=False)

    logging.info("[INFO] Running summary analysis...")
    summary = df.groupby(['owner', 'preferred_data_source']).size().reset_index(name='station_count')
    summary.to_csv(SUMMARY_FILE, index=False)

    logging.info(f"[INFO] Metadata and summary saved:\n- {FULL_METADATA_FILE}\n- {SUMMARY_FILE}")

if __name__ == "__main__":
    main()

print('TEST RUN COMPLETE')