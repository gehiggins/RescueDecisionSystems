# generate_ndbc_station_metadata.py

import argparse
from html.parser import HTMLParser
from pathlib import Path
import pandas as pd
import requests
import time
import logging

# --------------------------------------------
# Logging Setup
# --------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --------------------------------------------
# File Paths (Per RDS Data Schema Rules)
# --------------------------------------------
REPOSITORY_ROOT = Path(__file__).resolve().parent
FULL_METADATA_FILE = REPOSITORY_ROOT / 'data' / 'reference' / 'ndbc_station_metadata_full.csv'
SUMMARY_FILE = REPOSITORY_ROOT / 'data' / 'reference' / 'ndbc_station_metadata_summary.csv'

# --------------------------------------------
# NOAA Data Source URLs
# --------------------------------------------
BASE_URL = "https://www.ndbc.noaa.gov/station_page.php?station="
REALTIME2_URL_TEMPLATE = "https://www.ndbc.noaa.gov/data/realtime2/{}.txt"
FIVEDAY2_URL_TEMPLATE = "https://www.ndbc.noaa.gov/data/5day2/{}_5day.txt"
METADATA_COLUMNS = [
    'station_id',
    'name',
    'owner',
    'deployment_notes',
    'has_realtime2',
    'has_fiveday2',
    'no_recent_data',
    'preferred_data_source',
]


class _NdbcHtmlParser(HTMLParser):
    """Collect the small subset of NDBC page content needed by this script."""

    def __init__(self):
        super().__init__()
        self.links = []
        self.heading_parts = []
        self.paragraphs = []
        self.page_text_parts = []
        self._capture = None
        self._captured_parts = []

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            href = dict(attrs).get('href')
            if href:
                self.links.append(href)
        if tag in {'h1', 'p'}:
            self._capture = tag
            self._captured_parts = []

    def handle_data(self, data):
        self.page_text_parts.append(data)
        if self._capture:
            self._captured_parts.append(data)

    def handle_endtag(self, tag):
        if tag != self._capture:
            return
        text = ' '.join(''.join(self._captured_parts).split())
        if tag == 'h1' and text:
            self.heading_parts.append(text)
        elif tag == 'p' and text:
            self.paragraphs.append(text)
        self._capture = None
        self._captured_parts = []


def _parse_html(html):
    parser = _NdbcHtmlParser()
    parser.feed(html)
    return parser

# --------------------------------------------
# Fetch metadata for a single station
# --------------------------------------------
def fetch_station_metadata(station_id, request_timeout=10):
    url = BASE_URL + station_id
    response = requests.get(url, timeout=request_timeout)
    if response.status_code != 200:
        logging.warning(f"[WARN] Failed to fetch station page for {station_id}")
        return None

    page = _parse_html(response.text)
    name = page.heading_parts[0] if page.heading_parts else 'Unknown'
    deployment_notes = 'No deployment notes found'
    owner = 'Unknown'

    for text in page.paragraphs:
        if 'Owned and maintained by' in text:
            deployment_notes = text.strip()
            owner = text.split('by ')[-1].strip()

    page_text = ' '.join(page.page_text_parts)
    no_recent_data = any(
        phrase in page_text for phrase in ["No Recent Reports", "No data available"]
    )

    has_realtime2 = requests.head(
        REALTIME2_URL_TEMPLATE.format(station_id), timeout=request_timeout
    ).status_code == 200
    has_fiveday2 = requests.head(
        FIVEDAY2_URL_TEMPLATE.format(station_id), timeout=request_timeout
    ).status_code == 200

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
def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Refresh the committed NDBC station metadata files"
    )
    parser.add_argument('--full-output', type=Path, default=FULL_METADATA_FILE)
    parser.add_argument('--summary-output', type=Path, default=SUMMARY_FILE)
    parser.add_argument('--delay-seconds', type=float, default=0.5)
    parser.add_argument('--request-timeout', type=float, default=10.0)
    args = parser.parse_args(argv)
    if args.delay_seconds < 0:
        parser.error('--delay-seconds must be zero or greater')
    if args.request_timeout <= 0:
        parser.error('--request-timeout must be greater than zero')

    logging.info("[INFO] Starting full NDBC station metadata scan...")

    stations_url = "https://www.ndbc.noaa.gov/to_station.shtml"
    stations_page = requests.get(stations_url, timeout=args.request_timeout)
    stations_page.raise_for_status()
    station_page = _parse_html(stations_page.text)
    station_ids = set()

    for href in station_page.links:
        if 'station_page.php?station=' in href:
            station_id = href.split('station=')[-1]
            station_ids.add(station_id)

    logging.info(f"[INFO] Found {len(station_ids)} station IDs for processing.")

    metadata = []
    for station_id in sorted(station_ids):
        logging.info(f"[INFO] Fetching metadata for station {station_id}...")
        meta = fetch_station_metadata(station_id, args.request_timeout)
        if meta:
            metadata.append(meta)
        if args.delay_seconds:
            time.sleep(args.delay_seconds)

    df = pd.DataFrame(metadata, columns=METADATA_COLUMNS)

    logging.info(f"[INFO] Total stations parsed: {len(df)}")
    logging.info("[INFO] Saving full metadata file...")
    args.full_output.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.full_output, index=False)

    logging.info("[INFO] Running summary analysis...")
    summary = df.groupby(['owner', 'preferred_data_source']).size().reset_index(name='station_count')
    args.summary_output.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(args.summary_output, index=False)

    logging.info(f"[INFO] Metadata and summary saved:\n- {args.full_output}\n- {args.summary_output}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

