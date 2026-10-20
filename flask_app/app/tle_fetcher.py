"""Explicit utility for downloading satellite TLE files."""

import logging
import os
from pathlib import Path

import requests


LOG = logging.getLogger(__name__)
TLE_SOURCES = {
    "sarsat": "https://celestrak.org/NORAD/elements/gp.php?GROUP=sarsat&FORMAT=tle",
    "gps": "https://celestrak.org/NORAD/elements/gp.php?GROUP=gps-ops&FORMAT=tle",
    "galileo": "https://celestrak.org/NORAD/elements/gp.php?GROUP=galileo&FORMAT=tle",
    "glonass": "https://celestrak.org/NORAD/elements/gp.php?GROUP=glonass&FORMAT=tle",
}


def _tle_dir():
    repository_data = Path(__file__).resolve().parents[2] / "data" / "tle"
    return Path(os.getenv("RDS_TLE_DIR", str(repository_data)))


def fetch_tle(sources=None, timeout=20):
    """Download configured TLE groups and return the files successfully written."""
    output_dir = _tle_dir()
    output_dir.mkdir(parents=True, exist_ok=True)
    written = []

    for group, url in (sources or TLE_SOURCES).items():
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            LOG.warning("Failed to fetch TLE data for %s: %s", group, exc)
            continue

        file_path = output_dir / f"{group}.tle"
        file_path.write_text(response.text, encoding="utf-8")
        written.append(file_path)
        LOG.info("TLE data for %s saved to %s", group, file_path)

    return written


if __name__ == "__main__":
    fetch_tle()
