"""Explicit utility for downloading satellite TLE files."""

import logging
import math
import os
from pathlib import Path
import re
import tempfile

import requests


LOG = logging.getLogger(__name__)
DEFAULT_TIMEOUT_SECONDS = 20.0
MAX_TLE_BYTES = 5_000_000
TLE_HEADERS = {"User-Agent": "RescueDecisionSystems/0.2 (TLE downloader)"}
TLE_SOURCES = {
    "sarsat": "https://celestrak.org/NORAD/elements/gp.php?GROUP=sarsat&FORMAT=tle",
    "gps": "https://celestrak.org/NORAD/elements/gp.php?GROUP=gps-ops&FORMAT=tle",
    "galileo": "https://celestrak.org/NORAD/elements/gp.php?GROUP=galileo&FORMAT=tle",
    "glonass": "https://celestrak.org/NORAD/elements/gp.php?GROUP=glonass&FORMAT=tle",
}


def _tle_dir():
    repository_data = Path(__file__).resolve().parents[2] / "data" / "tle"
    return Path(os.getenv("RDS_TLE_DIR", str(repository_data)))


def _request_timeout(value):
    try:
        timeout = float(value)
    except (TypeError, ValueError, OverflowError):
        return DEFAULT_TIMEOUT_SECONDS
    return timeout if math.isfinite(timeout) and timeout > 0 else DEFAULT_TIMEOUT_SECONDS


def _valid_tle_payload(text):
    """Reject empty or error-page responses before replacing cached TLEs."""
    if not isinstance(text, str) or not text.strip():
        return False
    if len(text.encode("utf-8")) > MAX_TLE_BYTES:
        return False
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return any(line.startswith("1 ") for line in lines) and any(
        line.startswith("2 ") for line in lines
    )


def _write_tle_atomically(file_path, text):
    """Replace one regular cache file without following a file symlink."""
    if file_path.is_symlink():
        raise ValueError(f"TLE output cannot be a symbolic link: {file_path}")
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=file_path.parent,
            prefix=f".{file_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary.write(text)
            temporary_path = Path(temporary.name)
        os.replace(temporary_path, file_path)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()


def fetch_tle(sources=None, timeout=DEFAULT_TIMEOUT_SECONDS):
    """Download configured TLE groups and return the files successfully written."""
    output_dir = _tle_dir().expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    written = []
    timeout = _request_timeout(timeout)

    for group, url in (sources or TLE_SOURCES).items():
        group = str(group)
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,40}", group):
            LOG.warning("Skipping unsafe TLE group name: %r", group)
            continue
        try:
            response = requests.get(url, timeout=timeout, headers=TLE_HEADERS)
            response.raise_for_status()
        except requests.RequestException as exc:
            LOG.warning("Failed to fetch TLE data for %s: %s", group, exc)
            continue

        if not _valid_tle_payload(response.text):
            LOG.warning("TLE provider returned invalid data for %s", group)
            continue

        file_path = output_dir / f"{group}.tle"
        try:
            _write_tle_atomically(file_path, response.text)
        except (OSError, ValueError) as exc:
            LOG.warning("Failed to store TLE data for %s: %s", group, exc)
            continue
        written.append(file_path)
        LOG.info("TLE data for %s saved to %s", group, file_path)

    return written


if __name__ == "__main__":
    fetch_tle()
