# ======================================================================
# Script Name: sat_fetch_sarsat_list.py
# Last Updated (UTC): 2025-09-22
# Update Summary:
# - Initial version: builds SARSAT-capable satellite list from CelesTrak
# Description:
# - Fetches TLE groups from CelesTrak (LEO/GEO/MEO sats with SARSAT payloads)
# - Produces DataFrame and saves to /data/reference/sarsat_sat_list.csv
# External Data Sources:
# - CelesTrak TLE groups: sarsat, noaa, goes, gps-ops, galileo, glo-ops, beidou
# Internal Variables:
# - Uses get_file_path("sat_list") from file_manifest.csv
# Produced DataFrames:
# - sat_list_df: [type, constellation, designator, common_name, norad_id,
#   tle_source, nominal_alt_km, is_active, fetched_utc]
# Data Handling Notes:
# - If fetch/parsing fails, logs warning and reuses last known CSV.
# - Safe-fail: pipeline continues without fresh updates.
# ======================================================================

from app.setup_imports import *
import os
import requests
import re
from datetime import datetime

# -------------------------------
# Config
# -------------------------------

# Ensure a logger exists even if setup_imports didn’t expose `log`
import logging
log = globals().get("log") or logging.getLogger("sat_fetch")
if not log.handlers:
    logging.basicConfig(level=os.getenv("RDS_LOG_LEVEL", "INFO"))


CELESTRAK_GROUPS = {
    "sarsat":  "https://celestrak.org/NORAD/elements/gp.php?GROUP=SARSAT&FORMAT=TLE",
    "noaa":    "https://celestrak.org/NORAD/elements/gp.php?GROUP=NOAA&FORMAT=TLE",
    "goes":    "https://celestrak.org/NORAD/elements/gp.php?GROUP=GOES&FORMAT=TLE",
    "gps":     "https://celestrak.org/NORAD/elements/gp.php?GROUP=GPS-OPS&FORMAT=TLE",
    "galileo": "https://celestrak.org/NORAD/elements/gp.php?GROUP=GALILEO&FORMAT=TLE",
    "glonass": "https://celestrak.org/NORAD/elements/gp.php?GROUP=GLO-OPS&FORMAT=TLE",
    "beidou":  "https://celestrak.org/NORAD/elements/gp.php?GROUP=BEIDOU&FORMAT=TLE",
}

ALT_ENDPOINTS = {
    "sarsat":  ["https://celestrak.org/NORAD/elements/sarsat.txt"],
    "noaa":    ["https://celestrak.org/NORAD/elements/noaa.txt"],
    "goes":    ["https://celestrak.org/NORAD/elements/goes.txt"],
    "gps":     ["https://celestrak.org/NORAD/elements/gps-ops.txt"],
    "galileo": ["https://celestrak.org/NORAD/elements/galileo.txt"],
    "glonass": ["https://celestrak.org/NORAD/elements/glo-ops.txt"],
    "beidou":  ["https://celestrak.org/NORAD/elements/beidou.txt"],
}


NOMINAL_ALT = {
    "LEOSAR": 850.0,
    "GEOSAR": 35786.0,
    "MEOSAR": 20200.0,
}

# -------------------------------
# Helpers
# -------------------------------

def parse_tle_lines(lines, group):
    """Extract sat names and optional NORAD IDs from TLE headers."""
    sats = []
    for line in lines:
        if line.strip() and not line.startswith("1 ") and not line.startswith("2 "):
            name = line.strip()
            m = re.search(r"\((\d+)\)", name)
            norad_id = int(m.group(1)) if m else None
            sats.append((name, norad_id, group))
    return sats

import time
UA = {"User-Agent": "RDS-SARSAT-Fetch/1.0 (+https://local)"}

def _try_one(url, group):
    r = requests.get(url, headers=UA, timeout=20)
    if r.status_code == 200:
        return parse_tle_lines(r.text.splitlines(), group)
    log.warning(f"[sat_fetch] {group} fetch {url} -> HTTP {r.status_code}")
    return []

def safe_fetch(url, group):
    # primary with retries
    for i in range(3):
        try:
            sats = _try_one(url, group)
            if sats: 
                return sats
        except Exception as e:
            log.warning(f"[sat_fetch] {group} try {i+1} error: {e}")
        time.sleep(1 + i)  # jittered backoff

    # alternates
    for alt in ALT_ENDPOINTS.get(group, []):
        try:
            sats = _try_one(alt, group)
            if sats:
                return sats
        except Exception as e:
            log.warning(f"[sat_fetch] {group} alt error: {e}")
        time.sleep(1)

    return []

def classify_sat(name, group):
    """Classify as LEOSAR / GEOSAR / MEOSAR and tag constellation."""
    g = group.lower()
    n = name.lower()
    if "noaa" in g or "sarsat" in g or "metop" in n:
        return "LEOSAR", "NOAA/MetOp"
    if "goes" in g or "msg" in n or "elektro" in n or "insat" in n:
        return "GEOSAR", "GEO"
    if "gps" in g:
        return "MEOSAR", "GPS"
    if "galileo" in g:
        return "MEOSAR", "Galileo"
    if "glo" in g:
        return "MEOSAR", "GLONASS"
    if "bei" in g:
        return "MEOSAR", "BeiDou"
    return "UNKNOWN", group

# -------------------------------
# Main functions
# -------------------------------

def build_sat_list():
    """Build DataFrame of SARSAT satellites from external sources."""
    all_rows = []
    for group, url in CELESTRAK_GROUPS.items():
        sats = safe_fetch(url, group)
        for (name, norad_id, grp) in sats:
            sat_type, constellation = classify_sat(name, grp)
            row = {
                "type": sat_type,
                "constellation": constellation,
                "designator": None,  # left for RCC code mapping later
                "common_name": name,
                "norad_id": norad_id,
                "tle_source": grp,
                "nominal_alt_km": NOMINAL_ALT.get(sat_type, None),
                "is_active": 1,
                "fetched_utc": datetime.utcnow().isoformat(),
            }
            all_rows.append(row)

    return pd.DataFrame(all_rows)

def save_sat_list(df):
    """Save DataFrame to CSV, safe-fail with last known good fallback."""
    try:
        out_path = get_file_path("sat_list")
    except Exception:
        log.error("[sat_fetch] sat_list not found in file_manifest.csv")
        return

    try:
        df.to_csv(out_path, index=False)
        log.info(f"[sat_fetch] Saved updated sat_list to {out_path}")
    except Exception as e:
        log.warning(f"[sat_fetch] Save failed: {e}")

def fetch_and_update_sat_list():
    """Top-level routine: fetch, build DataFrame, save if good."""
    try:
        df = build_sat_list()
        if df.empty:
            log.warning("[sat_fetch] No satellites fetched; keeping last known CSV")
            return None
        save_sat_list(df)
        return df
    except Exception as e:
        log.warning(f"[sat_fetch] Unexpected error: {e}")
        return None

# -------------------------------
# CLI use
# -------------------------------

if __name__ == "__main__":
    log.info("[sat_fetch] Running standalone SARSAT satellite list update")
    fetch_and_update_sat_list()
