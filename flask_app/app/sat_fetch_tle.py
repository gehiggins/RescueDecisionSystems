from __future__ import annotations

# ============================== RDS STANDARD HEADER ==============================
# Script Name: sat_fetch_tle.py
# Last Updated (UTC): 2025-09-04
# Update Summary:
# - New module. Loads the curated SARSAT satellite reference and (optionally) designator map.
# - Provides resolver for the reporting satellite from alert_df.
# - TLE snapshot function stubbed for future use (offline-first; network optional).
# Description:
# - Purpose: Provide input tables for the satellite pipeline:
#     * sat_reference_df: curated set of SARSAT-capable satellites and nominal fields.
#     * designator_map: optional mapping like "S7" -> "NOAA-19".
#     * resolve_reporting_sat(): best-effort to pick the reporting satellite from alert_df.
# - Primary Inputs:
#   * CSV: /data/reference/sarsat_satellites.csv (required)
#   * CSV: /data/reference/sarsat_designators.csv (optional)
# - Primary Outputs:
#   * sat_reference_df (validated, coerced types)
#   * designator_map (dict)
# - External Data Sources:
#   * Local files only in MVP. TLE network fetch is a future feature of load_tle_snapshot().
# - Produced DataFrames:
#   * sat_reference_df columns:
#       ['sat_id','name','type','owner','constellation','altitude_km',
#        'footprint_radius_km','is_active']
# - Data Handling Notes:
#   * Strict column checks; NaN preserved; 'is_active' coerced to bool.
# ===============================================================================

from app.setup_imports import *
import pandas as pd
import numpy as np
import requests
import logging

from pathlib import Path
REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
BASELINE_CSV_PATH = REPOSITORY_ROOT / "data" / "reference" / "sarsat_satellites_baseline.csv"
# Expected columns: ['type','designator','common_name','norad_id','intl_designator']

LOG = logging.getLogger(__name__)

REF_DEFAULT_PATH = Path("data/reference/sarsat_satellites.csv")
DESIGNATORS_DEFAULT_PATH = Path("data/reference/sarsat_designators.csv")

REQUIRED_COLS = [
    "sat_id","name","type","owner","constellation",
    "altitude_km","footprint_radius_km","is_active"
]

# Core LEOSAR fallback used when a local reference file is not present. Catalog
# numbers and international designators follow the public CelesTrak catalog.
DEFAULT_REFERENCE_ROWS = [
    # type, designator, common_name, norad_id, intl_designator
    ("LEO", "NOAA-15", "NOAA 15", 25338, "1998-030A"),
    ("LEO", "NOAA-18", "NOAA 18", 28654, "2005-018A"),
    ("LEO", "NOAA-19", "NOAA 19", 33591, "2009-005A"),
    ("LEO", "METOP-A", "METOP-A", 29499, "2006-044A"),
    ("LEO", "METOP-B", "SARSAT 13 (METOP-B)", 38771, "2012-049A"),
    ("LEO", "METOP-C", "METOP-C", 43689, "2018-087A"),
]

def load_sat_reference() -> pd.DataFrame:
    """Load the SARSAT satellite baseline reference CSV."""
    if BASELINE_CSV_PATH.exists():
        try:
            df = pd.read_csv(BASELINE_CSV_PATH, encoding="utf-8")
        except UnicodeDecodeError:
            df = pd.read_csv(BASELINE_CSV_PATH, encoding="cp1252")
    else:
        df = pd.DataFrame(
            DEFAULT_REFERENCE_ROWS,
            columns=["type", "designator", "common_name", "norad_id", "intl_designator"],
        )

    required = ["type", "designator", "common_name", "norad_id", "intl_designator"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing required column '{col}' in baseline CSV")
    df["norad_id"] = pd.to_numeric(df["norad_id"], errors="coerce").astype("Int64")
    return df

def resolve_reporting_sat(alert_df: pd.DataFrame) -> pd.Series | None:
    """Resolve the reporting satellite from alert fields using the baseline."""
    df = load_sat_reference()
    row = None

    # Try by name (sat_name or satellite_name)
    name_col = None
    for c in ["sat_name", "satellite_name"]:
        if c in alert_df.columns:
            name_col = c
            break
    if name_col:
        val = str(alert_df.iloc[0][name_col]).strip().lower()
        # Exact match first
        matches = df[df["common_name"].str.lower() == val]
        if matches.empty:
            # Contains match
            matches = df[df["common_name"].str.lower().str.contains(val, na=False)]
        if not matches.empty:
            if len(matches) > 1:
                LOG.warning(f"Multiple satellite matches for name '{val}': {[m for m in matches['common_name']]}")
            return matches.iloc[0][["type", "designator", "common_name", "norad_id", "intl_designator"]]

    # Try by designator
    for c in ["sat_designator", "sat", "SAT"]:
        if c in alert_df.columns:
            val = str(alert_df.iloc[0][c]).strip().upper()
            matches = df[df["designator"].str.upper() == val]
            if not matches.empty:
                if len(matches) > 1:
                    LOG.warning(f"Multiple satellite matches for designator '{val}': {[m for m in matches['designator']]}")
                return matches.iloc[0][["type", "designator", "common_name", "norad_id", "intl_designator"]]
    return None

# --- TLE SNAPSHOT RESOLVER ---

# In-memory cache for TLEs
_TLE_CACHE_CATNR: dict[int, dict] = {}
_TLE_CACHE_GROUP: dict[str, list] = {}

def load_tle_snapshot(sat_names_or_ids: list[str]) -> pd.DataFrame:
    """
    Fetch TLEs for a list of satellites by norad_id or by group (constellation).
    Returns DataFrame with columns: ['name','norad_id','tle_line1','tle_line2','epoch_utc','provider','lookup_method','cache_hit']
    """
    rows = []
    # Try to parse norad_ids
    ids = []
    groups = set()
    for s in sat_names_or_ids:
        try:
            i = int(s)
            ids.append(i)
        except Exception:
            # Try to infer group from designator
            if isinstance(s, str):
                s_up = s.upper()
                if s_up.startswith("E"):
                    groups.add("galileo")
                elif s_up.startswith("R"):
                    groups.add("glonass")
                elif s_up.startswith("B") or s_up.startswith("C"):
                    groups.add("beidou")
    # E1. LEO/GEO by NORAD (preferred)
    for norad_id in ids:
        if norad_id in _TLE_CACHE_CATNR:
            tle = _TLE_CACHE_CATNR[norad_id]
            tle["cache_hit"] = True
            rows.append(tle)
            continue
        url = f"https://celestrak.org/NORAD/elements/gp.php?CATNR={norad_id}&FORMAT=TLE"
        try:
            resp = requests.get(url, timeout=10, headers={"User-Agent": "RDS-SAT/1.0"})
            if resp.status_code == 200:
                lines = [l.strip() for l in resp.text.splitlines() if l.strip()]
                if len(lines) >= 3:
                    tle = {
                        "name": lines[0],
                        "norad_id": norad_id,
                        "tle_line1": lines[1],
                        "tle_line2": lines[2],
                        "epoch_utc": None,
                        "provider": "celestrak",
                        "lookup_method": "catnr",
                        "cache_hit": False
                    }
                    # Try to parse epoch from line1
                    try:
                        epoch_str = lines[1][18:32]
                        from datetime import datetime, timedelta, timezone
                        yy = int(epoch_str[:2])
                        year = 2000 + yy if yy < 57 else 1900 + yy
                        day_of_year = float(epoch_str[2:])
                        dt = datetime(year, 1, 1, tzinfo=timezone.utc) + timedelta(days=day_of_year - 1)
                        tle["epoch_utc"] = dt.isoformat()
                    except Exception:
                        pass
                    _TLE_CACHE_CATNR[norad_id] = tle
                    rows.append(tle)
        except Exception as e:
            LOG.warning(f"TLE fetch failed for NORAD {norad_id}: {e}")

    # E2. MEO (GNSS) by GROUP
    for group in groups:
        if group in _TLE_CACHE_GROUP:
            for tle in _TLE_CACHE_GROUP[group]:
                tle = tle.copy()
                tle["cache_hit"] = True
                rows.append(tle)
            continue
        url = f"https://celestrak.org/NORAD/elements/gp.php?GROUP={group.upper()}&FORMAT=TLE"
        try:
            resp = requests.get(url, timeout=10, headers={"User-Agent": "RDS-SAT/1.0"})
            if resp.status_code == 200:
                lines = [l.strip() for l in resp.text.splitlines() if l.strip()]
                tles = []
                for i in range(0, len(lines) - 2, 3):
                    name = lines[i]
                    tle1 = lines[i+1]
                    tle2 = lines[i+2]
                    norad_id = None
                    try:
                        norad_id = int(tle1[2:7])
                    except Exception:
                        pass
                    tle = {
                        "name": name,
                        "norad_id": norad_id,
                        "tle_line1": tle1,
                        "tle_line2": tle2,
                        "epoch_utc": None,
                        "provider": "celestrak",
                        "lookup_method": "group",
                        "cache_hit": False
                    }
                    # Try to parse epoch from line1
                    try:
                        epoch_str = tle1[18:32]
                        from datetime import datetime, timedelta, timezone
                        yy = int(epoch_str[:2])
                        year = 2000 + yy if yy < 57 else 1900 + yy
                        day_of_year = float(epoch_str[2:])
                        dt = datetime(year, 1, 1, tzinfo=timezone.utc) + timedelta(days=day_of_year - 1)
                        tle["epoch_utc"] = dt.isoformat()
                    except Exception:
                        pass
                    tles.append(tle)
                    rows.append(tle)
                _TLE_CACHE_GROUP[group] = tles
        except Exception as e:
            LOG.warning(f"TLE fetch failed for group {group}: {e}")

    df = pd.DataFrame(rows, columns=["name","norad_id","tle_line1","tle_line2","epoch_utc","provider","lookup_method","cache_hit"])
    if "epoch_utc" in df.columns:
        df["epoch_utc"] = pd.to_datetime(df["epoch_utc"], errors="coerce", utc=True)
    return df

# --- Resolver Contract ---
"""
Inputs to TLE resolver: one baseline row (type, designator, common_name, norad_id, intl_designator) + alert_time_utc.

Outputs: name, norad_id (nullable), tle_line1, tle_line2, epoch_utc, provider, lookup_method, cache_hit.
"""

