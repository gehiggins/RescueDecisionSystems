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
import time
import math
import os
import re

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


def _lookup_text(value) -> str | None:
    """Return non-empty scalar alert text without truth-testing nullable values."""
    if value is None or not pd.api.types.is_scalar(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        return None
    text = str(value).strip()
    return text or None


def resolve_reporting_sat(alert_df: pd.DataFrame) -> pd.Series | None:
    """Resolve the reporting satellite from alert fields using the baseline."""
    if not isinstance(alert_df, pd.DataFrame) or alert_df.empty:
        return None
    df = load_sat_reference()
    common_names = df["common_name"].astype("string").str.lower()
    designators = df["designator"].astype("string").str.upper()

    # Try by name (sat_name or satellite_name)
    name_col = None
    for c in ["sat_name", "satellite_name"]:
        if c in alert_df.columns:
            name_col = c
            break
    if name_col:
        value = _lookup_text(alert_df.iloc[0][name_col])
        if value is not None:
            val = value.lower()
            # Exact match first, then a literal (not regex) partial match.
            matches = df[common_names == val]
            if matches.empty:
                matches = df[common_names.str.contains(val, na=False, regex=False)]
            if not matches.empty:
                if len(matches) > 1:
                    LOG.warning(
                        "Multiple satellite matches for name %r: %s",
                        val,
                        matches["common_name"].tolist(),
                    )
                return matches.iloc[0][["type", "designator", "common_name", "norad_id", "intl_designator"]]

    # Try by designator
    for c in ["sat_designator", "sat", "SAT"]:
        if c in alert_df.columns:
            value = _lookup_text(alert_df.iloc[0][c])
            if value is None:
                continue
            val = value.upper()
            matches = df[designators == val]
            if not matches.empty:
                if len(matches) > 1:
                    LOG.warning(
                        "Multiple satellite matches for designator %r: %s",
                        val,
                        matches["designator"].tolist(),
                    )
                return matches.iloc[0][["type", "designator", "common_name", "norad_id", "intl_designator"]]
    return None

# --- TLE SNAPSHOT RESOLVER ---

# In-memory cache for TLEs
_TLE_CACHE_CATNR: dict[int, dict] = {}
_TLE_CACHE_GROUP: dict[str, list] = {}
_TLE_PROVIDER_UNAVAILABLE_UNTIL = 0.0
_TLE_PROVIDER_BACKOFF_SECONDS = 60.0
_TLE_REQUEST_TIMEOUT_SECONDS = 10.0


def _request_timeout_seconds() -> float:
    """Return the configured finite positive satellite-provider timeout."""
    try:
        timeout = float(
            os.getenv(
                "RDS_SAT_REQUEST_TIMEOUT_SECONDS",
                str(_TLE_REQUEST_TIMEOUT_SECONDS),
            )
        )
    except (TypeError, ValueError):
        timeout = _TLE_REQUEST_TIMEOUT_SECONDS
    if not math.isfinite(timeout) or timeout <= 0:
        LOG.warning(
            "Invalid RDS_SAT_REQUEST_TIMEOUT_SECONDS; using %.0f seconds",
            _TLE_REQUEST_TIMEOUT_SECONDS,
        )
        return _TLE_REQUEST_TIMEOUT_SECONDS
    return timeout


def _provider_is_available() -> bool:
    return time.monotonic() >= _TLE_PROVIDER_UNAVAILABLE_UNTIL


def _mark_provider_unavailable() -> None:
    global _TLE_PROVIDER_UNAVAILABLE_UNTIL
    _TLE_PROVIDER_UNAVAILABLE_UNTIL = (
        time.monotonic() + _TLE_PROVIDER_BACKOFF_SECONDS
    )


def _tle_catalog_token(line: str, expected_prefix: str) -> str | None:
    """Extract a TLE catalog token only from the expected numbered line."""
    if not isinstance(line, str) or not line.startswith(expected_prefix):
        return None
    token = line[2:7].strip()
    return token if token and token.isalnum() else None


def _valid_tle_pair(line1: str, line2: str, expected_norad_id=None) -> bool:
    """Require matching line numbers/catalog fields before caching provider data."""
    catalog1 = _tle_catalog_token(line1, "1 ")
    catalog2 = _tle_catalog_token(line2, "2 ")
    if catalog1 is None or catalog1 != catalog2:
        return False
    if expected_norad_id is None:
        return True
    try:
        return int(catalog1) == int(expected_norad_id)
    except (TypeError, ValueError):
        return False


def _normalize_tle_requests(values) -> tuple[list[int], set[str]]:
    """Return unique positive catalog IDs and recognized constellation groups."""
    ids = []
    seen_ids = set()
    groups = set()
    if values is None:
        return ids, groups
    for value in values:
        if isinstance(value, bool):
            continue
        if isinstance(value, int):
            text = str(value)
        elif isinstance(value, str):
            text = value.strip()
        else:
            continue
        if re.fullmatch(r"\d+", text):
            catalog_id = int(text)
            if catalog_id > 0 and catalog_id not in seen_ids:
                seen_ids.add(catalog_id)
                ids.append(catalog_id)
            continue
        value_upper = text.upper()
        if value_upper in {"GALILEO", "GLONASS", "BEIDOU"}:
            groups.add(value_upper.lower())
        elif re.fullmatch(r"E\d+", value_upper):
            groups.add("galileo")
        elif re.fullmatch(r"R\d+", value_upper):
            groups.add("glonass")
        elif re.fullmatch(r"[BC]\d+", value_upper):
            groups.add("beidou")
    return ids, groups

def load_tle_snapshot(sat_names_or_ids: list[str]) -> pd.DataFrame:
    """
    Fetch TLEs for a list of satellites by norad_id or by group (constellation).
    Returns DataFrame with columns: ['name','norad_id','tle_line1','tle_line2','epoch_utc','provider','lookup_method','cache_hit']
    """
    rows = []
    ids, groups = _normalize_tle_requests(sat_names_or_ids)
    # E1. LEO/GEO by NORAD (preferred)
    for norad_id in ids:
        if norad_id in _TLE_CACHE_CATNR:
            # Cache-hit metadata belongs to this response, not the stored record.
            tle = _TLE_CACHE_CATNR[norad_id].copy()
            tle["cache_hit"] = True
            rows.append(tle)
            continue
        if not _provider_is_available():
            break
        url = f"https://celestrak.org/NORAD/elements/gp.php?CATNR={norad_id}&FORMAT=TLE"
        try:
            resp = requests.get(
                url,
                timeout=_request_timeout_seconds(),
                headers={"User-Agent": "RDS-SAT/1.0"},
            )
            if resp.status_code == 200:
                lines = [l.strip() for l in resp.text.splitlines() if l.strip()]
                if len(lines) >= 3 and _valid_tle_pair(
                    lines[1], lines[2], expected_norad_id=norad_id
                ):
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
                elif lines:
                    LOG.warning(
                        "Ignoring malformed TLE response for NORAD %s", norad_id
                    )
            elif resp.status_code == 429 or resp.status_code >= 500:
                _mark_provider_unavailable()
                LOG.warning(
                    "TLE provider unavailable (HTTP %s); pausing requests for %.0f seconds",
                    resp.status_code,
                    _TLE_PROVIDER_BACKOFF_SECONDS,
                )
                break
        except requests.RequestException as e:
            _mark_provider_unavailable()
            LOG.warning(f"TLE fetch failed for NORAD {norad_id}: {e}")
            break
        except Exception as e:
            LOG.warning(f"TLE response handling failed for NORAD {norad_id}: {e}")

    # E2. MEO (GNSS) by GROUP
    for group in groups:
        if group in _TLE_CACHE_GROUP:
            for tle in _TLE_CACHE_GROUP[group]:
                tle = tle.copy()
                tle["cache_hit"] = True
                rows.append(tle)
            continue
        if not _provider_is_available():
            break
        url = f"https://celestrak.org/NORAD/elements/gp.php?GROUP={group.upper()}&FORMAT=TLE"
        try:
            resp = requests.get(
                url,
                timeout=_request_timeout_seconds(),
                headers={"User-Agent": "RDS-SAT/1.0"},
            )
            if resp.status_code == 200:
                lines = [l.strip() for l in resp.text.splitlines() if l.strip()]
                tles = []
                for i in range(0, len(lines) - 2, 3):
                    name = lines[i]
                    tle1 = lines[i+1]
                    tle2 = lines[i+2]
                    if not _valid_tle_pair(tle1, tle2):
                        LOG.warning(
                            "Ignoring malformed TLE record in group %s", group
                        )
                        continue
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
                # Do not turn a malformed or empty provider response into a
                # process-lifetime cache hit. A later request must be able to
                # retry the constellation lookup after the provider recovers.
                if tles:
                    _TLE_CACHE_GROUP[group] = tles
            elif resp.status_code == 429 or resp.status_code >= 500:
                _mark_provider_unavailable()
                LOG.warning(
                    "TLE provider unavailable (HTTP %s); pausing requests for %.0f seconds",
                    resp.status_code,
                    _TLE_PROVIDER_BACKOFF_SECONDS,
                )
                break
        except requests.RequestException as e:
            _mark_provider_unavailable()
            LOG.warning(f"TLE fetch failed for group {group}: {e}")
            break
        except Exception as e:
            LOG.warning(f"TLE response handling failed for group {group}: {e}")

    df = pd.DataFrame(rows, columns=["name","norad_id","tle_line1","tle_line2","epoch_utc","provider","lookup_method","cache_hit"])
    if "epoch_utc" in df.columns:
        df["epoch_utc"] = pd.to_datetime(df["epoch_utc"], errors="coerce", utc=True)
    return df

# --- Resolver Contract ---
"""
Inputs to TLE resolver: one baseline row (type, designator, common_name, norad_id, intl_designator) + alert_time_utc.

Outputs: name, norad_id (nullable), tle_line1, tle_line2, epoch_utc, provider, lookup_method, cache_hit.
"""

