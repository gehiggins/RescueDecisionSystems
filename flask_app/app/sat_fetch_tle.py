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
from pathlib import Path
from typing import Optional, Dict

# Project-wide rule: internal imports must use `from app.` prefix
from app import __init__ as app_root  # noqa: F401
try:
    from app.utils_file_manifest import get_file_path as _manifest_get_file_path
except Exception:
    _manifest_get_file_path = None

REF_DEFAULT_PATH = Path("data/reference/sarsat_satellites.csv")
DESIGNATORS_DEFAULT_PATH = Path("data/reference/sarsat_designators.csv")

REQUIRED_COLS = [
    "sat_id","name","type","owner","constellation",
    "altitude_km","footprint_radius_km","is_active"
]

# ---- Auto-seed defaults (core LEOSAR) ----
DEFAULT_REFERENCE_ROWS = [
    # sat_id, name, type, owner, constellation, altitude_km, footprint_radius_km, is_active
    ("NOAA-15", "NOAA-15", "LEO", "USA", "LEOSAR", 850.0, 2500.0, 1),
    ("NOAA-18", "NOAA-18", "LEO", "USA", "LEOSAR", 850.0, 2500.0, 1),
    ("NOAA-19", "NOAA-19", "LEO", "USA", "LEOSAR", 850.0, 2500.0, 1),
    ("METOP-A", "MetOp-A", "LEO", "EU", "LEOSAR", 830.0, 2500.0, 1),
    ("METOP-B", "MetOp-B", "LEO", "EU", "LEOSAR", 830.0, 2500.0, 1),
    ("METOP-C", "MetOp-C", "LEO", "EU", "LEOSAR", 830.0, 2500.0, 1),
]

def _ensure_parent_dir(path: Path) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logging.warning(f"[sat_fetch_tle] Could not create parent dir for {path}: {e}")

def create_default_sat_reference_csv(path: Path) -> None:
    """Create a minimal reference CSV with core LEOSAR satellites."""
    _ensure_parent_dir(path)
    df = pd.DataFrame(
        DEFAULT_REFERENCE_ROWS,
        columns=["sat_id", "name", "type", "owner", "constellation", "altitude_km", "footprint_radius_km", "is_active"]
    )
    try:
        df.to_csv(path, index=False)
        logging.info(f"[sat_fetch_tle] Seeded default satellite reference at: {path}")
    except Exception as e:
        logging.error(f"[sat_fetch_tle] Failed to seed default reference CSV at {path}: {e}")

def load_sat_reference(manifest_id: Optional[str] = None) -> pd.DataFrame:
    """
    Load curated SARSAT-capable satellites.

    Behavior:
    - If the reference CSV is missing, seed it with core LEOSAR defaults (auto-bootstrap).
    - If the CSV exists but lacks required columns, add them with safe defaults (auto-migrate).
    - Coerce types and return a validated DataFrame.

    Returns columns:
      ['sat_id','name','type','owner','constellation','altitude_km','footprint_radius_km','is_active']
    """
    csv_path = _resolve_path(REF_DEFAULT_PATH, manifest_id)

    # 1) Auto-bootstrap if missing
    if not csv_path.exists():
        logging.warning(f"[sat_fetch_tle] Reference CSV missing, creating default: {csv_path}")
        create_default_sat_reference_csv(csv_path)

    # 2) Read whatever exists
    df = pd.read_csv(csv_path)

    # 3) Auto-migrate if required columns are missing
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        logging.warning(f"[sat_fetch_tle] Reference CSV missing columns {missing}; auto-migrating at {csv_path}")

        if "sat_id" in missing:
            if "name" in df.columns:
                df["sat_id"] = df["name"].astype(str).str.strip().replace("", pd.NA)
            else:
                df["sat_id"] = [f"UNKNOWN_{i+1}" for i in range(len(df))]
        if "owner" in missing:
            df["owner"] = "UNKNOWN"
        if "constellation" in missing:
            if "type" in df.columns:
                df["constellation"] = df["type"].astype(str).str.upper().map(
                    lambda t: "LEOSAR" if t == "LEO" else "UNKNOWN"
                )
            else:
                df["constellation"] = "UNKNOWN"
        if "altitude_km" in missing:
            df["altitude_km"] = pd.NA
        if "footprint_radius_km" in missing:
            df["footprint_radius_km"] = pd.NA
        if "is_active" in missing:
            df["is_active"] = 1

        # Ensure all required columns exist and order them
        for c in REQUIRED_COLS:
            if c not in df.columns:
                df[c] = pd.NA
        df = df[REQUIRED_COLS]

        # Persist migrated file so next run is clean
        try:
            _ensure_parent_dir(csv_path)
            df.to_csv(csv_path, index=False)
            logging.info(f"[sat_fetch_tle] Wrote auto-migrated reference CSV to {csv_path}")
        except Exception as e:
            logging.warning(f"[sat_fetch_tle] Could not write migrated CSV: {e}")

    # 4) Final coercions
    df["sat_id"] = df["sat_id"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).str.strip()
    df["type"] = df["type"].astype(str).str.upper().str.strip()
    df["owner"] = df["owner"].astype(str).str.strip()
    df["constellation"] = df["constellation"].astype(str).str.strip()
    df["altitude_km"] = pd.to_numeric(df["altitude_km"], errors="coerce")
    df["footprint_radius_km"] = pd.to_numeric(df["footprint_radius_km"], errors="coerce")
    df["is_active"] = df["is_active"].apply(lambda x: bool(int(x)) if str(x).isdigit() else bool(x))

    return df


def load_designator_map() -> Dict[str, str]:
    """
    Optional map 'S7' -> 'NOAA-19'. Returns {} if file missing or invalid.
    """
    p = _resolve_path(DESIGNATORS_DEFAULT_PATH)
    if not p.exists():
        return {}
    try:
        m = pd.read_csv(p)
        if not {"designator","name"}.issubset(m.columns):
            logging.warning("[sat_fetch_tle] designators CSV missing columns {'designator','name'}")
            return {}
        m["designator"] = m["designator"].astype(str).str.strip().str.upper()
        m["name"] = m["name"].astype(str).str.strip()
        return dict(zip(m["designator"], m["name"]))
    except Exception as e:
        logging.warning(f"[sat_fetch_tle] Failed loading designator map: {e}")
        return {}


def resolve_reporting_sat(alert_df: pd.DataFrame, sat_ref: pd.DataFrame, design_map: Dict[str,str]) -> Optional[pd.Series]:
    """
    Resolve reporting satellite row from alert_df using:
      1) sat_name / satellite_name exact match by name
      2) sat_designator / sat / SAT via designator map to name
    Returns a pandas.Series or None if not found.
    """
    def _first_non_null_str(df: pd.DataFrame, cols: list[str]) -> Optional[str]:
        for c in cols:
            if c in df.columns:
                vals = df[c].dropna()
                if len(vals) > 0:
                    v = str(vals.iloc[0]).strip()
                    if v:
                        return v
        return None

    # 1) Try name
    sat_name = _first_non_null_str(alert_df, ["sat_name","satellite_name"])
    if sat_name:
        # exact name first, then contains
        exact = sat_ref[sat_ref["name"].str.lower() == sat_name.lower()]
        if len(exact) == 1:
            return exact.iloc[0]
        contains = sat_ref[sat_ref["name"].str.lower().str.contains(sat_name.lower(), na=False)]
        if len(contains) >= 1:
            return contains.iloc[0]

    # 2) Try designator
    sat_designator = _first_non_null_str(alert_df, ["sat_designator","sat","SAT"])
    if sat_designator and design_map:
        mapped_name = design_map.get(sat_designator.strip().upper())
        if mapped_name:
            exact = sat_ref[sat_ref["name"].str.lower() == mapped_name.lower()]
            if len(exact) == 1:
                return exact.iloc[0]
            contains = sat_ref[sat_ref["name"].str.lower().str.contains(mapped_name.lower(), na=False)]
            if len(contains) >= 1:
                return contains.iloc[0]

    return None


def load_tle_snapshot(sat_names_or_ids: list[str]) -> pd.DataFrame:
    """
    Placeholder for future TLE snapshot support.
    MVP returns empty DF. Later:
      - Try local cached TLEs first (freshness window).
      - If allowed, fetch from network (e.g., CelesTrak).
      - Return DataFrame with columns:
          ['sat_id'|'name','tle_line1','tle_line2','epoch_utc']
    """
    logging.info("[sat_fetch_tle] TLE snapshot not implemented in MVP. Returning empty DataFrame.")
    return pd.DataFrame(columns=["sat_id","name","tle_line1","tle_line2","epoch_utc"])

