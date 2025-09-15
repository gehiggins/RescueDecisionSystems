# Script Name: dev_smoke_gis.py
# Last Updated (UTC): 2025-09-01
# Update Summary:
# - NEW: Minimal developer smoke test for GIS PNG rendering.
# - Calls app.gis_mapping.generate_gis_png() with known A/B coords and ring radii.
# Description:
# - Creates a deterministic alert_row and invokes the PNG renderer to produce:
#   data/maps/<site_id>/rds_map_<site_id>.png
#   (optional) data/maps/<site_id>/positions_<site_id>.geojson
# External Data Sources:
# - None (static in-memory test data).
# Internal Variables:
# - site_id (str), include_b (bool)
# Produced DataFrames:
# - None
# Data Handling Notes:
# - Assumes generate_gis_png(alert_row, out_dir) exists in app.gis_mapping.
# - Prints the acceptance lines and verifies output path exists.

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from datetime import datetime

# --- Standard import block (project-wide) ---
# setup_imports lives at the flask_app/ level (one directory above app/)
try:
    from app.setup_imports import *  # noqa: F401,F403  (provides pandas, logging, etc. per your project)
except Exception:
    # Fallback: ensure flask_app/ is on sys.path, then retry
    here = Path(__file__).resolve().parent  # .../flask_app/app
    flask_root = here.parent                # .../flask_app
    if str(flask_root) not in sys.path:
        sys.path.insert(0, str(flask_root))
    from app.setup_imports import *  # type: ignore  # noqa: F401,F403

import pandas as pd  # in case setup_imports didn't wildcard-import pandas
import logging

# Internal module import must use the app. prefix (project rule)
try:
    from app.gis_mapping import generate_gis_png
except ImportError as e:
    msg = (
        "ImportError: Could not import 'generate_gis_png' from 'app.gis_mapping'.\n"
        "â€¢ Ensure Step 1 (Minimal GIS render) has been implemented in flask_app/app/gis_mapping.py\n"
        "â€¢ The function signature must be: generate_gis_png(alert_row: pandas.Series, out_dir: str) -> dict\n"
        f"Details: {e}"
    )
    raise SystemExit(msg)

def build_alert_row(site_id: str, include_b: bool) -> pd.Series:
    """
    Construct a single-row Series consistent with alert_df columns needed by the GIS renderer.
    A: ~37.7600, -75.5033, ring 5000
    B: ~38.2550, -70.2083, ring 5000 (optional)
    """
    data = {
        "site_id": site_id,
        "position_lat_dd_a": 37.7600,
        "position_lon_dd_a": -75.5033,
        "range_ring_meters_a": 5000.0,
        # Provide B defaults; may be removed if not included
        "position_lat_dd_b": 38.2550 if include_b else float("nan"),
        "position_lon_dd_b": -70.2083 if include_b else float("nan"),
        "range_ring_meters_b": 5000.0 if include_b else 0.0,
    }
    return pd.Series(data)

def main():
    parser = argparse.ArgumentParser(description="Developer smoke test for GIS PNG rendering.")
    parser.add_argument("--site-id", default="SMOKETEST001", help="Site identifier used in output paths.")
    parser.add_argument("--a-only", action="store_true", help="Render only Position A (no Position B).")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    include_b = not args.a_only
    site_id = args.site_id

    # Build the test row
    alert_row = build_alert_row(site_id=site_id, include_b=include_b)

    # Output directory and expected paths
    out_dir = Path(f"data/maps/{site_id}")
    out_dir.mkdir(parents=True, exist_ok=True)
    expected_png = out_dir / f"rds_map_{site_id}.png"
    expected_geojson = out_dir / f"positions_{site_id}.geojson"

    # Call the renderer
    info = generate_gis_png(alert_row, str(out_dir))

    # Acceptance prints (renderer should also print these; duplicating here is okay)
    print(f"âœ… Map image saved: {expected_png}")
    if expected_geojson.exists():
        print(f"âœ… Positions GeoJSON saved: {expected_geojson}")

    # Minimal verification
    if not expected_png.exists():
        raise SystemExit(f"[FAIL] PNG not found at expected path: {expected_png}")

    print("[PASS] Smoke test completed successfully.")

if __name__ == "__main__":
    main()

