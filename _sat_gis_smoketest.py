from pathlib import Path
import pandas as pd
from datetime import datetime, timezone

from flask_app.app.sat_pipeline import build_sat_overlay_df
from flask_app.app.gis_mapping import generate_gis_map_html_from_dfs

# Minimal alert row (edit time/site/coords as needed)
alert_row = {
    "alert_id": "TEST001",
    "alert_time_utc": pd.Timestamp(datetime(2025, 9, 23, 2, 34, tzinfo=timezone.utc)),
    # Optional: put a known A-side to see next-pass marker logic
    "alert_lat_dd": 35.42,
    "alert_lon_dd": -76.61,
    "position_lat_dd_a": 35.42,
    "position_lon_dd_a": -76.61,
    "norad_id": 33591,  # NOAA-19 (test)
}
alert_df = pd.DataFrame([alert_row])

# Build sat overlays (MVP = reporting scope; LEO first; TLE on)
sat_overlay_df = build_sat_overlay_df(
    alert_df,
    types=("LEO",),
    use_tle=True
)
#sat_overlay_df = build_sat_overlay_df(alert_df, scope="reporting", types=("LEO",), use_tle=True)

print("sat_overlay_df rows:", len(sat_overlay_df))
print(sat_overlay_df[["sat_name","lat_dd","lon_dd","alt_km","footprint_radius_km","tle_age_hours"]].head().to_string(index=False))

sat_overlay_df = build_sat_overlay_df(alert_df, types=("LEO",), use_tle=True, fallback_to_nearest=True)

out_html = Path("data/maps/tests/sat_smoketest.html")
generate_gis_map_html_from_dfs(sat_overlay_df, alert_row, str(out_html))
print(f"[OK] Wrote {out_html}")

# 1) Reporting sat only
s1 = build_sat_overlay_df(alert_df, types=("LEO",), use_tle=True)
print("reporting-only rows:", len(s1))
print("cols:", list(s1.columns))
print("has_track:", isinstance(s1.iloc[0].get("track_coords"), (list, tuple)) and len(s1.iloc[0]["track_coords"]) > 1 if len(s1) else False)
print("has_nextpass:", isinstance(s1.iloc[0].get("next_pass_marker"), dict) if len(s1) else False)
generate_gis_map_html_from_dfs(s1, alert_row, "data/maps/tests/sat_reporting.html")

# 2) Reporting + nearest suggested LEOs
s2 = build_sat_overlay_df(alert_df, types=("LEO",), use_tle=True, fallback_to_nearest=True)
print("with-nearest rows:", len(s2))
generate_gis_map_html_from_dfs(s2, alert_row, "data/maps/tests/sat_with_nearest.html")
