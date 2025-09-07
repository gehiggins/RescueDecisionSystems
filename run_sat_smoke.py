import pandas as pd
from flask_app.app.sat_pipeline import build_sat_overlay_df

# Minimal alert_df — mimic a parsed SIT with a named LEO
alert_df = pd.DataFrame([{
    "sat_name": "NOAA-19",
    "position_lat_dd_a": 40.0,
    "position_lon_dd_a": -120.0,
    "position_lat_dd_b": None,
    "position_lon_dd_b": None,
}])

sat_overlay_df = build_sat_overlay_df(
    alert_df,
    scope="reporting",
    types=("LEO",),
    catalog_manifest_id=None,
    use_tle=False
)

print("\n=== sat_overlay_df ===")
print(sat_overlay_df.to_string(index=False))
