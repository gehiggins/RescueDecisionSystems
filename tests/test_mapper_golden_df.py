# tests/test_mapper_golden_df.py
try:
    from app.gis_mapping import generate_gis_map_html_from_dfs
except ModuleNotFoundError:
    import os, sys
    from pathlib import Path
    repo_root = Path(__file__).resolve().parents[1]
    flask_app_dir = repo_root / "flask_app"
    if str(flask_app_dir) not in sys.path:
        sys.path.insert(0, str(flask_app_dir))
    from app.gis_mapping import generate_gis_map_html_from_dfs

import pandas as pd
import os

def test_golden_gis_map_inputs_df():
    df = pd.DataFrame([
        # 1) Alert Position
        {
            "site_id": "TEST",
            "layer": "alert_position",
            "geom_type": "Point",
            "geometry": {"type": "Point", "coordinates": [-70.208333, 38.255]},
            "ts_utc": "2025-09-16T00:00:00Z",
            "ts_local": "2025-09-15T20:00:00-04:00",
            "local_tz": "America/New_York",
            "label": "Alert A",
            "source_table": "alerts",
            "source_id": "TEST-ALERT-1",
            "wave_height_display": "None",
            "wind_display": "None",
            "temp_display": "None",
        },
        # 2) Range Rings (valid)
        {
            "site_id": "TEST",
            "layer": "range_ring",
            "geom_type": "Circle",
            "geometry": {"type": "Circle", "center": [-70.208333, 38.255], "radius_m": 3704},
            "ts_utc": None,
            "ts_local": None,
            "local_tz": "America/New_York",
            "label": "EE95 Ring",
            "source_table": "alerts",
            "source_id": "TEST-ALERT-1",
            "wave_height_display": "None",
            "wind_display": "None",
            "temp_display": "None",
        },
        # 2) Range Rings (missing)
        {
            "site_id": "TEST",
            "layer": "range_ring",
            "geom_type": "Circle",
            "geometry": {"type": "Circle", "center": [-70.208333, 38.255], "radius_m": None},
            "ts_utc": None,
            "ts_local": None,
            "local_tz": "America/New_York",
            "label": "Missing Ring",
            "source_table": "alerts",
            "source_id": "TEST-ALERT-1",
            "wave_height_display": "None",
            "wind_display": "None",
            "temp_display": "None",
        },
        # 3) Weather (partial data)
        {
            "site_id": "TEST",
            "layer": "weather",
            "geom_type": "Point",
            "geometry": {"type": "Point", "coordinates": [-70.208333, 38.255]},
            "ts_utc": "2025-09-16T00:10:00Z",
            "ts_local": "2025-09-15T20:10:00-04:00",
            "local_tz": "America/New_York",
            "label": "Weather",
            "source_table": "weather",
            "source_id": "om:38.255,-70.2083",
            "wave_height_display": "None",
            "wind_display": "18 kt",
            "temp_display": "None",
        },
        # 4) Station (partial data)
        {
            "site_id": "TEST",
            "layer": "station",
            "geom_type": "Point",
            "geometry": {"type": "Point", "coordinates": [-70.208333, 38.255]},
            "ts_utc": None,
            "ts_local": None,
            "local_tz": "America/New_York",
            "label": "Station",
            "source_table": "stations",
            "source_id": "STATION-1",
            "wave_height_display": "None",
            "wind_display": "None",
            "temp_display": "None",
        },
        # 5) Satellite Overlays (LineString)
        {
            "site_id": "TEST",
            "layer": "satellite_overlay",
            "geom_type": "LineString",
            "geometry": {"type": "LineString", "coordinates": [[-70.30, 38.20], [-70.10, 38.30]]},
            "ts_utc": None,
            "ts_local": None,
            "local_tz": "America/New_York",
            "label": "LEO-A pass",
            "source_table": "satellite",
            "source_id": "LEO-A-1",
            "wave_height_display": "None",
            "wind_display": "None",
            "temp_display": "None",
        }
    ])

    out = generate_gis_map_html_from_dfs(
        gis_map_inputs_df=df,
        out_dir="data/maps/TEST",
        site_id="TEST"
    )

    assert isinstance(out, dict)
    assert out.get("status") == "ok"
    assert os.path.exists(out["map_html_path"])
    assert "Satellite Overlays" in out["layers"]
    # No float formatting on raw values in this test; only display fields are used.
    for col in ["wave_height_display", "wind_display", "temp_display"]:
        assert col in df.columns
        assert all(isinstance(x, str) for x in df[col])

# This test does not require network or DB.
