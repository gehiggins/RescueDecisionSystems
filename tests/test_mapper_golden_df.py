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
import base64

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


def test_renderer_embeds_local_satellite_image_overlay(tmp_path):
    image_path = tmp_path / "coverage.png"
    image_path.write_bytes(
        base64.b64decode(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk"
            "/x8AAusB9Wl2nWQAAAAASUVORK5CYII="
        )
    )
    output_path = tmp_path / "map.html"
    inputs = pd.DataFrame(
        [
            {
                "site_id": "TEST",
                "layer": "satellite_overlay",
                "geom_type": "ImageOverlay",
                "geometry": {
                    "type": "ImageOverlay",
                    "image_path": str(image_path),
                    "bounds": [[35.0, -78.0], [40.0, -72.0]],
                    "opacity": 0.6,
                    "name": "Coverage image",
                },
                "label": "Coverage image",
            }
        ]
    )

    result = generate_gis_map_html_from_dfs(
        inputs,
        alert_row={"site_id": "TEST", "position_lat_dd_a": 37.5, "position_lon_dd_a": -75.0},
        out_path=str(output_path),
    )

    html = output_path.read_text(encoding="utf-8")
    assert result["status"] == "ok"
    assert "Satellite Overlays" in result["layers"]
    assert "Satellite imagery" in html
    assert "[[35.0, -78.0], [40.0, -72.0]]" in html
    assert "data:image/png;base64" in html
