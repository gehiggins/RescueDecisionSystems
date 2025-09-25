# tests/test_mapper_golden_df.py
try:
    from app.gis_mapping import generate_gis_map_html_from_dfs
except ModuleNotFoundError:
    import os, sys
    REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    FLASK_APP_DIR = os.path.join(REPO_ROOT, "flask_app")
    if FLASK_APP_DIR not in sys.path:
        sys.path.insert(0, FLASK_APP_DIR)
    from app.gis_mapping import generate_gis_map_html_from_dfs
    
    import pandas as pd
from pathlib import Path
from app.gis_mapping import generate_gis_map_html_from_dfs

def build_golden_df():
    # Structure reused from tests/test_mapper_golden_df.py
    data = [
        {
            "site_id": "TEST",
            "layer": "alert_position",
            "geom_type": "Point",
            "geometry": {"type": "Point", "coordinates": [-122.33, 47.60]},
            "ts_utc": "2025-09-19T12:00:00Z",
            "ts_local": "2025-09-19T05:00:00-07:00",
            "local_tz": "America/Los_Angeles",
            "label": "A",
            "popup_html": "<b>A</b><br>Lat: 47.60000<br>Lon: -122.33000",
            "style_hint": {},
            "source_table": "positions",
            "source_id": "TEST",
            "is_maritime": False,
            "range_ring_meters": 5000,
            "wave_height_m": None,
            "wind_ms": None,
            "temp_C": None,
            "wave_height_display": "None",
            "wind_display": "None",
            "temp_display": "None",
        },
        {
            "site_id": "TEST",
            "layer": "weather",
            "geom_type": "Point",
            "geometry": {"type": "Point", "coordinates": [-122.35, 47.61]},
            "ts_utc": "2025-09-19T12:10:00Z",
            "ts_local": "2025-09-19T05:10:00-07:00",
            "local_tz": "America/Los_Angeles",
            "label": "Weather",
            "popup_html": "<b>Weather</b><br>Lat: 47.61000<br>Lon: -122.35000<br>Waves: 2.5 ft<br>Wind: 10 kt<br>Temp: 65 °F / 18.3 °C",
            "style_hint": {},
            "source_table": "weather",
            "source_id": "WX1",
            "is_maritime": False,
            "range_ring_meters": None,
            "wave_height_m": 0.76,
            "wind_ms": 5.14,
            "temp_C": 18.3,
            "wave_height_display": "2.5 ft",
            "wind_display": "10 kt",
            "temp_display": "65 °F / 18.3 °C",
        },
        {
            "site_id": "TEST",
            "layer": "station",
            "geom_type": "Point",
            "geometry": {"type": "Point", "coordinates": [-122.36, 47.62]},
            "ts_utc": None,
            "ts_local": None,
            "local_tz": "America/Los_Angeles",
            "label": "Station",
            "popup_html": "<b>Station</b><br>Lat: 47.62000<br>Lon: -122.36000<br>Type: buoy<br>Waves: None<br>Wind: None<br>Temp: None",
            "style_hint": {},
            "source_table": "stations",
            "source_id": "ST1",
            "is_maritime": False,
            "range_ring_meters": None,
            "wave_height_m": None,
            "wind_ms": None,
            "temp_C": None,
            "wave_height_display": "None",
            "wind_display": "None",
            "temp_display": "None",
        }
    ]
    df = pd.DataFrame(data)
    return df

if __name__ == "__main__":
    df = build_golden_df()
    out_csv = Path("data/debugging/golden_gis_map_inputs_df.csv")
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)
    print(f"Golden GIS DataFrame written to: {out_csv}")

    # Call the mapper
    result = generate_gis_map_html_from_dfs(
        gis_map_inputs_df=df,
        out_dir="data/maps/TEST",
        site_id="TEST"
    )
    print("Mapper result:")
    print(result)