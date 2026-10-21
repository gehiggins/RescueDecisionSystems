# tests/test_mapper_golden_df.py
try:
    from app.gis_mapping import (
        _satellite_orbit_class,
        _style_bool,
        first_notna,
        generate_gis_map_html_from_dfs,
    )
except ModuleNotFoundError:
    import os, sys
    from pathlib import Path
    repo_root = Path(__file__).resolve().parents[1]
    flask_app_dir = repo_root / "flask_app"
    if str(flask_app_dir) not in sys.path:
        sys.path.insert(0, str(flask_app_dir))
    from app.gis_mapping import (
        _satellite_orbit_class,
        _style_bool,
        first_notna,
        generate_gis_map_html_from_dfs,
    )

import pandas as pd
import os
import base64
import pytest


def test_satellite_style_boolean_normalization():
    assert _style_bool("false") is False
    assert _style_bool(" OFF ") is False
    assert _style_bool("true") is True
    assert _style_bool(0) is False
    assert _style_bool(1) is True
    assert _style_bool("unexpected", default=True) is True


@pytest.mark.parametrize("malformed", [[], {}, [1, 2]])
def test_first_notna_skips_non_scalar_values(malformed):
    row = {"preferred": malformed, "fallback": "usable"}

    assert first_notna(row, ["preferred", "fallback"]) == "usable"


@pytest.mark.parametrize(
    ("sat_type", "label", "expected"),
    [
        ("LEO", "ignored", "leo"),
        ("MEO", "ignored", "meo"),
        ("GEO", "ignored", "geo"),
        (None, "MEO rescue satellite", "meo"),
        (pd.NA, "GEO coverage", "geo"),
        ("", "unknown satellite", "leo"),
        ("unknown", "GEO-looking label", "leo"),
    ],
)
def test_satellite_orbit_layer_classification(sat_type, label, expected):
    assert _satellite_orbit_class(sat_type, label) == expected

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
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4"
            "z8DwHwAFAAH/iZk9HQAAAABJRU5ErkJggg=="
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


@pytest.mark.parametrize("opacity", [float("nan"), float("inf"), "bad", []])
def test_renderer_defaults_malformed_image_overlay_opacity(tmp_path, opacity):
    image_path = tmp_path / "coverage.png"
    image_path.write_bytes(
        base64.b64decode(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4"
            "z8DwHwAFAAH/iZk9HQAAAABJRU5ErkJggg=="
        )
    )
    output_path = tmp_path / "map.html"
    inputs = pd.DataFrame(
        [
            {
                "site_id": "TEST",
                "layer": "satellite_overlay",
                "geometry": {
                    "type": "ImageOverlay",
                    "image_path": str(image_path),
                    "bounds": [[35.0, -78.0], [40.0, -72.0]],
                    "opacity": opacity,
                },
                "label": "Coverage image",
            }
        ]
    )

    result = generate_gis_map_html_from_dfs(
        inputs,
        alert_row={
            "site_id": "TEST",
            "position_lat_dd_a": 37.5,
            "position_lon_dd_a": -75.0,
        },
        out_path=str(output_path),
    )

    html = output_path.read_text(encoding="utf-8")
    assert result["status"] == "ok"
    assert '"opacity": 0.6' in html
    assert '"opacity": NaN' not in html
    assert '"opacity": Infinity' not in html


@pytest.mark.parametrize("image_path", [True, [], {"path": "bad"}, "bad\x00path"])
def test_renderer_skips_malformed_image_overlay_path(tmp_path, image_path):
    output_path = tmp_path / "map.html"
    inputs = pd.DataFrame(
        [
            {
                "site_id": "TEST",
                "layer": "satellite_overlay",
                "geometry": {
                    "type": "ImageOverlay",
                    "image_path": image_path,
                    "bounds": [[35.0, -78.0], [40.0, -72.0]],
                },
                "label": "Malformed coverage image",
            },
            {
                "site_id": "TEST",
                "layer": "alert_position",
                "geometry": {"type": "Point", "coordinates": [-75.0, 37.5]},
                "label": "Alert A",
            },
        ]
    )

    result = generate_gis_map_html_from_dfs(
        inputs,
        alert_row={
            "site_id": "TEST",
            "position_lat_dd_a": 37.5,
            "position_lon_dd_a": -75.0,
        },
        out_path=str(output_path),
    )

    html = output_path.read_text(encoding="utf-8")
    assert result["status"] == "ok"
    assert "Alert A" in html
    assert "Malformed coverage image" not in html


@pytest.mark.parametrize("overlay_name", [[], {"name": "bad"}, ["a", "b"]])
def test_renderer_handles_malformed_image_overlay_name(
    tmp_path, overlay_name
):
    image_path = tmp_path / "coverage.png"
    image_path.write_bytes(
        base64.b64decode(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4"
            "z8DwHwAFAAH/iZk9HQAAAABJRU5ErkJggg=="
        )
    )
    output_path = tmp_path / "map.html"
    inputs = pd.DataFrame(
        [
            {
                "site_id": "TEST",
                "layer": "satellite_overlay",
                "geometry": {
                    "type": "ImageOverlay",
                    "image_path": str(image_path),
                    "bounds": [[35.0, -78.0], [40.0, -72.0]],
                    "name": overlay_name,
                },
                "label": "Coverage fallback",
            }
        ]
    )

    result = generate_gis_map_html_from_dfs(
        inputs,
        alert_row={
            "site_id": "TEST",
            "position_lat_dd_a": 37.5,
            "position_lon_dd_a": -75.0,
        },
        out_path=str(output_path),
    )

    html = output_path.read_text(encoding="utf-8")
    assert result["status"] == "ok"
    assert "Satellite imagery" in html
    assert "data:image/png;base64" in html


def test_renderer_skips_unreadable_image_overlay_content(tmp_path, caplog):
    image_path = tmp_path / "coverage.png"
    image_path.write_bytes(b"not a valid image")
    output_path = tmp_path / "map.html"
    inputs = pd.DataFrame(
        [
            {
                "site_id": "TEST",
                "layer": "satellite_overlay",
                "geometry": {
                    "type": "ImageOverlay",
                    "image_path": str(image_path),
                    "bounds": [[35.0, -78.0], [40.0, -72.0]],
                },
                "label": "Broken coverage image",
            },
            {
                "site_id": "TEST",
                "layer": "alert_position",
                "geometry": {"type": "Point", "coordinates": [-75.0, 37.5]},
                "label": "Alert A",
            },
        ]
    )

    result = generate_gis_map_html_from_dfs(
        inputs,
        alert_row={
            "site_id": "TEST",
            "position_lat_dd_a": 37.5,
            "position_lon_dd_a": -75.0,
        },
        out_path=str(output_path),
    )

    html = output_path.read_text(encoding="utf-8")
    assert result["status"] == "ok"
    assert "Alert A" in html
    assert "data:image/png;base64" not in html
    assert "Skipping unreadable satellite image overlay" in caplog.text


def test_renderer_skips_symlinked_image_overlay(tmp_path):
    target_path = tmp_path / "coverage-target.png"
    target_path.write_bytes(
        base64.b64decode(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4"
            "z8DwHwAFAAH/iZk9HQAAAABJRU5ErkJggg=="
        )
    )
    image_path = tmp_path / "coverage.png"
    image_path.symlink_to(target_path)
    output_path = tmp_path / "map.html"
    inputs = pd.DataFrame(
        [
            {
                "site_id": "TEST",
                "layer": "satellite_overlay",
                "geometry": {
                    "type": "ImageOverlay",
                    "image_path": str(image_path),
                    "bounds": [[35.0, -78.0], [40.0, -72.0]],
                },
                "label": "Linked coverage image",
            },
            {
                "site_id": "TEST",
                "layer": "alert_position",
                "geometry": {"type": "Point", "coordinates": [-75.0, 37.5]},
                "label": "Alert A",
            },
        ]
    )

    result = generate_gis_map_html_from_dfs(
        inputs,
        alert_row={
            "site_id": "TEST",
            "position_lat_dd_a": 37.5,
            "position_lon_dd_a": -75.0,
        },
        out_path=str(output_path),
    )

    html = output_path.read_text(encoding="utf-8")
    assert result["status"] == "ok"
    assert "Alert A" in html
    assert "data:image/png;base64" not in html


def test_renderer_skips_image_overlay_with_reversed_bounds(tmp_path):
    image_path = tmp_path / "coverage.png"
    image_path.write_bytes(
        base64.b64decode(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4"
            "z8DwHwAFAAH/iZk9HQAAAABJRU5ErkJggg=="
        )
    )
    output_path = tmp_path / "map.html"
    inputs = pd.DataFrame(
        [
            {
                "layer": "satellite_overlay",
                "geom_type": "ImageOverlay",
                "geometry": {
                    "type": "ImageOverlay",
                    "image_path": str(image_path),
                    "bounds": [[40.0, -78.0], [35.0, -72.0]],
                    "name": "Reversed coverage",
                },
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
    assert "Reversed coverage" not in html
    assert "data:image/png;base64" not in html


def test_renderer_omits_empty_satellite_layer_controls(tmp_path):
    output_path = tmp_path / "satellite-layers.html"
    inputs = pd.DataFrame(
        [
            {
                "layer": "satellite_overlay",
                "geom_type": "Point",
                "geometry": {"type": "Point", "coordinates": [-75.0, 37.5]},
                "label": "LEO next pass",
                "sat_type": "LEO",
            }
        ]
    )

    generate_gis_map_html_from_dfs(
        inputs,
        alert_row={"site_id": "TEST", "latitude_a": 37.5, "longitude_a": -75.0},
        out_path=str(output_path),
    )

    html = output_path.read_text(encoding="utf-8")
    assert r"Satellites \u2022 LEO" in html
    assert "Satellite footprints" not in html
    assert "Satellite tracks" not in html
    assert "Next-pass markers" not in html


def test_renderer_includes_layer_control_without_satellite_rows(tmp_path):
    output_path = tmp_path / "weather-layers.html"
    inputs = pd.DataFrame(
        [
            {
                "layer": "weather",
                "geom_type": "Point",
                "geometry": {"type": "Point", "coordinates": [-75.0, 37.5]},
                "label": "Weather",
            }
        ]
    )

    result = generate_gis_map_html_from_dfs(
        inputs,
        alert_row={"site_id": "TEST", "latitude_a": 37.5, "longitude_a": -75.0},
        out_path=str(output_path),
    )

    html = output_path.read_text(encoding="utf-8")
    assert result["layers"] == ["Weather"]
    assert '"Weather"' in html
    assert "L.control.layers" in html


def test_renderer_preserves_zero_coordinates_for_map_center(tmp_path):
    output_path = tmp_path / "zero-center.html"

    generate_gis_map_html_from_dfs(
        pd.DataFrame(columns=["layer"]),
        alert_row={
            "site_id": "ZERO",
            "position_lat_dd_a": 0.0,
            "position_lon_dd_a": 0.0,
        },
        out_path=str(output_path),
    )

    html = output_path.read_text(encoding="utf-8")
    assert "center: [0.0, 0.0]" in html


def test_renderer_centers_on_active_parser_coordinate_fields(tmp_path):
    output_path = tmp_path / "parser-center.html"

    generate_gis_map_html_from_dfs(
        pd.DataFrame(columns=["layer"]),
        alert_row={
            "site_id": "PARSER",
            "latitude_a": 12.25,
            "longitude_a": 145.75,
        },
        out_path=str(output_path),
    )

    html = output_path.read_text(encoding="utf-8")
    assert "center: [12.25, 145.75]" in html


def test_renderer_centers_on_first_valid_alert_geometry(tmp_path):
    output_path = tmp_path / "geometry-center.html"
    inputs = pd.DataFrame(
        [
            {
                "layer": "alert_position",
                "geometry": {"type": "Point", "coordinates": [-122.4, 37.8]},
                "label": "Alert B",
            }
        ]
    )

    generate_gis_map_html_from_dfs(
        inputs,
        alert_row={"site_id": "GEOMETRY"},
        out_path=str(output_path),
    )

    html = output_path.read_text(encoding="utf-8")
    assert "center: [37.8, -122.4]" in html


def test_renderer_escapes_site_id_in_map_title(tmp_path):
    output_path = tmp_path / "escaped-title.html"
    unsafe_site_id = '<script>alert("map")</script>'

    generate_gis_map_html_from_dfs(
        pd.DataFrame(columns=["layer"]),
        alert_row={
            "site_id": unsafe_site_id,
            "position_lat_dd_a": 10.0,
            "position_lon_dd_a": 20.0,
        },
        out_path=str(output_path),
    )

    html = output_path.read_text(encoding="utf-8")
    assert unsafe_site_id not in html
    assert "&lt;script&gt;alert(&quot;map&quot;)&lt;/script&gt;" in html


def test_renderer_skips_malformed_and_out_of_range_geometry(tmp_path):
    output_path = tmp_path / "resilient-map.html"
    inputs = pd.DataFrame(
        [
            {
                "layer": "alert_position",
                "geometry": {"type": "Point", "coordinates": ["bad"]},
                "label": "Bad alert",
            },
            {
                "layer": "weather",
                "geometry": {"type": "Point", "coordinates": [-75.0, 95.0]},
                "label": "Bad weather",
            },
            {
                "layer": "station",
                "geometry": {"type": "Point", "coordinates": [float("inf"), 37.5]},
                "label": "Bad station",
            },
            {
                "layer": "range_ring",
                "geometry": {
                    "type": "Circle",
                    "center": [-75.0, 37.5],
                    "radius_m": "bad",
                },
            },
            {
                "layer": "satellite_overlay",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [[-75.2, 37.4], [181.0, 37.5], [-75.0, 37.6]],
                },
                "label": "Partially valid track",
            },
            {
                "layer": "satellite_overlay",
                "geometry": {"type": "Point", "coordinates": ["bad", 37.5]},
                "label": "Bad next pass",
            },
            {
                "layer": "alert_position",
                "geometry": {"type": "Point", "coordinates": [True, 37.5]},
                "label": "Boolean alert",
            },
            {
                "layer": "range_ring",
                "geometry": {
                    "type": "Circle",
                    "center": [-75.0, 37.5],
                    "radius_m": True,
                },
                "label": "Boolean ring",
            },
        ]
    )

    result = generate_gis_map_html_from_dfs(
        inputs,
        alert_row={"site_id": "BOUNDARY", "position_lat_dd_a": "bad", "position_lon_dd_a": -75.0},
        out_path=str(output_path),
    )

    html = output_path.read_text(encoding="utf-8")
    assert result["status"] == "ok"
    assert "Partially valid track" in html
    assert "Bad alert" not in html
    assert "Bad weather" not in html
    assert "Bad station" not in html
    assert "Bad next pass" not in html
    assert "Boolean alert" not in html
    assert "Boolean ring" not in html


def test_renderer_skips_malformed_satellite_track_coordinate_collections(tmp_path):
    output_path = tmp_path / "malformed-tracks.html"
    inputs = pd.DataFrame(
        [
            {
                "layer": "satellite_overlay",
                "geometry": {"type": "LineString", "coordinates": pd.NA},
                "label": "Missing-coordinate track",
            },
            {
                "layer": "satellite_overlay",
                "geometry": {
                    "type": "LineString",
                    "coordinates": pd.Series([[-75.0, 37.5], [-74.5, 38.0]]),
                },
                "label": "Series-coordinate track",
            },
        ]
    )

    result = generate_gis_map_html_from_dfs(
        inputs,
        alert_row={"site_id": "TRACKS"},
        out_path=str(output_path),
    )

    html = output_path.read_text(encoding="utf-8")
    assert result["status"] == "ok"
    assert "Missing-coordinate track" not in html
    assert "Series-coordinate track" not in html


def test_renderer_accepts_series_metadata_and_escapes_position_label(tmp_path):
    output_path = tmp_path / "series-row.html"
    unsafe_label = '<img src=x onerror="alert(1)">'
    inputs = pd.DataFrame(
        [
            {
                "layer": "alert_position",
                "geometry": {"type": "Point", "coordinates": [-75.0, 37.5]},
                "label": unsafe_label,
            }
        ]
    )

    result = generate_gis_map_html_from_dfs(
        inputs,
        alert_row=pd.Series(
            {"site_id": "SERIES", "position_lat_dd_a": 37.5, "position_lon_dd_a": -75.0}
        ),
        out_path=str(output_path),
    )

    html = output_path.read_text(encoding="utf-8")
    assert result["status"] == "ok"
    assert unsafe_label not in html
    assert "&lt;img src=x onerror=&quot;alert(1)&quot;&gt;" in html


def test_renderer_falls_back_from_missing_explicit_site_id(tmp_path):
    output_path = tmp_path / "missing-explicit-site-id.html"

    result = generate_gis_map_html_from_dfs(
        pd.DataFrame(columns=["layer"]),
        alert_row=pd.Series({"site_id": "FALLBACK"}),
        site_id=pd.NA,
        out_path=str(output_path),
    )

    html = output_path.read_text(encoding="utf-8")
    assert result["status"] == "ok"
    assert result["site_id"] == "FALLBACK"
    assert "RDS Alert Map: FALLBACK" in html


def test_renderer_handles_missing_site_id_and_non_mapping_metadata(tmp_path):
    output_path = tmp_path / "unknown-site-id.html"

    result = generate_gis_map_html_from_dfs(
        pd.DataFrame(columns=["layer"]),
        alert_row=["not", "metadata"],
        site_id=float("nan"),
        out_path=str(output_path),
    )

    html = output_path.read_text(encoding="utf-8")
    assert result["status"] == "ok"
    assert result["site_id"] == "unknown"
    assert "RDS Alert Map: unknown" in html


def test_renderer_handles_missing_layer_column(tmp_path):
    output_path = tmp_path / "missing-layer.html"

    result = generate_gis_map_html_from_dfs(
        pd.DataFrame([{"label": "not a layer"}]),
        alert_row={"site_id": "EMPTY", "position_lat_dd_a": 0.0, "position_lon_dd_a": 0.0},
        out_path=str(output_path),
    )

    assert result["status"] == "ok"
    assert result["layers"] == []


def test_renderer_escapes_external_station_tooltip(tmp_path):
    output_path = tmp_path / "safe-station.html"
    unsafe = '<img src=x onerror="alert(1)">'
    inputs = pd.DataFrame(
        [
            {
                "layer": "station",
                "geometry": {"type": "Point", "coordinates": [-75.0, 37.5]},
                "label": unsafe,
                "popup_html": "Safe popup",
            }
        ]
    )

    result = generate_gis_map_html_from_dfs(
        inputs,
        alert_row={"site_id": "SAFE", "position_lat_dd_a": 37.5, "position_lon_dd_a": -75.0},
        out_path=str(output_path),
    )

    html = output_path.read_text(encoding="utf-8")
    assert result["status"] == "ok"
    assert unsafe not in html
    assert "&lt;img src=x onerror=&quot;alert(1)&quot;&gt;" in html


def test_renderer_defaults_malformed_satellite_styles(tmp_path):
    output_path = tmp_path / "style-resilient-map.html"
    inputs = pd.DataFrame(
        [
            {
                "site_id": "TEST",
                "layer": "satellite_overlay",
                "geom_type": "Circle",
                "geometry": {
                    "type": "Circle",
                    "center": [-75.0, 37.5],
                    "radius_m": 1000.0,
                    "style": {
                        "weight": "bad",
                        "fillOpacity": float("inf"),
                        "color": None,
                    },
                },
                "label": "Bad style circle",
                "sat_type": "LEO",
            },
            {
                "site_id": "TEST",
                "layer": "satellite_overlay",
                "geom_type": "LineString",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [[-75.0, 37.5], [-74.0, 38.0]],
                    "style": {"weight": [], "opacity": "bad", "color": ""},
                },
                "label": "Bad style track",
                "sat_type": "LEO",
            },
        ]
    )

    result = generate_gis_map_html_from_dfs(
        inputs,
        alert_row={
            "site_id": "TEST",
            "position_lat_dd_a": 37.5,
            "position_lon_dd_a": -75.0,
        },
        out_path=str(output_path),
    )

    html = output_path.read_text(encoding="utf-8")
    assert result["status"] == "ok"
    assert "Satellite Overlays" in result["layers"]
    assert "Bad style circle" in html
    assert "Bad style track" in html
