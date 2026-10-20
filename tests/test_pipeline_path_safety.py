from pathlib import Path

import pandas as pd
import pytest

from flask_app.app import pipeline_controller
from flask_app.app.pipeline_controller import _map_output_path, _safe_map_site_id


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        ("98372", "98372"),
        ("BEACON_2DCE6 12345 ABCFF", "BEACON_2DCE6_12345_ABCFF"),
        ("../../outside", "outside"),
        ("A/B\\C", "A_B_C"),
        ("..", "unknown"),
    ],
)
def test_map_site_id_is_safe_filename_component(raw_value, expected):
    assert _safe_map_site_id(raw_value) == expected


def test_map_site_id_length_is_bounded():
    assert len(_safe_map_site_id("A" * 200)) == 80


def test_satellite_overlay_site_symlink_cannot_escape_root(monkeypatch, tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    image_path = outside / "coverage.png"
    image_path.write_bytes(b"png")
    image_path.with_suffix(".bounds.json").write_text(
        "[[35.0, -78.0], [40.0, -72.0]]", encoding="utf-8"
    )

    overlays_root = tmp_path / "data" / "sat_overlays"
    overlays_root.mkdir(parents=True)
    site_link = overlays_root / "98372"
    try:
        site_link.symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("directory symlinks are unavailable on this platform")
    monkeypatch.setattr(
        pipeline_controller,
        "__file__",
        str(tmp_path / "flask_app" / "app" / "pipeline_controller.py"),
    )

    assert pipeline_controller.load_sat_overlays_for_site("98372") == []


def test_map_output_site_symlink_cannot_escape_root(tmp_path):
    maps_root = tmp_path / "maps"
    maps_root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    site_link = maps_root / "98372"
    try:
        site_link.symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("directory symlinks are unavailable on this platform")

    with pytest.raises(ValueError, match="symbolic link"):
        _map_output_path(maps_root, "98372")

    assert not (outside / "gis_map_98372.html").exists()


def test_pipeline_uses_safe_site_id_for_final_map_path(monkeypatch, tmp_path):
    parsed = {
        "site_id": "../../outside",
        "beacon_id": "TEST",
        "expected_error_nm": None,
        "range_ring_meters_a": 0.0,
        "range_ring_meters_b": 0.0,
        "position_method": None,
        "format_type_a": None,
        "format_type_b": None,
        "latitude_a": 37.5,
        "longitude_a": -75.5,
        "latitude_b": None,
        "longitude_b": None,
        "position_status_a": "valid",
        "position_status_b": None,
    }
    finalized = pd.DataFrame(
        [
            {
                **parsed,
                "position_lat_dd_a": 37.5,
                "position_lon_dd_a": -75.5,
                "position_lat_dd_b": pd.NA,
                "position_lon_dd_b": pd.NA,
                "detect_time": pd.Timestamp("2026-10-20T06:00:00Z"),
            }
        ]
    )
    rendered_paths = []

    monkeypatch.setattr(pipeline_controller, "pre_scan_for_coordinates", lambda _m: [])
    monkeypatch.setattr(
        pipeline_controller,
        "parse_sarsat_message",
        lambda _m, pre_scan_results: parsed,
    )
    monkeypatch.setattr(
        pipeline_controller, "finalize_alert_df", lambda _df, _existing: finalized
    )
    monkeypatch.setattr(
        pipeline_controller, "run_wx_pipeline", lambda **_kwargs: pd.DataFrame()
    )
    monkeypatch.setattr(
        pipeline_controller,
        "build_gis_map_inputs_df",
        lambda *_args, **_kwargs: pd.DataFrame([{"layer": "position"}]),
    )

    def record_map_path(_inputs, _alert, output_path):
        rendered_paths.append(Path(output_path))
        return {"map_html_path": output_path, "layers": ["position"]}

    monkeypatch.setattr(
        pipeline_controller, "generate_gis_map_html_from_dfs", record_map_path
    )
    maps_root = tmp_path / "maps"

    result = pipeline_controller.process_sarsat_alert("alert", maps_root=maps_root)

    expected = maps_root / "outside" / "gis_map_outside.html"
    assert result["status"] == "ok"
    assert rendered_paths == [expected]
    assert expected.parent.is_dir()
    assert not (tmp_path / "outside").exists()


def test_pipeline_handles_nullable_identifiers_and_uses_legacy_position(
    monkeypatch, tmp_path
):
    parsed = {
        "site_id": None,
        "beacon_id": None,
        "expected_error_nm": None,
        "range_ring_meters_a": None,
        "range_ring_meters_b": None,
        "position_method": None,
        "format_type_a": None,
        "format_type_b": None,
        "latitude_a": 37.5,
        "longitude_a": -75.5,
        "latitude_b": None,
        "longitude_b": None,
        "position_status_a": "valid",
        "position_status_b": None,
    }
    finalized = pd.DataFrame(
        [
            {
                **parsed,
                "site_id": pd.NA,
                "beacon_id": pd.NA,
                "alert_sequence_number": 7,
                "position_lat_dd_a": pd.NA,
                "position_lon_dd_a": pd.NA,
                "position_method": pd.NA,
                "position_method_a": "GNSS",
                "range_ring_meters_a": "bad",
                "detect_time": pd.NaT,
            }
        ]
    )
    captured = {}

    monkeypatch.setattr(pipeline_controller, "pre_scan_for_coordinates", lambda _m: [])
    monkeypatch.setattr(
        pipeline_controller,
        "parse_sarsat_message",
        lambda _m, pre_scan_results: parsed,
    )
    monkeypatch.setattr(
        pipeline_controller, "finalize_alert_df", lambda _df, _existing: finalized
    )
    monkeypatch.setattr(
        pipeline_controller, "run_wx_pipeline", lambda **_kwargs: pd.DataFrame()
    )

    def capture_inputs(positions_df, **_kwargs):
        captured["positions"] = positions_df.copy()
        return pd.DataFrame([{"layer": "alert_position"}])

    monkeypatch.setattr(
        pipeline_controller, "build_gis_map_inputs_df", capture_inputs
    )
    monkeypatch.setattr(
        pipeline_controller,
        "generate_gis_map_html_from_dfs",
        lambda _inputs, _alert, output_path: {
            "map_html_path": output_path,
            "layers": ["Alert Positions"],
        },
    )

    result = pipeline_controller.process_sarsat_alert(
        "alert", maps_root=tmp_path / "maps"
    )

    assert result["status"] == "ok"
    assert Path(result["map_html_path"]).parent.name == "ALERT_7"
    position = captured["positions"].iloc[0]
    assert position["lat_dd"] == 37.5
    assert position["lon_dd"] == -75.5
    assert position["method"] == "GNSS"
    assert position["range_ring_meters"] == 0.0
