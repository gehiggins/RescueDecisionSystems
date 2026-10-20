from pathlib import Path

import pandas as pd
import pytest

from flask_app.app import pipeline_controller
from flask_app.app.pipeline_controller import _safe_map_site_id


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
