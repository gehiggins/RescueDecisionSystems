import pytest

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
