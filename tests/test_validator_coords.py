import pytest
from app.field_validator import (
    validate_and_extract_coordinate_token,
    validate_and_extract_coordinate_pair
)
from flask_app.app.preparse_coordinate_mapper import pre_scan_for_coordinates

def approx(val, ref, tol=1e-4):
    return abs(val - ref) < tol

def test_lat_token_minutes_only_ok():
    res = validate_and_extract_coordinate_token("latitude", "37 45.600N", {}, None)
    assert res["valid"] is True
    assert approx(res["value"], 37 + 45.6/60)
    assert res["confidence"] > 0.8

def test_lon_token_minutes_only_ok():
    res = validate_and_extract_coordinate_token("longitude", "075 30.200W", {}, None)
    assert res["valid"] is True
    assert approx(res["value"], -(75 + 30.2/60))
    assert res["confidence"] > 0.8

def test_token_with_seconds_ok():
    res = validate_and_extract_coordinate_token("latitude", "47 06 36.0N", {}, None)
    expected = 47 + 6/60 + 36/3600
    assert res["valid"] is True
    assert approx(res["value"], expected)
    assert res["confidence"] > 0.8

def test_pair_order_flexible():
    res = validate_and_extract_coordinate_pair("coordinate_pair", "075 30.200W 37 45.600N", {}, None)
    assert res["valid"] is True
    assert approx(res["value"]["lat_dd"], 37 + 45.6/60)
    assert approx(res["value"]["lon_dd"], -(75 + 30.2/60))

def test_malformed_minutes_only_lon_missing_deg():
    res = validate_and_extract_coordinate_token("longitude", "30.200W", {}, None)
    assert res["valid"] is False
    notes = ";".join(res.get("notes", []))
    assert "degree" in notes or "struct" in notes or "missing" in notes

def test_range_out_of_bounds():
    res = validate_and_extract_coordinate_token("latitude", "99 00.0N", {}, None)
    assert res["valid"] is False
    notes = ";".join(res.get("notes", []))
    assert ("out of range" in notes) or ("range: out of bounds" in notes)


@pytest.mark.parametrize(
    ("field_name", "token"),
    [
        ("latitude", "37 99.000N"),
        ("longitude", "075 75.000W"),
        ("latitude", "47 06 99.0N"),
    ],
)
def test_dms_components_must_be_below_sixty(field_name, token):
    res = validate_and_extract_coordinate_token(field_name, token, {}, None)

    assert res["valid"] is False
    assert "components" in res["checks_failed"]
    assert "out of bounds" in ";".join(res["notes"])


@pytest.mark.parametrize(
    "coordinate_pair",
    [
        "99 00.000N 075 30.000W",
        "37 99.000N 075 30.000W",
        "37 45.000N 181 00.000W",
    ],
)
def test_coordinate_pair_requires_both_tokens_to_be_valid(coordinate_pair):
    res = validate_and_extract_coordinate_pair(
        "coordinate_pair", coordinate_pair, {}, None
    )

    assert res["valid"] is False
    assert res["is_valid"] is False


def test_coordinate_prescan_excludes_invalid_component_pair():
    result = pre_scan_for_coordinates("POSITION: 37 99.000N 075 30.000W")

    assert result.empty
