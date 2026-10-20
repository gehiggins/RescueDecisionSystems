import pytest
from app.field_validator import (
    FIELD_CONFIG,
    validate_and_extract,
    validate_and_extract_coordinate_token,
    validate_and_extract_coordinate_pair
)
from flask_app.app.preparse_coordinate_mapper import pre_scan_for_coordinates

def approx(val, ref, tol=1e-4):
    return abs(val - ref) < tol


def test_generic_validator_tries_configured_fallback_patterns():
    result = validate_and_extract(
        "beacon_id",
        "BEACON ID: ABCDEF123456",
        FIELD_CONFIG["beacon_id"],
    )

    assert result["valid"] is True
    assert result["value"] == "ABCDEF123456"
    assert result["fallback_used"] is True


def test_generic_validator_reports_failure_after_all_patterns_miss():
    result = validate_and_extract(
        "beacon_id", "BEACON ID: not-a-hex-id", FIELD_CONFIG["beacon_id"]
    )

    assert result["valid"] is False
    assert result["value"] is None
    assert result["checks_failed"] == ["pattern_no_match"]
    assert sum("did not match" in note for note in result["notes"]) == 2

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


@pytest.mark.parametrize(
    ("field_name", "token"),
    [
        ("latitude", "-37.500S"),
        ("latitude", "-37.500N"),
        ("longitude", "-075.500W"),
        ("longitude", "-075.500E"),
    ],
)
def test_signed_decimal_degree_with_cardinal_is_rejected(field_name, token):
    res = validate_and_extract_coordinate_token(field_name, token, {}, None)

    assert res["valid"] is False


@pytest.mark.parametrize(
    ("field_name", "token"),
    [
        ("latitude", "37.500E"),
        ("longitude", "075.500N"),
    ],
)
def test_coordinate_token_requires_matching_hemisphere(field_name, token):
    res = validate_and_extract_coordinate_token(field_name, token, {}, None)

    assert res["valid"] is False
    assert "hemisphere" in res["checks_failed"]


def test_coordinate_prescan_excludes_signed_cardinal_pair():
    result = pre_scan_for_coordinates("POSITION: -37.500S -075.500W")

    assert result.empty


def test_dms_pair_reports_exact_source_span():
    line = 'POSITION: 37° 30\' 00"N 075° 30\' 00"W TRAILER'
    expected = '37° 30\' 00"N 075° 30\' 00"W'

    result = validate_and_extract_coordinate_pair(
        "coordinate_pair", line, {}, None
    )

    assert result["valid"] is True
    assert line[result["start_pos"] : result["end_pos"]] == expected


def test_coordinate_prescan_preserves_offsets_after_crlf_lines():
    expected = '37° 30\' 00"N 075° 30\' 00"W'
    message = f"HEADER\r\nPOSITION: {expected} TRAILER\r\nEND"

    result = pre_scan_for_coordinates(message)

    assert len(result) == 1
    row = result.iloc[0]
    assert message[int(row["start_pos"]) : int(row["end_pos"])] == expected
