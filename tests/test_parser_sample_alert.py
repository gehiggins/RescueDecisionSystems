from pathlib import Path

import pandas as pd
import pytest

from flask_app.app.parser_sarsat_msg import parse_sarsat_message
from flask_app.app.preparse_coordinate_mapper import pre_scan_for_coordinates


def test_sample_alert_parses_exact_beacon_and_detection_time():
    repository_root = Path(__file__).resolve().parents[1]
    message = (repository_root / "sample_sarsat_message.txt").read_text(
        encoding="utf-8-sig"
    )

    parsed = parse_sarsat_message(message, pre_scan_for_coordinates(message))

    assert parsed["beacon_id"] == "2DCE6 12345 ABCFF"
    assert parsed["site_id"] == "98372"
    assert parsed["detect_time"] == pd.Timestamp("2025-03-02T13:32:17Z")
    assert parsed["satellite_name"] == "GOES-16"
    assert parsed["satellite_id"] == "GOES-16"
    assert parsed["detection_frequency"] == pytest.approx(406.0285)
    assert parsed["lut_id"] == "US LUT - Wallops Island"
    assert parsed["message_type"] == "STANDARD LOCATION PROTOCOL"
    assert parsed["national_location_protocol"] == "US"
    assert parsed["position_device"] == "INTERNAL"
    assert parsed["position_resolution"] == "4 SECONDS"


def test_satellite_id_requires_its_own_labeled_line():
    message = (
        "BEACON ID: ABC123 SITE ID: 42\n"
        "NOTE: PRIOR SATELLITE ID: UNRELATED\n"
        "SATELLITE ID: GOES-16\n"
        "A 37 45.600N 075 30.200W"
    )

    parsed = parse_sarsat_message(message, pre_scan_for_coordinates(message))

    assert parsed["satellite_name"] == "GOES-16"


def test_position_metadata_requires_its_own_labeled_lines():
    message = (
        "BEACON ID: ABC123 SITE ID: 42\n"
        "NOTE: POSITION DEVICE: EXTERNAL\n"
        "POSITION DEVICE: INTERNAL\n"
        "POSITION RESOLUTION: 4 SECONDS\n"
        "A 37 45.600N 075 30.200W"
    )

    parsed = parse_sarsat_message(message, pre_scan_for_coordinates(message))

    assert parsed["position_device"] == "INTERNAL"
    assert parsed["position_resolution"] == "4 SECONDS"


def test_operational_metadata_requires_labeled_lines():
    message = (
        "BEACON ID: ABC123 SITE ID: 42\n"
        "NOTE: LUT ID: WRONG\n"
        "DETECTION FREQUENCY: 406.0285 MHZ\n"
        "LUT ID: US LUT - Wallops Island\n"
        "MESSAGE TYPE: STANDARD LOCATION PROTOCOL\n"
        "NATIONAL LOCATION PROTOCOL: US\n"
        "A 37 45.600N 075 30.200W"
    )

    parsed = parse_sarsat_message(message, pre_scan_for_coordinates(message))

    assert parsed["detection_frequency"] == pytest.approx(406.0285)
    assert parsed["lut_id"] == "US LUT - Wallops Island"
    assert parsed["message_type"] == "STANDARD LOCATION PROTOCOL"
    assert parsed["national_location_protocol"] == "US"


def test_parser_retains_two_positions_when_they_share_one_line():
    message = (
        "BEACON ID: ABC123 SITE ID: 42\n"
        "POSITIONS: A 37 45.600N 075 30.200W "
        "B 38 15.300N 070 12.500W"
    )

    parsed = parse_sarsat_message(message, pre_scan_for_coordinates(message))

    assert parsed is not None
    assert parsed["latitude_a"] == pytest.approx(37.76)
    assert parsed["longitude_a"] == pytest.approx(-75.50333333333333)
    assert parsed["latitude_b"] == pytest.approx(38.255)
    assert parsed["longitude_b"] == pytest.approx(-70.20833333333333)


def _coordinate_scan(**metadata):
    row = {
        "is_valid": True,
        "lat_dd": 37.5,
        "lon_dd": -75.5,
        "format_type": "Decimal Degrees",
        "confidence": 0.9,
    }
    row.update(metadata)
    return pd.DataFrame([row])


@pytest.mark.parametrize(
    "expected_error_nm",
    ["bad", pd.NA, float("nan"), float("inf"), -1, 0],
)
def test_invalid_optional_expected_error_does_not_discard_alert(
    expected_error_nm, caplog
):
    message = "BEACON ID: ABC123\nSITE ID: 42"

    parsed = parse_sarsat_message(
        message,
        _coordinate_scan(expected_error_nm=expected_error_nm),
    )

    assert parsed is not None
    assert parsed["beacon_id"] == "ABC123"
    assert parsed["site_id"] == "42"
    assert parsed["expected_error_nm"] is None
    assert parsed["range_ring_meters_a"] == 5000.0
    assert parsed["range_ring_source_a"] == "fallback_default"


def test_valid_text_expected_error_is_normalized_to_range_ring():
    parsed = parse_sarsat_message(
        "BEACON ID: ABC123\nSITE ID: 42",
        _coordinate_scan(expected_error_nm="2.5 nautical miles"),
    )

    assert parsed["expected_error_nm"] == 2.5
    assert parsed["range_ring_meters_a"] == 4630.0
    assert parsed["range_ring_source_a"] == "EE_95"


def test_missing_optional_metadata_does_not_replace_message_identifiers():
    parsed = parse_sarsat_message(
        "BEACON ID: ABC123\nSITE ID: 42",
        _coordinate_scan(
            beacon_id=pd.NA,
            site_id=pd.NA,
            position_method=pd.NA,
            position_resolution=pd.NA,
        ),
    )

    assert parsed is not None
    assert parsed["beacon_id"] == "ABC123"
    assert parsed["site_id"] == "42"
    assert parsed["position_method"] is None
    assert parsed["position_resolution"] is None
    assert parsed["range_ring_source_a"] == "fallback_default"


@pytest.mark.parametrize(
    "pre_scan_time",
    ["not-a-time", ["2026-10-20T12:00:00Z"], 12345, float("inf")],
)
def test_invalid_prescan_detection_time_falls_back_to_message(
    pre_scan_time, caplog
):
    message = (
        "BEACON ID: ABC123\n"
        "SITE ID: 42\n"
        "DATE DETECTED: 20 OCT 2026\n"
        "TIME DETECTED: 12:34:56 UTC"
    )

    parsed = parse_sarsat_message(
        message,
        _coordinate_scan(detect_time=pre_scan_time),
    )

    assert parsed is not None
    assert parsed["detect_time"] == pd.Timestamp("2026-10-20T12:34:56Z")
    assert "Ignoring invalid pre-scan detection time" in caplog.text


def test_valid_prescan_detection_time_is_normalized_to_utc():
    parsed = parse_sarsat_message(
        "BEACON ID: ABC123\nSITE ID: 42",
        _coordinate_scan(detect_time="2026-10-20T08:34:56-04:00"),
    )

    assert parsed["detect_time"] == pd.Timestamp("2026-10-20T12:34:56Z")
