from pathlib import Path

import pandas as pd

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
