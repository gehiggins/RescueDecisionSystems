"""Run the active SARSAT pre-scan and parser against one message file."""

from pathlib import Path
import argparse

import pandas as pd

from flask_app.app.parser_sarsat_msg import parse_sarsat_message
from flask_app.app.preparse_coordinate_mapper import pre_scan_for_coordinates


DEFAULT_MESSAGE = Path(__file__).resolve().parent / "sample_sarsat_message.txt"


def main(argv=None):
    parser = argparse.ArgumentParser(description="Smoke-test the active RDS parser")
    parser.add_argument(
        "message_file",
        nargs="?",
        type=Path,
        default=DEFAULT_MESSAGE,
        help="SARSAT message file (defaults to the committed sample)",
    )
    args = parser.parse_args(argv)

    raw_message = args.message_file.read_text(encoding="utf-8", errors="replace")
    coordinates = pre_scan_for_coordinates(raw_message)
    parsed_alert = parse_sarsat_message(raw_message, coordinates)
    if not isinstance(parsed_alert, dict):
        raise SystemExit("Parser smoke failed: no parsed alert returned")

    alert_df = pd.DataFrame([parsed_alert])
    print("Parsed alert:")
    print(alert_df.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
