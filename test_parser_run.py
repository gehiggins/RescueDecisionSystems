#test_parser_run.py

from flask_app.setup_imports import *
from flask_app.app.sarsat_parser import parse_sarsat_message

def main():
    with open('sample_sarsat_message.txt', 'r') as f:
        raw_message = f.read()

    parsed_alert = parse_sarsat_message(raw_message)

    # Convert parsed data to DataFrame (simulating pipeline behavior)
    alert_df = pd.DataFrame([parsed_alert])

    print("✅ Parsed Alert DataFrame:")
    print(alert_df)

if __name__ == '__main__':
    main()
