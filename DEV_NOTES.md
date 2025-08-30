# RescueDecisionSystems – Dev Map

## How to run
```bash
conda activate unified_env
python -m flask_app.app.pipeline_controller

# Project Map (High-Signal Files)

- **flask_app/app/preparse_coordinate_mapper.py**  
  Pre-scan of SARSAT messages; emits `data/debugging/debug_preparsed_coordinates.csv`.  
  🔗 Validator integration point: where coordinate rows are appended.

- **flask_app/app/parser_sarsat_msg.py**  
  Main structured parser; sets A/B positions + metadata.  
  🔗 Validator integration point: inside `parse_sarsat_message(...)`, at the PROB EE SOL LATITUDE/LONGITUDE section.

- **flask_app/app/utils_coordinates.py**  
  Coordinate utilities (regex / clean / convert).

- **flask_app/app/gis_mapping.py**  
  Folium map rendering.

- **flask_app/app/pipeline_controller.py**  
  Orchestrates: parse → map → DB.

- **data/debugging/debug_preparsed_coordinates.csv**  
  Output of preparse scan for QA.

## Data Paths
- Debug CSV: `data/debugging/debug_preparsed_coordinates.csv`
- Shapefiles: `data/shapefiles/...`
- Map output: `data/maps/`

## Integration Plan for Validator
- **Pre-scan**: swap ad-hoc validity checks → `validate_and_extract()` so invalid-but-detected pairs still get logged.  
  File: `flask_app/app/preparse_coordinate_mapper.py`
- **Main parse**: wrap PROB EE SOL LAT/LON logic with `validate_and_extract()`.  
  File: `flask_app/app/parser_sarsat_msg.py`

## New Module
- `flask_app/app/field_validator.py`  
  API: `validate_and_extract(field_name, raw_text, config, context=None)`  
  Returns: `value, raw_span, valid, confidence, checks_passed, checks_failed, fallback_used, notes`.