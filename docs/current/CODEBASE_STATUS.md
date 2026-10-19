# RDS codebase status

Status: verified current

Reviewed against code: 2026-10-19

Baseline: `main` commit `10e41ddb54e85ab17543f339137f7f1da2d78f26`

Historical document dates preserved in: `docs/archive/2025/README.md`

## What is implemented

The active processing path is centered on
`flask_app/app/pipeline_controller.py::process_sarsat_alert`. By direct source
inspection, it currently:

1. pre-scans coordinates with `pre_scan_for_coordinates`;
2. parses the SARSAT message with `parse_sarsat_message`;
3. constructs and finalizes an `alert_df`;
4. derives A/B position rows and invokes the weather sub-pipeline;
5. assembles one `gis_map_inputs_df`; and
6. renders a Folium HTML map under `data/maps/<site_id>/`.

The repository also contains implemented components for:

- coordinate validation and parsing (`field_validator.py`,
  `preparse_coordinate_mapper.py`, `parser_sarsat_msg.py`);
- weather policy and aggregation (`rds_engine/`, `wx_pipeline.py`, `wx_obs.py`,
  Open-Meteo and optional Meteostat fetchers);
- unified GIS input assembly and rendering (`gis_map_inputs_builder.py`,
  `gis_mapping.py`); and
- TLE lookup, Skyfield propagation, footprint calculation, and satellite
  overlay construction (`sat_fetch_tle.py`, `sat_compute_groundtrack.py`,
  `sat_compute_footprint.py`, `sat_pipeline.py`).

All 63 Python files in the active source/test paths parse successfully when
read with their committed UTF-8 BOMs. That is a syntax-only check, not a runtime
or integration test.

## Important current limitations

- The database-save and legacy NOAA fetch block in `process_sarsat_alert` is
  enclosed in a triple-quoted block and is not executed.
- If no saved satellite image overlay exists, the controller injects a
  hard-coded `TEST Overlay` line. Therefore, seeing a satellite layer on a map
  does not by itself prove live satellite integration.
- `gis_map_inputs_builder.py` can call `sat_pipeline.py`, but it constructs a
  minimal alert using the current time. The audited code does not demonstrate
  an end-to-end guarantee that a reporting satellite is propagated at the
  original alert timestamp.
- The canonical map-frame name in code is `gis_map_inputs_df`, not
  `map_feature_df`.
- `sat_routes.py`, `gis_feature_builder.py`, `weather_data_selection.py`, and
  `realtime_analysis.py` do not exist in the active source tree. Earlier guides
  that describe them as active are superseded.
- `flask_app/run.py` imports `create_app` from the `app` package, but
  `flask_app/app/__init__.py` is empty. `routes.py` also imports missing modules
  named `parser`, `gis_plot`, and `weather_fetch`. The web entry point is
  therefore not certified as runnable.
- The root script path in `pipeline_controller.py` is a developer-specific
  Windows path. Running that file directly without supplying another input is
  not portable.
- Dependency declarations are inconsistent and incomplete; see
  `INSTALLATION_REQUIREMENTS.md`.

## Verification performed

- Compared every document previously stored in `docs/` with the active source
  tree and module names.
- Inspected the current orchestration, weather, satellite, GIS, Flask, database,
  packaging, and test files.
- Parsed 63 active Python files with Python's AST parser; no active-source syntax
  errors were found.
- Confirmed the historical `flask_app/app/local archive/satellite_tracker.py`
  has a syntax error. It is not part of the active source assessment.

Runtime tests were not executed because the audit environment contains neither
`pytest` nor the project runtime dependencies. No claim of runtime correctness,
external API availability, database connectivity, or deployment readiness is
made.

## Documentation disposition

The v0/v0.2 documents were superseded by later drafts. The v0.5 documents are
useful design records but mix implemented behavior with proposed contracts and
missing modules. They were archived without content edits rather than receiving
a 2026 review stamp that could be mistaken for technical certification.
