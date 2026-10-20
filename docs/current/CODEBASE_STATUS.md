# RDS codebase status

Status: verified current

Reviewed against code and runtime: 2026-10-19

Baseline: `main` commit `01444a3c9bb777f62e37785175f9f82bdee3269d`

Historical document dates are preserved in: `docs/archive/2025/README.md`

## Current working product path

The active browser workflow is the Flask application created by
`flask_app.app.create_app`.

Submitting a message to `POST /process_alert` now runs the integrated alert
pipeline:

1. pre-scan and validate A/B coordinates;
2. parse beacon, site, detection time, error radius, and position fields;
3. finalize the alert sequence data;
4. retrieve and normalize available weather observations;
5. build satellite overlays only when suitable TLE data is available;
6. assemble one `gis_map_inputs_df`;
7. render an HTML map under the configured map directory; and
8. return parsed fields, map URL, layer names, and weather/satellite row counts.

The map URL returned by the API is served by the Flask application. Database
configuration is not required for parsing, enrichment, or map generation.

## Verified behavior

Using the committed `sample_sarsat_message.txt`, the web workflow correctly
returns:

- beacon ID `2DCE6 12345 ABCFF`;
- site ID `98372`;
- detection time `2025-03-02T13:32:17Z`;
- both A and B positions;
- 2 NM / 3,704 metre error rings; and
- a served HTML map.

Live Open-Meteo retrieval was exercised for both sample positions. A
current-time NOAA-15 test exercised CelesTrak TLE retrieval, Skyfield
propagation, satellite footprints, and map overlays.

Satellite propagation uses the alert detection time. TLE data more than 168
hours from the alert time is rejected by default rather than used to draw a
potentially misleading position. The limit is configurable through
`RDS_SAT_MAX_TLE_AGE_HOURS`.

## Automated verification

- Default offline suite: 37 passed; 2 live tests deselected.
- Explicit external-service suite: 2 passed.
- Editable development installation succeeded.
- The deployment requirements wrapper installed successfully.
- `pip check` reported no broken requirements.
- Gunicorn application configuration validation passed.

The default and live test commands are documented in `pytest.ini` and
`INSTALLATION_REQUIREMENTS.md`.

## Important current limitations

- Database saving remains disabled in the legacy triple-quoted block in
  `process_sarsat_alert`; current web processing is not persistent.
- When `DATABASE_URL` is absent or unavailable, sequence assignment starts from
  an empty local history for each processing run.
- Historical satellite overlays require TLE data close enough to the alert
  time. The current CelesTrak fetch path supplies current TLEs, not an archive
  of historical element sets.
- Meteostat station observations and coordinate-derived timezone lookup remain
  optional extras. The core Open-Meteo path does not require them.
- Weather and TLE enrichment depend on their external services. If data is
  unavailable, RDS continues with the alert positions and range rings it can
  verify.
- Python 3.9 is the installation environment verified during this review.
- No production deployment was performed as part of this verification.

## Deferred capabilities

The proposed operational visualization, globe, and SRR coordination/chat
capabilities are recorded in
`docs/future/OPERATIONAL_VISUALIZATION_AND_SRR_COORDINATION.md`. They remain
deferred until the existing processing product is sufficiently complete and
stable.
