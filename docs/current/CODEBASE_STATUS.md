# RDS codebase status

Status: verified current

Reviewed against code and runtime: 2026-10-20

Last verified maintenance update: 2026-10-20

Baseline: `main` commit `4e9b127c3a949b51f45b894c964a93ed6675c1fb`

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

Live Open-Meteo retrieval was exercised for both sample positions on
2026-10-20. The full controller produced two weather rows and an HTML map with
alert-position, range-ring, and weather layers. Open-Meteo is explicitly asked
for metres-per-second wind values to match the `wind_ms` schema; the verified
sample displayed 19 kt and 31 kt after this correction, rather than the
incorrect 70 kt and 111 kt produced when the provider's default km/h values
were treated as m/s. During the initial 2026-10-19 review, a current-time
NOAA-15 test exercised CelesTrak TLE retrieval, Skyfield propagation,
satellite footprints, and map overlays. A direct CelesTrak check on 2026-10-20
timed out, so current
TLE-provider availability was not reconfirmed; cached TLEs remain usable and
provider failures now enter a short backoff instead of causing a retry cascade.

Satellite propagation uses the alert detection time. TLE data more than 168
hours from the alert time is rejected by default rather than used to draw a
potentially misleading position. The limit is configurable through
`RDS_SAT_MAX_TLE_AGE_HOURS`. Invalid, non-finite, zero, or negative limits
fall back to the 168-hour safeguard.

Coordinate validation rejects latitude/longitude pairs unless both tokens are
valid. Minutes and seconds must remain below 60, hemisphere letters must match
the coordinate type, and signed decimal degrees cannot also carry a cardinal
suffix that could reverse the intended hemisphere.

The browser now reports processing state, returned layers, and weather and
satellite row counts. Invalid or excessive alert requests are rejected before
pipeline work starts, and the direct map endpoint rejects non-finite or
out-of-range coordinates. Generated map serving is confined to the configured
map directory, including when a requested file is a symbolic link.
Pipeline-generated map and satellite-overlay directories also use normalized,
bounded path components when a numeric site ID is unavailable. The normalized
identifier is retained through the final map write, so raw alert text cannot
escape the configured map directory.

Validated local satellite PNG overlays are loaded only with finite geographic
bounds and non-symlinked image/sidecar files. They are embedded into the
generated HTML map as georeferenced satellite imagery. Equator and
prime-meridian coordinates remain valid map centers, and untrusted site IDs
are HTML-escaped in the map title.

Detailed pipeline failures are logged on the server but are not returned to
the browser. Raised exceptions, malformed integration results, malformed JSON
requests, missing reported map files, and direct map-rendering failures retain
stable JSON error responses. Pandas missing values serialize as JSON `null`.
Non-finite parsed numeric values serialize as JSON `null`, so strict clients
do not receive non-standard `NaN` or `Infinity` tokens. The browser also
handles non-JSON proxy or server failures without displaying a JSON parsing
error.

The web application adds frame, content-type, referrer, and no-store response
headers where appropriate. Its four public pages render with valid references
to committed static assets.

Diagnostic alert CSVs are disabled during normal operation. They can be
enabled deliberately with `RDS_DEBUG_DUMP=1`; `RDS_DEBUG_DIR` selects their
output directory. Diagnostic filenames are confined to that directory and
symbolic-link output targets are rejected before writing. Untimestamped
map-input transforms remain offline instead of silently contacting the
satellite service.

Active data and projection defaults no longer depend on one developer's file
system. Repository-relative paths are used by default, with environment
variables available for deliberate overrides. Legacy TLE and sample-message
utilities are quiet on import and perform file or network work only when
called explicitly. The NOAA diagnostic and weather health utility likewise do
not contact a provider or change application-wide logging merely when imported.

Open-Meteo and Meteostat retrieval are isolated so an unexpected failure in
one provider does not discard valid observations from the other. Weather-row
reduction preserves the source identifier used to associate observations with
stations. Missing or non-finite measurements and invalid timestamps remain
absent instead of aborting map generation. Invalid A/B and direct provider
reference coordinates are rejected independently, while valid targets
continue through the weather pipeline. Empty, null, or unsupported weather
target inputs return the canonical empty observation schema without contacting
a provider.

Open-Meteo keeps usable timestamp rows when individual provider arrays are
short or malformed. Meteostat's documented km/h wind and gust values are
converted to the shared m/s schema, invalid station metadata is filtered per
row, and valid stations remain available. Invalid provider time windows are
rejected before network lookup. Shared weather calculations accept finite
scalar numeric strings and reject malformed, array-like, and non-finite
values.

Satellite context enrichment isolates visible-satellite, upcoming-pass, and
nearest-satellite failures. Empty provider results preserve the overlay schema;
incomplete satellite metadata does not discard valid geometry. Satellite
subpoints can render without a footprint radius, and malformed track vertices
or next-pass coordinates are removed individually. Missing, array-like, or
malformed alert timestamps return an empty satellite overlay before reference
or TLE-provider work begins. Satellite propagation boundaries normalize valid
times to UTC and reject malformed timestamps or out-of-range alert coordinates
before invoking Skyfield. Malformed or empty constellation responses are not
cached as successful TLE lookups, allowing a later provider recovery to be
retried.

Coordinate source spans identify the exact matched DMS or decimal-degree token
pair and preserve offsets across LF, CRLF, and CR line endings. Coordinate
interpretation rules themselves were not changed by that maintenance.

Map inputs accept only finite, in-range coordinates and positive finite range
rings. Numeric range-ring text is normalized; malformed radii are omitted
without stopping other layers.

The map renderer independently validates point, ring, track, and satellite
geometry, so malformed rows cannot abort otherwise valid layers. External
station and provider display values are escaped before entering map popup and
tooltip HTML. Alert finalization normalizes site identifiers, ignores invalid
historical sequence values, and assigns distinct increasing sequence numbers
to repeated sites in the same batch. Shared UTC slicing accepts naive, aware,
and mixed-offset timestamps without comparing incompatible indexes. Shared
coordinate utilities enforce pole and antimeridian bounds even when called
outside the primary alert validator.

Database modules can be imported without a database URL. Database setup is
lazy and explicit, and missing configuration produces a clear error if a
database operation is actually requested. A retired local database credential
was removed from the active tree; it may still exist in Git history and should
be rotated if it was ever used. History was not rewritten during this work.

## Automated verification

- Default offline suite: 384 passed; 2 live tests deselected.
- Explicit live weather suite: 2 passed on 2026-10-20. Live checks remain
  outside per-commit CI. A direct CelesTrak check timed out that day, so no
  current TLE-provider success is claimed for this baseline.
- Editable development installation succeeded.
- The deployment requirements wrapper installed successfully.
- `pip check` reported no broken requirements.
- Gunicorn application configuration validation passed.
- GitHub Actions repeats the installation, dependency, active-module
  compilation, offline-test, and Gunicorn checks on Python 3.9 and Python 3.11
  for each pull request and push to `main`.

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
- Python 3.9 is the local installation environment verified during this review;
  continuous verification covers both Python 3.9 and Python 3.11.
- That macOS Python 3.9 environment emits third-party compatibility warnings
  for LibreSSL/urllib3 and the installed Matplotlib/pyparsing versions. They do
  not currently fail the offline or live suites, but a newer Python runtime and
  dependency refresh remain advisable before deployment.
- No production deployment was performed as part of this verification.

## Deferred capabilities

The proposed operational visualization, globe, and SRR coordination/chat
capabilities are recorded in
`docs/future/OPERATIONAL_VISUALIZATION_AND_SRR_COORDINATION.md`. They remain
deferred until the existing processing product is sufficiently complete and
stable.
