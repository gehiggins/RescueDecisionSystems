# RDS codebase status

Status: verified current

Reviewed against code and runtime: 2026-10-20

Last verified maintenance update: 2026-10-20

Baseline: `main` commit `61fea367dbc1e2c0c12d57328b8a5a24c6440a09`

Historical document dates are preserved in: `docs/archive/2025/README.md`

## Current working product path

The active browser workflow is the Flask application created by
`flask_app.app.create_app`.

Submitting a message to `POST /process_alert` now runs the integrated alert
pipeline:

1. pre-scan and validate A/B coordinates;
2. parse beacon, site, detection time, reporting satellite, operational
   metadata, error radius, and position fields;
3. finalize the alert sequence data;
4. retrieve and normalize available weather observations;
5. build satellite overlays only when suitable TLE data is available;
6. assemble one `gis_map_inputs_df`;
7. render an HTML map under the configured map directory; and
8. return parsed fields, map URL, layer names, and weather/satellite row counts.

The map URL returned by the API is served by the Flask application. Database
configuration is not required for parsing, enrichment, or map generation.

An installed development checkout provides the `run_rds` command. It binds to
the loopback interface and disables Flask debug mode by default; a developer
must explicitly request another host or enable debug mode. This launcher is for
local development, not production deployment.

## Verified behavior

Using the committed `sample_sarsat_message.txt`, the web workflow correctly
returns:

- beacon ID `2DCE6 12345 ABCFF`;
- site ID `98372`;
- detection time `2025-03-02T13:32:17Z`;
- reporting satellite `GOES-16` (NORAD catalog 41866 / international
  designator `2016-071A` in the fallback reference);
- detection frequency `406.0285 MHz` and LUT `US LUT - Wallops Island`;
- position device `INTERNAL` and resolution `4 SECONDS`;
- message type `STANDARD LOCATION PROTOCOL` and national location protocol
  `US`;
- both A and B positions;
- 2 NM / 3,704 metre error rings; and
- a served HTML map.

An offline end-to-end regression now submits that committed sample through the
real `POST /process_alert` route, while replacing only external providers and
diagnostic writes. It verifies the parser output, reporting-satellite handoff,
map layers, generated map URL, served Leaflet HTML, and no-store response
behavior together. The ambiguous `PROBABILITY: 95 A 5 B` source line is not
mapped to distress probability because the repository does not yet establish
that interpretation.

Live historical Open-Meteo retrieval was exercised for both sample positions
on 2026-10-20 after alert-time weather selection was connected. The full
controller used the 2025 detection time, produced two historical-weather rows,
and rendered an HTML map with both alert positions, range rings, and weather
markers. A direct 2020 request through the RDS fetcher also returned the exact
requested hour with finite wind data. The controller centers weather retrieval
on the parsed alert detection time. Recent alerts use the live forecast
service, archived alerts from 2022 onward use the historical-forecast service,
and earlier alerts use the long-term historical weather archive. Open-Meteo is
explicitly asked for metres-per-second wind values to match the `wind_ms`
schema. During the initial 2026-10-19 review, a current-time NOAA-15 test
exercised CelesTrak TLE retrieval, Skyfield propagation, satellite footprints,
and map overlays. A direct CelesTrak check on 2026-10-20 timed out, so current
TLE-provider availability was not reconfirmed; cached TLEs remain usable and
provider failures enter a short backoff instead of causing a retry cascade.

Satellite propagation uses the alert detection time. TLE data more than 168
hours from the alert time is rejected by default rather than used to draw a
potentially misleading position. The limit is configurable through
`RDS_SAT_MAX_TLE_AGE_HOURS`. Invalid, non-finite, zero, or negative limits
fall back to the 168-hour safeguard. Stale elements are rejected before RDS
calculates a subpoint, ground track, or next-pass marker. When a resolved
satellite reference and an explicit alert NORAD ID are both present, the alert
override is applied without truth-testing the pandas reference record. Alert,
reference, and provider catalog IDs must be positive whole numbers; malformed,
fractional, boolean, or non-scalar values do not trigger TLE lookup. Nullable
or non-scalar display names fall back to usable designator data without
stopping enrichment.

Coordinate validation rejects latitude/longitude pairs unless both tokens are
valid. Minutes and seconds must remain below 60, hemisphere letters must match
the coordinate type, and signed decimal degrees cannot also carry a cardinal
suffix that could reverse the intended hemisphere.

The browser now reports processing state, returned layers, and weather and
satellite row counts. It clears the preceding alert map when a new request
starts, displays valid maps built from either position A or B, and does not
require the original site ID when the controller safely generates one. Alerts
with no parsed position do not present the renderer's fallback center as an
alert location; changing that behavior remains a product decision. Invalid or
excessive alert requests are rejected before pipeline work starts, and the
direct map endpoint rejects non-finite or out-of-range coordinates. Generated
map serving is confined to the configured map directory, including when a
requested file is a symbolic link.
The public map route serves only generated HTML maps. Invalid, empty, or
overly broad map-directory settings fall back to the repository's dedicated
`data/maps` directory. Alert-character and HTTP request-body limits accept
positive whole-number configuration values and fall back to their safe
defaults when a setting is malformed, non-finite, fractional, boolean, zero,
or negative.
Pipeline-generated map and satellite-overlay directories also use normalized,
bounded path components when a numeric site ID is unavailable. The normalized
identifier is retained through the final map write, so raw alert text cannot
escape the configured map directory.

Validated local satellite PNG overlays are loaded only with ordered, finite,
in-range geographic bounds and non-symlinked image/sidecar files. The loader,
map-input builder, and renderer apply the same boundary rule, so reversed or
malformed coverage cannot reach the displayed map. Valid images are embedded
into the generated HTML map as georeferenced satellite imagery. Image opacity
is finite and constrained from zero to one. Malformed or filesystem-rejected
image paths are skipped without stopping valid alert layers from rendering.
The renderer independently rejects symlinked files, verifies that image
content is a readable PNG, and treats non-scalar image names as missing.
Satellite footprints, tracks, and pass markers share one orbit-layer classifier; explicit
LEO, MEO, and GEO metadata is preserved, while missing or unknown types use the
documented LEO default instead of being mislabeled as GEO.

Map layer controls are available even when satellite enrichment is absent, so
Weather and Stations remain independently toggleable on degraded-data maps.
Unused satellite footprint, track, and next-pass controls were removed; actual
satellite features remain grouped under populated LEO, MEO, GEO, and imagery
controls.

The map renderer recognizes both normalized coordinate fields and the
`latitude_a`/`longitude_a` names emitted by the active parser. It keeps each
latitude/longitude candidate paired, then falls back to position B or the
first valid alert geometry. Valid alerts therefore open centered on their
plotted location rather than the coordinate-less default. Equator and
prime-meridian coordinates remain valid map centers, and untrusted site IDs
are HTML-escaped in the map title.

Detailed pipeline failures are logged on the server but are not returned to
the browser. Raised exceptions, malformed integration results, malformed JSON
requests, missing reported map files, invalid reported map-path types, and
direct map-rendering failures retain stable JSON error responses. Pandas
missing values serialize as JSON `null`.
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
The satellite GIS, TLE, NOAA, weather, and Meteostat diagnostic scripts default
to deterministic offline checks; live provider access requires an explicit
command-line option. Golden GIS fixture generation requires an explicit output
path and does not overwrite the committed fixture by default.

The import-rewrite maintenance helper previews proposed `from app.` namespace
changes by default. Applying changes requires an explicit `--apply` option; the
helper excludes archives and notebook checkpoints, ignores comments and string
literals, and uses atomic permission-preserving file replacement. The broader
application namespace migration has not been performed.

The standalone coastline-distance utility now resolves the committed Natural
Earth data relative to the repository, validates its inputs, and calculates in
a local metre-based projection. A representative 37.5, -74.5 check returned
84.87 km. This utility is not connected to maritime policy decisions in the
active alert pipeline.

The explicit TLE downloader constrains output names, identifies and bounds its
requests, validates provider payloads, and uses atomic file replacement without
following output-file symbolic links. An explicitly empty source selection is
a no-op. These safeguards were verified offline; no current provider download
is claimed for this baseline.

The optional NDBC buoy adapter now normalizes usable coordinates from the
committed station metadata, retrieves preferred observations with fallback,
and preserves nearby-station context when observations are unavailable. The
retired NOAA weather compatibility interface imports successfully and delegates
offshore requests to that maintained adapter, but it remains disabled in the
active production path. The NDBC metadata refresh script uses repository-
relative outputs, declared project dependencies, bounded requests, and explicit
command-line output options; its live multi-station refresh was not run during
this maintenance batch, so the committed reference dataset was not replaced.

The NDBC realtime2 and 5day2 parsers now read field names from the provider's
header row rather than the units row, so valid temperature, wind, and wave
measurements are retained. Their requests are bounded and identify the RDS
weather client. Parser selection is limited to observations from the preceding
60 minutes; stale multi-hour rows and future-dated rows are rejected.
Non-finite and malformed numeric provider tokens remain absent rather than
becoming temperature, wind, direction, or wave measurements. A read-only live
check of station 44009 on 2026-10-20 returned a current observation and
populated temperature, wind speed, and wind direction through the repaired
parser. Negative wind speeds and wave heights, and directions outside 0–360
degrees, are also excluded from the shared weather schema. NDBC metadata and
direct lookups accept only normalized station-code identifiers. Validation is
enforced by the shared realtime2 and 5day2 parsers, so empty, path-like, and
query-like identifiers do not reach provider requests through direct callers.

The optional NOAA shore adapter no longer returns Seattle's KSEA station for
every alert. It discovers NWS observation stations around the requested point,
validates and ranks them by distance, and preserves the shared Celsius and
metres-per-second schema. A live sample-position check on 2026-10-20 returned
KWAL, KMFV, and KSBY in increasing distance order. NOAA active-alert clients
now use the documented `/alerts/active/zone/{zone_id}` route through a shared,
bounded lookup. A live check resolved the sample position to marine zone
ANZ652 and returned one active alert. These repaired legacy adapters remain
outside the active production path. NOAA discovery and observation lookups
likewise accept only normalized station-code identifiers and discard malformed
provider entries independently. Station discovery also validates that provider
feature collections, metadata objects, and GeoJSON coordinate containers have
the expected shapes. Boolean, non-finite, and out-of-range station coordinates
are discarded entry by entry without interrupting other station results.

NOAA active-alert coordinates are checked for malformed, non-finite, and
out-of-range values before any provider request. Point and alert response
objects are validated at their boundaries; a non-list `features` value is
treated as no alerts. Structured no-alert results retain the same eight-column
schema as populated results, while compatibility headlines skip nullable or
non-text entries independently.

The central empty DataFrame schemas now initialize successfully with current
pandas. Timestamp columns are UTC-aware, while list and coordinate-container
fields use pandas object columns. An isolation audit imported every active
`flask_app/app` module and every `rds_engine` module successfully.

Open-Meteo and Meteostat retrieval are isolated so an unexpected failure in
one provider does not discard valid observations from the other. Weather-row
reduction preserves the source identifier used to associate observations with
stations. Missing or non-finite measurements and invalid timestamps remain
absent instead of aborting map generation. Invalid A/B and direct provider
reference coordinates are rejected independently, while valid targets
continue through the weather pipeline. Empty, null, or unsupported weather
target inputs return the canonical empty observation schema without contacting
a provider.
Boolean and NumPy-boolean values are rejected at weather coordinate and numeric
configuration boundaries instead of being interpreted as zero or one. NOAA and
NDBC station-count controls require finite positive whole numbers and retain
their existing upper bounds; malformed NDBC limits use the ten-buoy default,
while invalid NOAA limits skip the provider request.
NOAA station, NOAA alert, and NDBC observation request timeouts require finite
positive values; invalid settings fall back to each provider's established
bounded default.
Meteostat station searches are capped at 100 candidates before provider access.
Finite weather lookback values that exceed Python's supported datetime range
fall back to the established six-hour window instead of aborting the alert.
The standalone weather health check applies the same six-hour overflow fallback
and the same 100-station provider ceiling, and reports the effective values it
actually used.

Weather policy is evaluated independently for every valid alert position.
When A and B have identical coordinates, RDS performs one provider lookup but
retains the most conservative combined metadata: the largest available error
radius and the smallest available coast distance. Invalid optional policy
metadata is omitted rather than passed downstream.
The reusable context boundary also rejects malformed, non-finite, boolean, and
out-of-range coordinates before classification. The standalone observation
reducer applies the same provider-ID validation as the full aggregator. Weather
health summaries normalize mixed-offset observation times to UTC and report an
unknown span when no valid timestamp remains.

Open-Meteo keeps usable timestamp rows when individual provider arrays are
short or malformed. Meteostat's documented km/h wind and gust values are
converted to the shared m/s schema, invalid station metadata is filtered per
row, and valid stations remain available. Invalid provider time windows are
rejected before network lookup. Shared weather calculations accept finite
scalar numeric strings and reject malformed, array-like, and non-finite
values. Open-Meteo atmospheric and marine values are range-checked before
entering the shared schema: negative wind, gust, pressure, precipitation,
wave-height, and wave-period values are omitted, as are percentages and
directions outside their physical ranges. Invalid humidity cannot produce a
derived dew point. Open-Meteo also parses conventional true/false text for its
marine-data option, so a false-like string cannot accidentally enable the
marine request; unrecognized values log a warning and use the existing enabled
default. Meteostat now applies matching finite and physical range
checks to its temperature, dew-point, humidity, wind, direction, pressure, and
precipitation fields before creating shared weather rows. Malformed Meteostat
station identifiers are skipped independently so valid neighboring stations
can still be queried.

Satellite context enrichment isolates visible-satellite, upcoming-pass, and
nearest-satellite failures. Empty provider results preserve the overlay schema;
incomplete satellite metadata does not discard valid geometry. Satellite
subpoints can render without a footprint radius, and malformed track vertices
or next-pass coordinates are removed individually. Malformed alert coordinates
skip only the optional next-pass calculation while preserving valid subpoint
and footprint output. Missing, array-like, or malformed alert timestamps return
an empty satellite overlay before reference or TLE-provider work begins.
Satellite propagation boundaries normalize scalar times to UTC and reject
malformed or array-like provider epochs and out-of-range alert coordinates
before invoking Skyfield. Primary subpoint exceptions and malformed propagated
coordinates or altitudes degrade to an empty overlay instead of stopping alert
processing. Visible, upcoming, and nearest candidate paths filter malformed or
duplicate catalog identifiers before provider lookup and apply the same epoch
and subpoint validation to returned rows. Malformed or empty constellation
responses are not cached as successful TLE lookups, allowing a later provider
recovery to be retried.

Coordinate source spans identify the exact matched DMS or decimal-degree token
pair and preserve offsets across irregular whitespace and LF, CRLF, and CR
line endings. Multiple valid A/B coordinate pairs on one line are collected in
source order and retained by the parser. Coordinate interpretation rules
themselves were not changed by that maintenance.

Map inputs accept only finite, in-range coordinates and positive finite range
rings. Numeric range-ring text is normalized; malformed radii are omitted
without stopping other layers.

The map renderer independently validates point, ring, track, and satellite
geometry, so malformed rows cannot abort otherwise valid layers. External
station and provider display values are escaped before entering map popup and
tooltip HTML. Shared map field fallbacks skip non-scalar malformed values and
continue to later usable fields. Alert finalization normalizes site identifiers,
ignores invalid
historical sequence values, parses exact integer sequence text without
floating-point rounding, and assigns distinct increasing sequence numbers to
repeated sites in the same batch. Shared UTC slicing accepts naive, aware,
and mixed-offset timestamps without comparing incompatible indexes. Shared
coordinate utilities enforce pole and antimeridian bounds even when called
outside the primary alert validator.

Database modules can be imported without a database URL. Database setup is
lazy and explicit, and missing configuration produces a clear error if a
database operation is actually requested. A retired local database credential
was removed from the active tree; it may still exist in Git history and should
be rotated if it was ever used. History was not rewritten during this work.

## Automated verification

- Default offline suite: 730 passed; 2 live tests deselected.
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
