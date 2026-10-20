# Installation requirements

Status: verified current

Reviewed and installation-tested: 2026-10-19

Last verified maintenance update: 2026-10-20

Continuous verification baseline: `main` commit
`666c75b95aed29a808bacef3655fa2fd8d804d97` (2026-10-20)

Supersedes: `docs/archive/2025/RDS_Installation_Requirements_2025-02-28.md`

## Supported installation paths

Create and activate a Python virtual environment from the repository root.

For development and tests:

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[test]'
```

For the web application with the Gunicorn server dependency:

```bash
.venv/bin/pip install -r flask_app/requirements.txt
```

`setup.py` is the authoritative dependency declaration.
`flask_app/requirements.txt` is a small deployment wrapper that installs this
project with its `server` extra; it does not maintain a second dependency list.

## Optional integrations

Install optional packages only when their corresponding integration is needed:

```bash
.venv/bin/pip install -e '.[weather]'
.venv/bin/pip install -e '.[timezone]'
```

- `weather` installs Meteostat support. Open-Meteo remains available through
  the core installation.
- `timezone` installs `timezonefinder` for coordinate-derived local timezones.

PostgreSQL persistence requires a reachable database and `DATABASE_URL`.
Parsing, live weather/satellite enrichment, and map generation work without a
database; alert sequence numbering then starts from an empty local history.

## Verification performed

- Installed the editable project with the `test` extra in a Python 3.9 virtual
  environment. Continuous verification also exercises Python 3.11.
- Installed `flask_app/requirements.txt` from the repository root and confirmed
  that the Gunicorn server dependency was included.
- Imported and exercised the Flask application, GIS renderer, live weather
  path, and Skyfield satellite propagation path.
- Ran the default offline suite after installation: 670 passed, with the two
  explicitly marked live-provider tests deselected.
- Ran the explicit live-provider suite: 2 passed on 2026-10-20.
- Ran the committed sample alert through the full controller and map writer;
  it produced two live weather rows and the alert, range-ring, and weather map
  layers. CelesTrak timed out during that run, and the pipeline completed
  without satellite rows as designed.

Live weather and TLE retrieval require outbound internet access. Production
deployment configuration is intentionally outside this installation check.

`RDS_SAT_REQUEST_TIMEOUT_SECONDS` optionally controls the per-request
CelesTrak timeout. It defaults to 10 seconds; invalid, non-finite, zero, or
negative values fall back to that default.

## Continuous verification

GitHub Actions runs the supported Python 3.9 and Python 3.11 installations,
dependency checks, active-module compilation, offline test suites, and
Gunicorn configuration checks for every pull request and every update to
`main`. Live external-service tests remain deliberately separate so temporary
provider outages do not block unrelated code changes.
