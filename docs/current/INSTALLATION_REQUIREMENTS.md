# Installation requirements

Status: verified current

Reviewed and installation-tested: 2026-10-19

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
  environment.
- Installed `flask_app/requirements.txt` from the repository root and confirmed
  that the Gunicorn server dependency was included.
- Imported and exercised the Flask application, GIS renderer, live weather
  path, and Skyfield satellite propagation path.
- Ran the default offline suite after installation.

Live weather and TLE retrieval require outbound internet access. Production
deployment configuration is intentionally outside this installation check.
