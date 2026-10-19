# Installation requirements audit

Status: verified current inventory; environment not validated

Reviewed against code: 2026-10-19

Baseline: `main` commit `10e41ddb54e85ab17543f339137f7f1da2d78f26`

Supersedes: `docs/archive/2025/RDS_Installation_Requirements_2025-02-28.md`

## Current manifest state

The repository does not yet have one complete, reproducible dependency
manifest.

`setup.py` declares Flask, Flask-Login, SQLAlchemy, psycopg2-binary, pandas,
`numpy<1.27`, `scipy>=1.10`, scikit-learn, geopandas, shapely, contextily,
Pillow, requests, and python-dotenv.

`flask_app/requirements.txt` separately declares Flask, Flask-SQLAlchemy,
psycopg2-binary, requests, and gunicorn. It is not a complete replacement for
`setup.py`.

Direct imports in active code add dependencies that are not declared in
`setup.py`, including:

- `folium` and `matplotlib` for GIS rendering;
- `geopy` and `joblib`, imported by the shared setup module;
- `skyfield` for satellite propagation;
- optional `timezonefinder` for local timezone lookup;
- optional `meteostat` for station observations;
- `beautifulsoup4` for the NDBC metadata-generation utility; and
- `pytest` for the test suite.

Skyfield supplies the SGP4 propagation path used by the satellite code. Its
transitive dependencies should not be relied on as implicit declarations in a
reproducible environment.

## What can be stated safely

- The repository advertises `pip install -e .`, but `setup.py` alone is
  insufficient for all active imports.
- The two manifest files should be reconciled before publishing a supported
  installation command.
- The committed NumPy constraint (`numpy<1.27`) and SciPy lower bound reflect a
  historical compatibility note. Their compatibility with the current full
  stack was not re-tested during this documentation audit.
- Database-backed operation additionally requires a reachable PostgreSQL
  service and appropriate configuration. The repository currently contains
  local/default connection assumptions that should be reviewed before use.

## Verification boundary

This document is a source-and-manifest inventory, not a successful installation
record. The audit environment did not have the project dependencies or `pytest`
installed, and no packages were installed as part of the documentation cleanup.
