Python Packages (core environment - unified_env)

- Flask
- Flask-Login
- SQLAlchemy
- psycopg2-binary
- Pandas
- Numpy (note: enforce <1.27 due to scipy conflict)
- Scikit-learn
- Geopandas
- Shapely
- Contextily
- Pillow
- Requests
- Regex (re is built-in but document common usage patterns for parsing)

Additional Files (stored in /data or /static if required)

- Optional: Shapefile for US RCC boundaries (if needed for future GIS layers)
- Basemap tiles cached in /data/maps/cache/ (if using contextily)

Installation Note (Local and Droplet)

- Local install via: 
    ```bash
    pip install -e .
    ```
- Droplet install (inside SSH session):
    ```bash
    source ~/unified_env/bin/activate
    pip install -e .
    ```

- Dependency Conflicts (as of 2/28/2025)

    Issue: scipy 1.10.1 requires numpy<1.27,>=1.19.5, but numpy 2.0.2 was installed.

    Temporary Fix: Manually downgrade numpy after installing contextily
    ```bash
    pip install numpy==1.26.4
    ```
    Note: This should be logged in RDS_Installation_Requirements.md for now.
