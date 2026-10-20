from setuptools import setup, find_packages

setup(
    name='RescueDecisionSystems',
    version='0.2',
    packages=find_packages(),
    install_requires=[
        'Flask',
        'SQLAlchemy',
        'psycopg2-binary',
        'pandas',
        'numpy<1.27',
        'scipy>=1.10',
        'scikit-learn',
        'joblib',
        'geopandas',
        'shapely',
        'geopy',
        'folium',
        'matplotlib',
        'pillow',
        'requests',
        'python-dotenv',
        'skyfield'
    ],
    extras_require={
        'server': ['gunicorn'],
        'weather': ['meteostat'],
        'timezone': ['timezonefinder'],
        'test': ['pytest'],
    },
    entry_points={
        'console_scripts': [
            'run_rds=flask_app.run:main'
        ]
    }
)
