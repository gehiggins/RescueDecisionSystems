from setuptools import setup, find_packages

setup(
    name='RescueDecisionSystems',
    version='0.2',
    packages=find_packages(),
    install_requires=[
        'Flask',
        'Flask-Login',
        'SQLAlchemy',
        'psycopg2-binary',
        'pandas',
        'numpy<1.27',   # 👈 Locking numpy to avoid scipy conflict
        'scipy>=1.10',
        'scikit-learn',
        'geopandas',
        'shapely',
        'contextily',
        'pillow',
        'requests',
        'python-dotenv'
    ],
    entry_points={
        'console_scripts': [
            'run_rds=flask_app.run:main'
        ]
    }
)
