"""
setup_imports.py

Centralized module imports for Rescue Decision Systems (RDS).
This ensures all scripts load necessary packages consistently.
"""




import os
import sys
from pathlib import Path

# Add the flask_app directory to Python path (robust)
repo_root = Path(__file__).resolve().parents[1]      # .../RescueDecisionSystems
flask_app_dir = repo_root / "flask_app"              # .../RescueDecisionSystems/flask_app
if str(flask_app_dir) not in sys.path:
    sys.path.insert(0, str(flask_app_dir))
    import logging
    logging.info(f"✅ Added Flask app directory to Python path: {flask_app_dir}")



import re
import logging
import json
from datetime import datetime

# ✅ Configure Logging (Move this before any logging calls)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ✅ Data Handling
import pandas as pd
import numpy as np

# ✅ SQL & Database
import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, DateTime
from sqlalchemy.orm import sessionmaker


# ✅ GIS & Geospatial
import geopandas as gpd
from shapely.geometry import Point
from geopy.distance import geodesic  


# ✅ Web & API Requests
import requests

# ✅ Machine Learning (if needed later)
import joblib
from sklearn.preprocessing import StandardScaler

# ✅ Utility Functions
from typing import Dict, List, Tuple, Optional


logging.info("✅ Loaded setup_imports for consistent imports across scripts.")

# ✅ensure load_dotenv() is consistently applied across all scripts
from dotenv import load_dotenv
import os

# ✅ Load environment variables from .env file
load_dotenv()

# Optional: Log or print confirmation for debugging
if os.getenv("DATABASE_URL"):
    print("[OK] DATABASE_URL loaded successfully.")
else:
    print("[WARN] DATABASE_URL not found.")
