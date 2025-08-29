"""
setup_imports.py

Centralized module imports for Rescue Decision Systems (RDS).
This ensures all scripts load necessary packages consistently.
"""



import os
import sys

# Add the flask_app directory to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))



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

# ✅ Ensure Python recognizes the `flask_app` directory
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
FLASK_APP_DIR = os.path.join(PROJECT_ROOT, "flask_app")
PARENT_DIR = os.path.abspath(os.path.join(PROJECT_ROOT, ".."))

if FLASK_APP_DIR not in sys.path:
    sys.path.append(FLASK_APP_DIR)
    logging.info(f"✅ Added Flask app directory to Python path: {FLASK_APP_DIR}")

if PARENT_DIR not in sys.path:
    sys.path.append(PARENT_DIR)
    logging.info(f"✅ Added parent directory to Python path: {PARENT_DIR}")

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
