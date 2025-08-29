#test_database.py

import sys
import os

# Add flask_app to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from setup_imports import * 

from sql_models import SARSATAlert, WeatherData, Base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pandas as pd
import numpy as np


# ✅ Load environment and connect to the local database
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:Neworleans#01@127.0.0.1/flask_db")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

# ✅ Create database tables if they don't exist
Base.metadata.create_all(engine)

# ✅ Start a session
session = SessionLocal()

# ✅ Insert test SARSAT alert
test_alert = SARSATAlert(
    beacon_id="TEST123456",
    site_id="TEST_SITE",
    detect_time=datetime.utcnow(),
    latitude_a=47.6062,
    longitude_a=-122.3321,
    latitude_b=None,  # Missing value to test NULL handling
    longitude_b=None,
    alert_type="171",
    beacon_type="EPIRB",
    activation_type="Automatic",
    detection_frequency=406.037,
    satellite_id="SAT123",
    lut_id="LUT123",
    num_detections=3,
    position_resolution="5 km",
    probability_distress=0.85,
    status="Pending"
)
session.add(test_alert)
session.commit()

# ✅ Retrieve the generated alert_id
alert_id = test_alert.id

# ✅ Insert test NOAA weather data (including missing values)
test_weather = WeatherData(
    alert_id=alert_id,
    station_id="STATION123",
    temperature=15.2,
    dewpoint=None,  # Test NULL handling
    humidity=80.5,
    pressure=1013.2,
    visibility=None,  # Test NULL handling
    wind_speed=5.5,
    wind_gust=np.nan,  # Test NULL handling (should convert to NULL in SQL)
    wind_direction=180,
    wave_height=None,
    wave_period=6.5,
    sea_state="Calm",
    water_temperature=np.nan,  # Test NULL handling (should convert to NULL in SQL)
    current_speed=1.2,
    current_direction=45,
    observation_time=datetime.utcnow()
)
session.add(test_weather)
session.commit()

# ✅ Retrieve and display the stored weather data
stored_weather = session.query(WeatherData).filter_by(alert_id=alert_id).first()

# ✅ Convert to DataFrame to view results
df_weather_test = pd.DataFrame([{
    "station_id": stored_weather.station_id,
    "temperature": stored_weather.temperature,
    "dewpoint": stored_weather.dewpoint,
    "humidity": stored_weather.humidity,
    "pressure": stored_weather.pressure,
    "visibility": stored_weather.visibility,
    "wind_speed": stored_weather.wind_speed,
    "wind_gust": stored_weather.wind_gust,
    "wind_direction": stored_weather.wind_direction,
    "wave_height": stored_weather.wave_height,
    "wave_period": stored_weather.wave_period,
    "sea_state": stored_weather.sea_state,
    "water_temperature": stored_weather.water_temperature,
    "current_speed": stored_weather.current_speed,
    "current_direction": stored_weather.current_direction,
    "observation_time": stored_weather.observation_time
}])

print("\n✅ Weather Data Test - Stored in SQL:")
print(df_weather_test)

# ✅ Close session
session.close()
