# sql_models.py - Defines Database Models for SARSAT Alert Processing
# 2025-03-07 (Updated for alert_sequence_number & site_creation_time)

from app.setup_imports import *
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from datetime import datetime
from dotenv import load_dotenv
import os

# âœ… Load environment variables (database connection)
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

class SARSATAlert(Base):
    """
    Database model for SARSAT alerts.
    """
    __tablename__ = "sarsat_alerts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    beacon_id = Column(String, nullable=False, index=True)
    site_id = Column(String, nullable=True, index=True)
    detect_time = Column(DateTime, nullable=True)
    latitude_a = Column(Float, nullable=True)
    longitude_a = Column(Float, nullable=True)
    latitude_b = Column(Float, nullable=True)
    longitude_b = Column(Float, nullable=True)
    alert_type = Column(String, nullable=True)
    beacon_type = Column(String, nullable=True)
    activation_type = Column(String, nullable=True)
    detection_frequency = Column(Float, nullable=True)
    satellite_id = Column(String, nullable=True)
    lut_id = Column(String, nullable=True)
    num_detections = Column(Integer, nullable=True)
    position_resolution = Column(String, nullable=True)
    probability_distress = Column(Float, nullable=True)
    status = Column(String, default="Pending")
    
    # âœ… Newly Added Fields
    alert_sequence_number = Column(Integer, nullable=True)  # Ensures correct sequencing of alerts
    site_creation_time = Column(DateTime, nullable=True)  # Ensures proper site tracking
    
    created_at = Column(DateTime, default=datetime.utcnow)

class WeatherData(Base):
    """
    Database model for weather data linked to SARSAT alerts.
    """
    __tablename__ = "weather_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    alert_id = Column(Integer, ForeignKey("sarsat_alerts.id"), nullable=False)
    station_id = Column(String, nullable=True)
    temperature = Column(Float, nullable=True)
    dewpoint = Column(Float, nullable=True)
    humidity = Column(Float, nullable=True)
    pressure = Column(Float, nullable=True)
    visibility = Column(Float, nullable=True)
    wind_speed = Column(Float, nullable=True)
    wind_gust = Column(Float, nullable=True)
    wind_direction = Column(Float, nullable=True)
    wave_height = Column(Float, nullable=True)
    wave_period = Column(Float, nullable=True)
    sea_state = Column(String, nullable=True)
    water_temperature = Column(Float, nullable=True)
    current_speed = Column(Float, nullable=True)
    current_direction = Column(Float, nullable=True)
    observation_time = Column(DateTime, nullable=True)

    sarsat_alert = relationship("SARSATAlert", back_populates="weather_data")

# âœ… Define Relationship Between Alerts & Weather Data
SARSATAlert.weather_data = relationship("WeatherData", order_by=WeatherData.id, back_populates="sarsat_alert")

