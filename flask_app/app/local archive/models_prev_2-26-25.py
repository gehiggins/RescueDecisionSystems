#models.py

from sqlalchemy import create_engine, Column, String, Integer, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
from datetime import datetime
from dotenv import load_dotenv  # ✅ Import dotenv to load .env variables

# ✅ Load environment variables from .env
load_dotenv()

Base = declarative_base()

class SARSATAlert(Base):
    __tablename__ = "sarsat_alerts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    beacon_id = Column(String, nullable=False, index=True)  # Indexed for fast lookup
    site_id = Column(String, nullable=False, index=True)  # Indexed for case tracking
    detect_time = Column(DateTime, nullable=False)
    latitude = Column(Float, nullable=True)  # Changed from String → Float
    longitude = Column(Float, nullable=True)  # Changed from String → Float
    alert_type = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)  # Tracks when record was added

# ✅ Read DATABASE_URL from .env
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("⚠️ DATABASE_URL is not set! Configure your .env file or system environment.")

# ✅ Initialize database engine & session
try:
    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    print("✅ Successfully connected to the database!")
except Exception as e:
    raise ValueError(f"❌ Database connection failed: {e}")

def save_alert_to_db(parsed_data):
    """Saves parsed SARSAT alert to PostgreSQL with error handling."""
    session = SessionLocal()
    try:
        alert = SARSATAlert(
            beacon_id=parsed_data["beacon_id"],
            site_id=parsed_data["site_id"],
            detect_time=datetime.strptime(parsed_data["detect_time"], "%d %H%M%S %b"),  # Converts to datetime
            latitude=float(parsed_data["latitude"]) if parsed_data.get("latitude") else None,
            longitude=float(parsed_data["longitude"]) if parsed_data.get("longitude") else None,
            alert_type=parsed_data["alert_type"]
        )
        session.add(alert)
        session.commit()
        return True
    except Exception as e:
        session.rollback()
        print(f"⚠️ Database Error: {e}")
        return False
    finally:
        session.close()
