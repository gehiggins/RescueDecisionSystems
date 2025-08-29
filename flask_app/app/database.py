#database.py

from flask_app.setup_imports import *
from flask_app.app.sql_models import SARSATAlert, WeatherData, SessionLocal

def save_alert_to_db(df):
    """
    Saves parsed SARSAT alert data to the PostgreSQL database.
    """
    session = SessionLocal()
    try:
        alert = SARSATAlert(
            beacon_id=df.iloc[0]["beacon_id"],
            site_id=df.iloc[0].get("site_id"),
            detect_time=pd.to_datetime(df.iloc[0]["detect_time"], errors='coerce'),
            latitude_a=df.iloc[0].get("latitude_a"),
            longitude_a=df.iloc[0].get("longitude_a"),
            latitude_b=df.iloc[0].get("latitude_b"),
            longitude_b=df.iloc[0].get("longitude_b"),
            alert_type=df.iloc[0].get("alert_type"),
            beacon_type=df.iloc[0].get("beacon_type"),
            activation_type=df.iloc[0].get("activation_type"),
            detection_frequency=df.iloc[0].get("detection_frequency"),
            satellite_id=df.iloc[0].get("satellite_id"),
            lut_id=df.iloc[0].get("lut_id"),
            num_detections=df.iloc[0].get("num_detections"),
            position_resolution=df.iloc[0].get("position_resolution"),
            probability_distress=df.iloc[0].get("probability_distress"),
            status=df.iloc[0].get("status", "Pending")
        )
        session.add(alert)
        session.commit()
        logging.info(f"✅ Alert saved with ID: {alert.id}")
        return alert.id
    except Exception as e:
        session.rollback()
        logging.error(f"❌ Failed to save alert to DB: {e}")
        return None
    finally:
        session.close()

def save_weather_to_db(alert_id, weather_df, position):
    """
    Saves fetched weather data for a specific alert ID and position (A/B) to PostgreSQL.
    """
    session = SessionLocal()
    try:
        for _, row in weather_df.iterrows():
            weather = WeatherData(
                alert_id=alert_id,
                station_id=row["station_id"],
                temperature=row.get("temperature"),
                dewpoint=row.get("dewpoint"),
                humidity=row.get("humidity"),
                pressure=row.get("pressure"),
                visibility=row.get("visibility"),
                wind_speed=row.get("wind_speed"),
                wind_gust=row.get("wind_gust"),
                wind_direction=row.get("wind_direction"),
                wave_height=row.get("wave_height"),
                wave_period=row.get("wave_period"),
                sea_state=row.get("sea_state"),
                water_temperature=row.get("water_temperature"),
                current_speed=row.get("current_speed"),
                current_direction=row.get("current_direction"),
                observation_time=pd.to_datetime(row.get("observation_time"), errors='coerce'),
                position_label=position  # New field (ensure schema supports this)
            )
            session.add(weather)
        session.commit()
        logging.info(f"✅ Weather data saved for alert ID: {alert_id}, Position: {position}")
    except Exception as e:
        session.rollback()
        logging.error(f"❌ Failed to save weather data to DB: {e}")
    finally:
        session.close()

def get_existing_alerts():
    """
    Fetches existing alerts from the database to determine previous sequence numbers.
    Returns a Pandas DataFrame with site_id and alert_sequence_number.
    """
    session = SessionLocal()
    try:
        query = session.query(SARSATAlert.site_id, SARSATAlert.alert_sequence_number).all()
        existing_alerts_df = pd.DataFrame(query, columns=["site_id", "alert_sequence_number"])
        return existing_alerts_df
    except Exception as e:
        logging.error(f"❌ Failed to fetch existing alerts from DB: {e}")
        return pd.DataFrame(columns=["site_id", "alert_sequence_number"])  # Return empty DataFrame if error
    finally:
        session.close()
