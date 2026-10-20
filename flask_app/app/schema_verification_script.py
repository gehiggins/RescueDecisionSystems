"""Explicit database smoke check for the current RDS SQL models.

This script inserts disposable test records, so it only runs when called
directly and when the operator has supplied ``DATABASE_URL``.
"""

from datetime import datetime
import os

import numpy as np
import pandas as pd


def main():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise SystemExit(
            "DATABASE_URL must be set explicitly before running the database smoke check."
        )

    # Delay database imports until after configuration validation. Importing
    # this module must never connect to or modify a database.
    from .sql_models import Base, SARSATAlert, SessionLocal, WeatherData, engine

    Base.metadata.create_all(engine)
    with SessionLocal() as session:
        print("Inserting test SARSAT alert...")
        test_alert = SARSATAlert(
            beacon_id="TEST123456",
            site_id="TEST_SITE",
            detect_time=datetime.utcnow(),
            latitude_a=47.6062,
            longitude_a=-122.3321,
            latitude_b=None,
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
            status="Pending",
        )
        session.add(test_alert)
        session.commit()

        print("Inserting test NOAA weather data...")
        test_weather = WeatherData(
            alert_id=test_alert.id,
            station_id="STATION123",
            temperature=15.2,
            dewpoint=None,
            humidity=80.5,
            pressure=1013.2,
            visibility=None,
            wind_speed=5.5,
            wind_gust=np.nan,
            wind_direction=180,
            wave_height=None,
            wave_period=6.5,
            sea_state="Calm",
            water_temperature=np.nan,
            current_speed=1.2,
            current_direction=45,
            observation_time=datetime.utcnow(),
        )
        session.add(test_weather)
        session.commit()

        stored_weather = (
            session.query(WeatherData).filter_by(alert_id=test_alert.id).first()
        )
        result = pd.DataFrame(
            [
                {
                    column: getattr(stored_weather, column)
                    for column in (
                        "station_id",
                        "temperature",
                        "dewpoint",
                        "humidity",
                        "pressure",
                        "visibility",
                        "wind_speed",
                        "wind_gust",
                        "wind_direction",
                        "wave_height",
                        "wave_period",
                        "sea_state",
                        "water_temperature",
                        "current_speed",
                        "current_direction",
                        "observation_time",
                    )
                }
            ]
        )

    print("\nWeather data smoke check stored in SQL:")
    print(result)
    return result


if __name__ == "__main__":
    main()
