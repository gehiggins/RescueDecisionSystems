# utils_weather.py - Weather Utilities for Rescue Decision Systems
# 2025-03-06 Initial Draft

from flask_app.setup_imports import *
from datetime import datetime, timezone

def calculate_timelate(observation_time):
    """
    Calculates the age of an observation (in hours) compared to current UTC time.
    Returns:
        float: Age in hours, or NaN if observation_time is invalid.
    """
    try:
        if pd.isna(observation_time) or observation_time is None:
            return np.nan

        now = datetime.now(timezone.utc)
        age_seconds = (now - observation_time).total_seconds()
        return age_seconds / 3600.0  # Convert seconds to hours
    except Exception as e:
        logging.warning(f"⚠️ Failed to calculate timelate: {e}")
        return np.nan

def prioritize_weather_stations(weather_df):
    """
    Adds priority ranking based on completeness and freshness.
    Priority rules (highest to lowest):
    1. Complete data within 1 hour.
    2. Partial data within 1 hour.
    3. Complete data within 12 hours.
    4. Partial data within 12 hours.
    5. Stale data (older than 12 hours) flagged as 'stale'.

    Returns:
        DataFrame: Same input DataFrame with a 'priority' column.
    """
    if weather_df.empty:
        return weather_df

    weather_df['priority'] = 5  # Default - stale

    fresh_mask = weather_df['timelate'] <= 1
    recent_mask = (weather_df['timelate'] > 1) & (weather_df['timelate'] <= 12)
    stale_mask = weather_df['timelate'] > 12

    complete_mask = weather_df[['temperature', 'wind_speed', 'wind_direction']].notna().all(axis=1)
    partial_mask = ~complete_mask

    weather_df.loc[fresh_mask & complete_mask, 'priority'] = 1
    weather_df.loc[fresh_mask & partial_mask, 'priority'] = 2
    weather_df.loc[recent_mask & complete_mask, 'priority'] = 3
    weather_df.loc[recent_mask & partial_mask, 'priority'] = 4
    weather_df.loc[stale_mask, 'priority'] = 5

    return weather_df

def celsius_to_fahrenheit(c):
    """
    Converts Celsius to Fahrenheit.
    """
    if c is None or pd.isna(c):
        return np.nan
    return (c * 9 / 5) + 32

def meters_per_second_to_knots(mps):
    """
    Converts meters/second to knots.
    """
    if mps is None or pd.isna(mps):
        return np.nan
    return mps * 1.94384
