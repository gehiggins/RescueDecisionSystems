"""Regression coverage for the central empty DataFrame schemas."""

import datetime

import pandas as pd

from flask_app.app import data_schema


def test_prebuilt_schema_dataframes_import_with_expected_columns():
    assert data_schema.df_alerts.empty
    assert data_schema.df_weather.empty
    assert data_schema.df_gis_maps.empty
    assert data_schema.df_alerts.columns.tolist() == list(
        data_schema.SARSAT_ALERT_SCHEMA
    )
    assert data_schema.df_weather.columns.tolist() == list(
        data_schema.WEATHER_DATA_SCHEMA
    )
    assert data_schema.df_gis_maps.columns.tolist() == list(
        data_schema.GIS_DATA_SCHEMA
    )


def test_schema_constructor_maps_logical_types_to_pandas_dtypes():
    frame = data_schema.create_dataframe(
        {
            "timestamp": datetime.datetime,
            "coordinates": tuple,
            "items": list,
            "distance": float,
        }
    )

    assert isinstance(frame["timestamp"].dtype, pd.DatetimeTZDtype)
    assert str(frame["timestamp"].dtype.tz) == "UTC"
    assert frame["coordinates"].dtype == object
    assert frame["items"].dtype == object
    assert frame["distance"].dtype == float
