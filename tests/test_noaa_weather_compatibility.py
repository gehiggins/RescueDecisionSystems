"""Regression coverage for the retired NOAA compatibility interface."""

import pandas as pd

from flask_app.app import noaa_weather_fetch


def test_compatibility_offshore_fetch_delegates_to_ndbc_adapter(monkeypatch):
    captured = {}

    def fetch_ndbc(lat, lon, position_label, max_buoys):
        captured.update(
            lat=lat,
            lon=lon,
            position_label=position_label,
            max_buoys=max_buoys,
        )
        return pd.DataFrame([{"station_id": "44009"}])

    monkeypatch.setattr(noaa_weather_fetch, "fetch_ndbc_buoy_data", fetch_ndbc)

    result = noaa_weather_fetch.fetch_offshore_buoys(
        37.5, -74.5, max_buoys=3
    )

    assert result["station_id"].tolist() == ["44009"]
    assert captured == {
        "lat": 37.5,
        "lon": -74.5,
        "position_label": "compatibility",
        "max_buoys": 3,
    }


def test_missing_legacy_zone_column_does_not_discard_buoy_data(monkeypatch):
    buoy = pd.DataFrame(
        [
            {
                "station_id": "44009",
                "temperature": 20.0,
                "wind_speed": 5.0,
                "wave_height": 1.0,
            }
        ]
    )
    monkeypatch.setattr(
        noaa_weather_fetch,
        "_fetch_onshore_weather_stations",
        lambda *_args: pd.DataFrame(),
    )
    monkeypatch.setattr(
        noaa_weather_fetch,
        "fetch_offshore_buoys",
        lambda *_args, **_kwargs: buoy,
    )
    monkeypatch.setattr(
        noaa_weather_fetch,
        "fetch_weather_alerts_zone",
        lambda *_args: ("TEST-ZONE", ["Test alert"]),
    )

    result = noaa_weather_fetch.fetch_nearest_weather_stations(37.5, -74.5)

    assert result["station_id"].tolist() == ["44009"]
    assert result["weather_alerts_zone"].isna().all()
