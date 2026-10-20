import numpy as np
import pandas as pd
import pytest

from app import wx_fetch_meteostat


class _StationsQuery:
    def nearby(self, lat, lon):
        assert (lat, lon) == (37.5, -75.5)
        return self

    def fetch(self, limit):
        assert limit == 3
        return pd.DataFrame(
            [{"latitude": 37.5, "longitude": -75.5, "name": "Test Station"}],
            index=pd.Index(["KTEST"], name="id"),
        )


class _HourlyQuery:
    def __init__(self, station_id, start, end):
        assert station_id == "KTEST"
        self.start = start
        self.end = end

    def fetch(self):
        return pd.DataFrame(
            [{"wspd": 36.0, "wpgt": 72.0, "wdir": 180.0}],
            index=pd.DatetimeIndex(["2026-10-20T12:00:00"]),
        )


class _PartiallyMalformedStationsQuery:
    def nearby(self, lat, lon):
        assert (lat, lon) == (37.5, -75.5)
        return self

    def fetch(self, limit):
        assert limit == 3
        return pd.DataFrame(
            [
                {"latitude": "bad", "longitude": -75.5, "name": "Bad Station"},
                {"latitude": 37.5, "longitude": -75.5, "name": "Test Station"},
            ],
            index=pd.Index(["KBAD", "KTEST"], name="id"),
        )


def test_meteostat_converts_metric_wind_and_gust_to_metres_per_second(monkeypatch):
    monkeypatch.setattr(wx_fetch_meteostat, "_HAS_METEOSTAT", True)
    monkeypatch.setattr(wx_fetch_meteostat, "Stations", _StationsQuery, raising=False)
    monkeypatch.setattr(wx_fetch_meteostat, "Hourly", _HourlyQuery, raising=False)

    result = wx_fetch_meteostat.fetch_meteostat_obs_near(
        37.5,
        -75.5,
        "2026-10-20T12:00:00Z",
        "2026-10-20T12:00:00Z",
        radius_km=50.0,
        max_stations=1,
    )

    assert len(result) == 1
    assert isinstance(result.index, pd.RangeIndex)
    assert result.iloc[0]["wind_ms"] == 10.0
    assert result.iloc[0]["wind_gust_ms"] == 20.0


def test_meteostat_wind_conversion_rejects_nonfinite_values():
    assert np.isnan(wx_fetch_meteostat._kmh_to_ms(None))
    assert np.isnan(wx_fetch_meteostat._kmh_to_ms("bad"))
    assert np.isnan(wx_fetch_meteostat._kmh_to_ms(np.inf))


@pytest.mark.parametrize(
    ("radius_km", "max_stations", "expected"),
    [
        ("25", "3", (25.0, 3)),
        ("bad", 2, (50.0, 2)),
        (0, 2.5, (50.0, 5)),
        (True, False, (50.0, 5)),
        (float("inf"), float("nan"), (50.0, 5)),
    ],
)
def test_meteostat_normalizes_direct_search_parameters(
    radius_km, max_stations, expected
):
    assert wx_fetch_meteostat._normalize_search_parameters(
        radius_km, max_stations
    ) == expected


def test_meteostat_does_not_pass_invalid_station_limit_to_client(monkeypatch):
    captured = {}

    class StationsQuery:
        def nearby(self, lat, lon):
            captured["point"] = (lat, lon)
            return self

        def fetch(self, limit):
            captured["limit"] = limit
            return pd.DataFrame()

    monkeypatch.setattr(wx_fetch_meteostat, "_HAS_METEOSTAT", True)
    monkeypatch.setattr(
        wx_fetch_meteostat, "Stations", StationsQuery, raising=False
    )

    result = wx_fetch_meteostat.fetch_meteostat_obs_near(
        37.5,
        -75.5,
        "2026-10-20T12:00:00Z",
        "2026-10-20T12:00:00Z",
        radius_km="bad",
        max_stations=2.5,
    )

    assert result.empty
    assert captured == {"point": (37.5, -75.5), "limit": 15}


def test_meteostat_rejects_invalid_time_window_without_station_lookup(monkeypatch):
    monkeypatch.setattr(wx_fetch_meteostat, "_HAS_METEOSTAT", True)
    monkeypatch.setattr(
        wx_fetch_meteostat,
        "Stations",
        lambda: (_ for _ in ()).throw(
            AssertionError("station provider called with invalid time window")
        ),
        raising=False,
    )

    result = wx_fetch_meteostat.fetch_meteostat_obs_near(
        37.5,
        -75.5,
        "invalid",
        "2026-10-20T12:00:00Z",
    )

    assert result.empty
    assert "valid_utc" in result.columns


@pytest.mark.parametrize(
    ("latitude", "longitude"),
    [
        ("bad", -75.5),
        (91.0, -75.5),
        (37.5, -181.0),
        (float("nan"), 0.0),
        (True, False),
    ],
)
def test_meteostat_rejects_invalid_coordinates_without_station_lookup(
    monkeypatch, latitude, longitude
):
    monkeypatch.setattr(wx_fetch_meteostat, "_HAS_METEOSTAT", True)
    monkeypatch.setattr(
        wx_fetch_meteostat,
        "Stations",
        lambda: (_ for _ in ()).throw(
            AssertionError("station provider called with invalid coordinates")
        ),
        raising=False,
    )

    result = wx_fetch_meteostat.fetch_meteostat_obs_near(
        latitude,
        longitude,
        "2026-10-20T12:00:00Z",
        "2026-10-20T12:00:00Z",
    )

    assert result.empty
    assert "valid_utc" in result.columns


def test_meteostat_preserves_valid_stations_when_one_coordinate_is_bad(monkeypatch):
    monkeypatch.setattr(wx_fetch_meteostat, "_HAS_METEOSTAT", True)
    monkeypatch.setattr(
        wx_fetch_meteostat,
        "Stations",
        _PartiallyMalformedStationsQuery,
        raising=False,
    )
    monkeypatch.setattr(wx_fetch_meteostat, "Hourly", _HourlyQuery, raising=False)

    result = wx_fetch_meteostat.fetch_meteostat_obs_near(
        37.5,
        -75.5,
        "2026-10-20T12:00:00Z",
        "2026-10-20T12:00:00Z",
        radius_km=50.0,
        max_stations=1,
    )

    assert len(result) == 1
    assert result.iloc[0]["source_id"] == "KTEST"
