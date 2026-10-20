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


class _InvalidHourlyQuery:
    def __init__(self, station_id, start, end):
        assert station_id == "KTEST"

    def fetch(self):
        return pd.DataFrame(
            [
                {
                    "temp": 20.0,
                    "dwpt": float("inf"),
                    "rhum": 101.0,
                    "wspd": -1.0,
                    "wdir": 361.0,
                    "wpgt": -2.0,
                    "pres": -1.0,
                    "prcp": -1.0,
                }
            ],
            index=pd.DatetimeIndex(["2026-10-20T12:00:00"]),
        )


class _MixedStationIdentifiersQuery:
    def nearby(self, lat, lon):
        assert (lat, lon) == (37.5, -75.5)
        return self

    def fetch(self, limit):
        assert limit == 6
        return pd.DataFrame(
            [
                {"latitude": 37.5, "longitude": -75.5, "name": "Bad"},
                {"latitude": 37.6, "longitude": -75.6, "name": "Valid"},
            ],
            index=pd.Index(["../BAD", " KTEST "], name="id"),
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
    assert np.isnan(wx_fetch_meteostat._kmh_to_ms(-1))


def test_meteostat_rejects_physically_invalid_measurements(monkeypatch):
    monkeypatch.setattr(wx_fetch_meteostat, "_HAS_METEOSTAT", True)
    monkeypatch.setattr(
        wx_fetch_meteostat, "Stations", _StationsQuery, raising=False
    )
    monkeypatch.setattr(
        wx_fetch_meteostat, "Hourly", _InvalidHourlyQuery, raising=False
    )

    result = wx_fetch_meteostat.fetch_meteostat_obs_near(
        37.5,
        -75.5,
        "2026-10-20T12:00:00Z",
        "2026-10-20T12:00:00Z",
        radius_km=50.0,
        max_stations=1,
    )

    invalid_fields = [
        "dewpoint_c",
        "rh_pct",
        "wind_ms",
        "wind_dir_deg",
        "wind_gust_ms",
        "pressure_hpa",
        "precip_mmhr",
    ]
    assert result.iloc[0][invalid_fields].isna().all()
    assert result.iloc[0]["temp_c"] == 20.0


@pytest.mark.parametrize(
    ("minimum", "maximum", "value"),
    [(0, 100, 0), (0, 100, 100), (0, 360, 0), (0, 360, 360)],
)
def test_meteostat_measurement_range_preserves_boundaries(
    minimum, maximum, value
):
    assert wx_fetch_meteostat._measurement(
        value, minimum=minimum, maximum=maximum
    ) == float(value)


@pytest.mark.parametrize(
    "invalid_station_id",
    [None, "", "   ", True, pd.NA, "../KTEST", "KTEST?x=1", "KTEST/extra"],
)
def test_meteostat_station_identifier_rejects_unsafe_values(
    invalid_station_id,
):
    assert wx_fetch_meteostat._station_id(invalid_station_id) is None


def test_meteostat_skips_invalid_station_identifier(monkeypatch):
    monkeypatch.setattr(wx_fetch_meteostat, "_HAS_METEOSTAT", True)
    monkeypatch.setattr(
        wx_fetch_meteostat,
        "Stations",
        _MixedStationIdentifiersQuery,
        raising=False,
    )
    monkeypatch.setattr(
        wx_fetch_meteostat, "Hourly", _HourlyQuery, raising=False
    )

    result = wx_fetch_meteostat.fetch_meteostat_obs_near(
        37.5,
        -75.5,
        "2026-10-20T12:00:00Z",
        "2026-10-20T12:00:00Z",
        radius_km=50.0,
        max_stations=2,
    )

    assert result["source_id"].tolist() == ["KTEST"]


@pytest.mark.parametrize(
    ("radius_km", "max_stations", "expected"),
    [
        ("25", "3", (25.0, 3)),
        ("bad", 2, (50.0, 2)),
        (0, 2.5, (50.0, 5)),
        (True, False, (50.0, 5)),
        (np.bool_(True), np.bool_(False), (50.0, 5)),
        (float("inf"), float("nan"), (50.0, 5)),
        (50, 1_000_000, (50.0, 100)),
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
