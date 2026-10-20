from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
import requests

from flask_app.app import sat_fetch_tle
from flask_app.app import sat_compute_groundtrack
from flask_app.app import sat_pipeline


def test_satellite_reference_falls_back_without_local_csv(monkeypatch, tmp_path):
    missing_reference = tmp_path / "missing-satellite-reference.csv"
    monkeypatch.setattr(sat_fetch_tle, "BASELINE_CSV_PATH", missing_reference)

    reference = sat_fetch_tle.load_sat_reference()

    assert not reference.empty
    assert set(
        ["type", "designator", "common_name", "norad_id", "intl_designator"]
    ).issubset(reference.columns)
    assert reference["norad_id"].notna().all()


def test_satellite_reference_path_is_repository_relative():
    expected = (
        Path(sat_fetch_tle.__file__).resolve().parents[2]
        / "data"
        / "reference"
        / "sarsat_satellites_baseline.csv"
    )

    assert sat_fetch_tle.BASELINE_CSV_PATH == expected


def test_satellite_pipeline_handles_alert_without_reporting_satellite(monkeypatch):
    monkeypatch.setattr(
        sat_pipeline,
        "load_tle_snapshot",
        lambda _identifiers: pd.DataFrame(
            columns=[
                "name",
                "norad_id",
                "tle_line1",
                "tle_line2",
                "epoch_utc",
                "provider",
                "lookup_method",
                "cache_hit",
            ]
        ),
    )
    alert = pd.DataFrame(
        [
            {
                "alert_id": "98372",
                "alert_time_utc": pd.Timestamp("2026-10-19T18:00:00Z"),
                "alert_lat_dd": 37.76,
                "alert_lon_dd": -75.50,
            }
        ]
    )

    overlay = sat_pipeline.build_sat_overlay_df(alert, use_tle=True)

    assert overlay.empty


@pytest.mark.parametrize("alert_time", [None, "invalid", ["2026-10-20T12:00:00Z"]])
def test_satellite_pipeline_rejects_invalid_alert_time_without_provider_lookup(
    monkeypatch, alert_time
):
    monkeypatch.setattr(
        sat_pipeline,
        "load_sat_reference",
        lambda: pytest.fail("satellite reference loaded for invalid alert time"),
    )
    alert = pd.DataFrame(
        [
            {
                "alert_id": "98372",
                "alert_time_utc": alert_time,
                "alert_lat_dd": 37.76,
                "alert_lon_dd": -75.50,
            }
        ]
    )

    overlay = sat_pipeline.build_sat_overlay_df(alert, use_tle=True)

    assert overlay.empty
    assert "norad_id" in overlay.columns


def test_satellite_pipeline_rejects_non_dataframe_alert_input():
    overlay = sat_pipeline.build_sat_overlay_df(None, use_tle=True)

    assert overlay.empty
    assert "at_time_utc" in overlay.columns


def test_stale_tle_is_not_used_for_historical_alert(monkeypatch):
    monkeypatch.setenv("RDS_SAT_MAX_TLE_AGE_HOURS", "72")
    stale_tle = pd.DataFrame(
        [
            {
                "name": "NOAA 15",
                "norad_id": 25338,
                "tle_line1": "line 1",
                "tle_line2": "line 2",
                "epoch_utc": pd.Timestamp("2026-10-19T18:00:00Z"),
                "provider": "test",
                "lookup_method": "test",
                "cache_hit": False,
            }
        ]
    )
    monkeypatch.setattr(sat_pipeline, "load_tle_snapshot", lambda _ids: stale_tle)
    monkeypatch.setattr(
        sat_pipeline, "compute_subpoint_at", lambda *_args: (10.0, 20.0, 850.0)
    )
    monkeypatch.setattr(
        sat_pipeline,
        "compute_short_track",
        lambda *_args, **_kwargs: ([], None, None),
    )
    monkeypatch.setattr(sat_pipeline, "find_next_pass_marker", lambda *_a, **_k: None)
    alert = pd.DataFrame(
        [
            {
                "alert_id": "98372",
                "alert_time_utc": pd.Timestamp("2025-03-02T13:32:17Z"),
                "alert_lat_dd": 37.76,
                "alert_lon_dd": -75.50,
                "norad_id": 25338,
            }
        ]
    )

    overlay = sat_pipeline.build_sat_overlay_df(alert, use_tle=True)

    assert overlay.empty


@pytest.mark.parametrize("configured", ["not-a-number", "nan", "inf", "-1", "0"])
def test_invalid_tle_age_configuration_uses_safe_default(
    monkeypatch, configured
):
    monkeypatch.setenv("RDS_SAT_MAX_TLE_AGE_HOURS", configured)

    assert sat_pipeline._max_tle_age_hours() == 168.0


def test_positive_tle_age_configuration_is_preserved(monkeypatch):
    monkeypatch.setenv("RDS_SAT_MAX_TLE_AGE_HOURS", "72")

    assert sat_pipeline._max_tle_age_hours() == 72.0


@pytest.mark.parametrize("configured", ["bad", "nan", "inf", "-1", "0"])
def test_invalid_tle_request_timeout_uses_safe_default(monkeypatch, configured):
    monkeypatch.setenv("RDS_SAT_REQUEST_TIMEOUT_SECONDS", configured)

    assert sat_fetch_tle._request_timeout_seconds() == 10.0


def test_tle_request_timeout_is_configurable(monkeypatch):
    monkeypatch.setenv("RDS_SAT_REQUEST_TIMEOUT_SECONDS", "2.5")

    assert sat_fetch_tle._request_timeout_seconds() == 2.5


def test_tle_fetch_uses_configured_request_timeout(monkeypatch):
    captured = {}
    monkeypatch.setenv("RDS_SAT_REQUEST_TIMEOUT_SECONDS", "2.5")
    monkeypatch.setattr(sat_fetch_tle, "_TLE_CACHE_CATNR", {})
    monkeypatch.setattr(sat_fetch_tle, "_TLE_CACHE_GROUP", {})
    monkeypatch.setattr(sat_fetch_tle, "_TLE_PROVIDER_UNAVAILABLE_UNTIL", 0.0)

    class Response:
        status_code = 404
        text = ""

    def fake_get(_url, **kwargs):
        captured.update(kwargs)
        return Response()

    monkeypatch.setattr(sat_fetch_tle.requests, "get", fake_get)

    sat_fetch_tle.load_tle_snapshot(["25338"])

    assert captured["timeout"] == 2.5


def test_tle_provider_timeout_stops_retry_cascade(monkeypatch):
    calls = []
    monkeypatch.setattr(sat_fetch_tle, "_TLE_CACHE_CATNR", {})
    monkeypatch.setattr(sat_fetch_tle, "_TLE_CACHE_GROUP", {})
    monkeypatch.setattr(sat_fetch_tle, "_TLE_PROVIDER_UNAVAILABLE_UNTIL", 0.0)
    monkeypatch.setattr(sat_fetch_tle, "_TLE_PROVIDER_BACKOFF_SECONDS", 60.0)

    def time_out(url, **_kwargs):
        calls.append(url)
        raise requests.ConnectTimeout("provider unavailable")

    monkeypatch.setattr(sat_fetch_tle.requests, "get", time_out)

    first = sat_fetch_tle.load_tle_snapshot(["25338", "28654"])
    second = sat_fetch_tle.load_tle_snapshot(["33591"])

    assert first.empty
    assert second.empty
    assert len(calls) == 1
    assert list(first.columns) == [
        "name",
        "norad_id",
        "tle_line1",
        "tle_line2",
        "epoch_utc",
        "provider",
        "lookup_method",
        "cache_hit",
    ]


def test_tle_backoff_keeps_cached_satellites_available(monkeypatch):
    cached = {
        "name": "NOAA 15",
        "norad_id": 25338,
        "tle_line1": "line 1",
        "tle_line2": "line 2",
        "epoch_utc": "2026-10-20T00:00:00Z",
        "provider": "celestrak",
        "lookup_method": "catnr",
        "cache_hit": False,
    }
    monkeypatch.setattr(sat_fetch_tle, "_TLE_CACHE_CATNR", {25338: cached})
    monkeypatch.setattr(sat_fetch_tle, "_TLE_CACHE_GROUP", {})
    monkeypatch.setattr(sat_fetch_tle, "_TLE_PROVIDER_UNAVAILABLE_UNTIL", float("inf"))
    monkeypatch.setattr(
        sat_fetch_tle.requests,
        "get",
        lambda *_args, **_kwargs: pytest.fail("network called during provider backoff"),
    )

    snapshot = sat_fetch_tle.load_tle_snapshot(["25338", "28654"])

    assert len(snapshot) == 1
    assert snapshot.iloc[0]["norad_id"] == 25338
    assert bool(snapshot.iloc[0]["cache_hit"]) is True


@pytest.mark.parametrize("group_name", ["galileo", "glonass", "beidou"])
def test_tle_resolver_accepts_explicit_group_names(monkeypatch, group_name):
    calls = []
    monkeypatch.setattr(sat_fetch_tle, "_TLE_CACHE_CATNR", {})
    monkeypatch.setattr(sat_fetch_tle, "_TLE_CACHE_GROUP", {})
    monkeypatch.setattr(sat_fetch_tle, "_TLE_PROVIDER_UNAVAILABLE_UNTIL", 0.0)

    class Response:
        status_code = 200
        text = (
            "TEST SATELLITE\n"
            "1 43055U 17079A   26293.00000000  .00000000  00000-0  00000-0 0  9999\n"
            "2 43055  56.0000 100.0000 0001000  10.0000 350.0000  2.00000000    00\n"
        )

    def fake_get(url, **_kwargs):
        calls.append(url)
        return Response()

    monkeypatch.setattr(sat_fetch_tle.requests, "get", fake_get)

    snapshot = sat_fetch_tle.load_tle_snapshot([group_name])

    assert len(snapshot) == 1
    assert snapshot.iloc[0]["lookup_method"] == "group"
    assert f"GROUP={group_name.upper()}" in calls[0]


def test_next_pass_marker_exposes_map_and_legacy_fields(monkeypatch):
    class FakeGeo:
        def __sub__(self, _observer):
            return self

        @staticmethod
        def altaz():
            elevation = SimpleNamespace(
                degrees=sat_compute_groundtrack.np.array([-1.0, 5.0, 10.0, -1.0])
            )
            return elevation, None, None

    class FakeSatellite:
        @staticmethod
        def at(_when):
            return FakeGeo()

    class FakeTimescale:
        @staticmethod
        def from_datetimes(values):
            return values

        @staticmethod
        def from_datetime(value):
            return value

    fake_subpoint = SimpleNamespace(
        latitude=SimpleNamespace(degrees=12.5),
        longitude=SimpleNamespace(degrees=181.0),
    )
    monkeypatch.setattr(sat_compute_groundtrack, "_make_sat", lambda *_a: FakeSatellite())
    monkeypatch.setattr(
        sat_compute_groundtrack,
        "load",
        SimpleNamespace(timescale=lambda: FakeTimescale()),
    )
    monkeypatch.setattr(
        sat_compute_groundtrack,
        "wgs84",
        SimpleNamespace(
            latlon=lambda *_a, **_k: object(),
            subpoint=lambda _geo: fake_subpoint,
        ),
    )

    marker = sat_compute_groundtrack.find_next_pass_marker(
        "line 1",
        "line 2",
        alert_latlon=(37.5, -75.0),
        start_utc=pd.Timestamp("2026-10-20T12:00:00Z").to_pydatetime(),
        max_hours=1,
    )

    assert marker["coordinates"] == [-179.0, 12.5]
    assert marker["when_utc"] == marker["time_utc"]
    assert marker["max_elev_deg"] == marker["elevation_max_deg"] == 10.0
    assert marker["lat_dd"] == 12.5
    assert marker["lon_dd"] == -179.0


def test_upcoming_passes_use_groundtrack_contract(monkeypatch):
    reference = pd.DataFrame([{"type": "LEO", "norad_id": 25338}])
    tles = pd.DataFrame(
        [
            {
                "name": "NOAA 15",
                "norad_id": 25338,
                "tle_line1": "line 1",
                "tle_line2": "line 2",
                "epoch_utc": "2026-10-20T11:00:00Z",
            }
        ]
    )
    captured = {}

    def next_pass(_line1, _line2, alert_latlon, start_utc, max_hours):
        captured.update(
            alert_latlon=alert_latlon,
            start_utc=start_utc,
            max_hours=max_hours,
        )
        return {
            "when_utc": "2026-10-20T12:30:00Z",
            "coordinates": [-74.0, 38.0],
            "max_elev_deg": 35.0,
        }

    monkeypatch.setattr(sat_pipeline, "load_sat_reference", lambda: reference)
    monkeypatch.setattr(sat_pipeline, "load_tle_snapshot", lambda _ids: tles)
    monkeypatch.setattr(sat_pipeline, "_tle_is_fresh", lambda *_args: True)
    monkeypatch.setattr(sat_pipeline, "find_next_pass_marker", next_pass)
    monkeypatch.setattr(
        sat_pipeline, "compute_subpoint_at", lambda *_args: (38.0, -74.0, 850.0)
    )
    at_time = pd.Timestamp("2026-10-20T12:00:00Z")

    upcoming = sat_pipeline._upcoming_passes(
        37.5, -75.0, at_time, exclude_norad=set(), max_hours=6
    )

    assert len(upcoming) == 1
    assert upcoming.iloc[0]["role"] == "upcoming"
    assert upcoming.iloc[0]["next_pass_marker"]["coordinates"] == [-74.0, 38.0]
    assert captured["alert_latlon"] == (37.5, -75.0)
    assert captured["start_utc"] == at_time.to_pydatetime()
    assert captured["max_hours"] == 6.0


def test_satellite_pipeline_uses_requested_nearest_fallback(monkeypatch):
    reference = pd.DataFrame(
        [
            {
                "type": "LEO",
                "designator": "NOAA-15",
                "common_name": "NOAA 15",
                "norad_id": 25338,
                "intl_designator": "1998-030A",
            }
        ]
    )
    fallback = pd.DataFrame(
        [
            {
                "sat_name": "NOAA 15",
                "norad_id": 25338,
                "at_time_utc": pd.Timestamp("2026-10-20T12:00:00Z"),
                "lat_dd": 38.0,
                "lon_dd": -74.0,
                "alt_km": 850.0,
                "footprint_radius_km": 3000.0,
                "tle_epoch_utc": pd.Timestamp("2026-10-20T11:00:00Z"),
                "tle_age_hours": 1.0,
                "source": "fallback:nearest",
                "track_coords": None,
                "track_start_utc": None,
                "track_end_utc": None,
                "next_pass_marker": None,
                "_variant": "suggested",
                "distance_km": 100.0,
                "sat_type": "LEO",
            }
        ]
    )
    captured = {}
    monkeypatch.setattr(sat_pipeline, "load_sat_reference", lambda: reference)
    monkeypatch.setattr(sat_pipeline, "resolve_reporting_sat", lambda _alert: None)
    monkeypatch.setattr(
        sat_pipeline,
        "load_tle_snapshot",
        lambda _ids: pd.DataFrame(
            columns=["name", "norad_id", "tle_line1", "tle_line2", "epoch_utc"]
        ),
    )

    def nearest(lat, lon, at_time, **kwargs):
        captured.update(lat=lat, lon=lon, at_time=at_time, kwargs=kwargs)
        return fallback.copy()

    monkeypatch.setattr(sat_pipeline, "_fallback_nearest_leo", nearest)
    monkeypatch.setattr(sat_pipeline, "_visible_candidates", lambda *_a, **_k: pd.DataFrame())
    monkeypatch.setattr(sat_pipeline, "_upcoming_passes", lambda *_a, **_k: pd.DataFrame())
    alert = pd.DataFrame(
        [
            {
                "alert_id": "98372",
                "alert_time_utc": pd.Timestamp("2026-10-20T12:00:00Z"),
                "alert_lat_dd": 37.5,
                "alert_lon_dd": -75.0,
            }
        ]
    )

    overlay = sat_pipeline.build_sat_overlay_df(
        alert, types=("LEO",), use_tle=True, fallback_to_nearest=True
    )

    assert len(overlay) == 1
    assert overlay.iloc[0]["role"] == "suggested"
    assert overlay.iloc[0]["source"] == "fallback:nearest"
    assert overlay.iloc[0]["sat_type"] == "LEO"
    assert captured["lat"] == 37.5
    assert captured["lon"] == -75.0
    assert captured["kwargs"]["types"] == ("LEO",)


def test_upcoming_enrichment_survives_visible_satellite_failure(monkeypatch):
    reference = pd.DataFrame(
        [
            {
                "type": "LEO",
                "designator": "NOAA-15",
                "common_name": "NOAA 15",
                "norad_id": 25338,
                "intl_designator": "1998-030A",
            }
        ]
    )
    upcoming = pd.DataFrame(
        [
            {
                "sat_name": "NOAA 15",
                "norad_id": 25338,
                "at_time_utc": pd.Timestamp("2026-10-20T12:30:00Z"),
                "lat_dd": 38.0,
                "lon_dd": -74.0,
                "alt_km": 850.0,
                "footprint_radius_km": 3000.0,
                "tle_epoch_utc": pd.Timestamp("2026-10-20T11:00:00Z"),
                "role": "upcoming",
                "sat_type": "LEO",
            }
        ]
    )
    captured = {}
    monkeypatch.setattr(sat_pipeline, "load_sat_reference", lambda: reference)
    monkeypatch.setattr(sat_pipeline, "resolve_reporting_sat", lambda _alert: None)
    monkeypatch.setattr(
        sat_pipeline, "load_tle_snapshot", lambda _ids: pd.DataFrame()
    )
    monkeypatch.setattr(
        sat_pipeline,
        "_visible_candidates",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("visible failed")),
    )

    def upcoming_passes(*_args, **kwargs):
        captured.update(kwargs)
        return upcoming.copy()

    monkeypatch.setattr(sat_pipeline, "_upcoming_passes", upcoming_passes)
    monkeypatch.setenv("RDS_SAT_TOPN_NEARBY", "invalid")
    monkeypatch.setenv("RDS_SAT_MAX_CANDIDATES", "invalid")
    monkeypatch.setenv("RDS_SAT_MAX_HOURS", "nan")
    monkeypatch.setenv("RDS_SAT_TOPN_UPCOMING", "0")
    alert = pd.DataFrame(
        [
            {
                "alert_id": "98372",
                "alert_time_utc": pd.Timestamp("2026-10-20T12:00:00Z"),
                "alert_lat_dd": 37.5,
                "alert_lon_dd": -75.0,
            }
        ]
    )

    overlay = sat_pipeline.build_sat_overlay_df(alert, use_tle=True)

    assert len(overlay) == 1
    assert overlay.iloc[0]["role"] == "upcoming"
    assert captured["max_candidates"] == 256
    assert captured["max_hours"] == 12.0
    assert captured["top_n"] == 4


def test_satellite_pipeline_does_not_fetch_context_when_tle_disabled(monkeypatch):
    monkeypatch.setattr(
        sat_pipeline,
        "_visible_candidates",
        lambda *_a, **_k: pytest.fail("visible satellites fetched with TLE disabled"),
    )
    monkeypatch.setattr(
        sat_pipeline,
        "_upcoming_passes",
        lambda *_a, **_k: pytest.fail("upcoming passes fetched with TLE disabled"),
    )
    alert = pd.DataFrame(
        [
            {
                "alert_id": "98372",
                "alert_time_utc": pd.Timestamp("2026-10-20T12:00:00Z"),
                "alert_lat_dd": 37.5,
                "alert_lon_dd": -75.0,
            }
        ]
    )

    overlay = sat_pipeline.build_sat_overlay_df(alert, use_tle=False)

    assert overlay.empty


def test_empty_nearest_fallback_preserves_overlay_contract(monkeypatch):
    monkeypatch.setattr(
        sat_pipeline, "resolve_reporting_sat", lambda _alert: None
    )
    monkeypatch.setattr(
        sat_pipeline, "_fallback_nearest_leo", lambda *_a, **_k: pd.DataFrame()
    )
    monkeypatch.setattr(
        sat_pipeline, "_visible_candidates", lambda *_a, **_k: pd.DataFrame()
    )
    monkeypatch.setattr(
        sat_pipeline, "_upcoming_passes", lambda *_a, **_k: pd.DataFrame()
    )
    alert = pd.DataFrame(
        [
            {
                "alert_id": "98372",
                "alert_time_utc": pd.Timestamp("2026-10-20T12:00:00Z"),
                "alert_lat_dd": 37.5,
                "alert_lon_dd": -75.0,
            }
        ]
    )

    overlay = sat_pipeline.build_sat_overlay_df(
        alert, use_tle=True, fallback_to_nearest=True
    )

    assert overlay.empty
    assert "norad_id" in overlay.columns
