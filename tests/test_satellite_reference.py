from pathlib import Path

import pandas as pd
import pytest
import requests

from flask_app.app import sat_fetch_tle
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
