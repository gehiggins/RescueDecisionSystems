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


def test_satellite_reference_resolves_sample_alert_goes_16(monkeypatch, tmp_path):
    monkeypatch.setattr(
        sat_fetch_tle,
        "BASELINE_CSV_PATH",
        tmp_path / "missing-satellite-reference.csv",
    )

    satellite = sat_fetch_tle.resolve_reporting_sat(
        pd.DataFrame([{"satellite_name": "GOES-16"}])
    )

    assert satellite["type"] == "GEO"
    assert satellite["norad_id"] == 41866
    assert satellite["intl_designator"] == "2016-071A"


def test_satellite_reference_path_is_repository_relative():
    expected = (
        Path(sat_fetch_tle.__file__).resolve().parents[2]
        / "data"
        / "reference"
        / "sarsat_satellites_baseline.csv"
    )

    assert sat_fetch_tle.BASELINE_CSV_PATH == expected


@pytest.mark.parametrize("alert", [None, pd.DataFrame(), "not-a-dataframe"])
def test_reporting_satellite_resolver_rejects_invalid_alert_input(
    monkeypatch, alert
):
    monkeypatch.setattr(
        sat_fetch_tle,
        "load_sat_reference",
        lambda: pytest.fail("reference loaded for invalid alert input"),
    )

    assert sat_fetch_tle.resolve_reporting_sat(alert) is None


@pytest.mark.parametrize("name", [None, pd.NA, "", "   ", "["])
def test_reporting_satellite_resolver_ignores_missing_or_regex_like_names(name):
    alert = pd.DataFrame([{"satellite_name": name}])

    assert sat_fetch_tle.resolve_reporting_sat(alert) is None


def test_reporting_satellite_resolver_uses_literal_partial_name_match(caplog):
    alert = pd.DataFrame([{"satellite_name": "NOAA"}])

    satellite = sat_fetch_tle.resolve_reporting_sat(alert)

    assert satellite["common_name"] == "NOAA 15"
    assert "Multiple satellite matches" in caplog.text


def test_reporting_satellite_resolver_skips_blank_name_and_uses_designator():
    alert = pd.DataFrame([{"satellite_name": pd.NA, "sat_designator": "NOAA-18"}])

    satellite = sat_fetch_tle.resolve_reporting_sat(alert)

    assert satellite["norad_id"] == 28654


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


def test_satellite_pipeline_applies_norad_override_to_resolved_reference(
    monkeypatch,
):
    requested = []

    def capture_tle_request(identifiers):
        requested.append(identifiers)
        return pd.DataFrame()

    monkeypatch.setattr(sat_pipeline, "load_tle_snapshot", capture_tle_request)
    alert = pd.DataFrame(
        [
            {
                "alert_id": "98372",
                "alert_time_utc": pd.Timestamp("2026-10-20T12:00:00Z"),
                "satellite_name": "NOAA 15",
                "norad_id": 28654,
            }
        ]
    )

    overlay = sat_pipeline.build_sat_overlay_df(alert, use_tle=True)

    assert overlay.empty
    assert requested == [["28654"]]


@pytest.mark.parametrize(
    "invalid_norad_id",
    [True, [], {}, 25338.5, "25338.5", "not-a-catalog-id", 0, -1],
)
def test_satellite_pipeline_ignores_invalid_norad_override(
    monkeypatch, invalid_norad_id
):
    monkeypatch.setattr(
        sat_pipeline,
        "load_sat_reference",
        lambda: pd.DataFrame(
            columns=["type", "designator", "common_name", "norad_id"]
        ),
    )
    monkeypatch.setattr(sat_pipeline, "resolve_reporting_sat", lambda _alert: None)
    monkeypatch.setattr(
        sat_pipeline,
        "load_tle_snapshot",
        lambda _identifiers: pytest.fail("invalid NORAD ID reached TLE lookup"),
    )
    alert = pd.DataFrame(
        [
            {
                "alert_time_utc": pd.Timestamp("2026-10-20T12:00:00Z"),
                "norad_id": invalid_norad_id,
            }
        ]
    )

    overlay = sat_pipeline.build_sat_overlay_df(alert, use_tle=True)

    assert overlay.empty


def test_satellite_pipeline_handles_nullable_alert_satellite_names(monkeypatch):
    requested = []
    monkeypatch.setattr(sat_pipeline, "resolve_reporting_sat", lambda _alert: None)
    monkeypatch.setattr(
        sat_pipeline,
        "load_tle_snapshot",
        lambda identifiers: requested.append(identifiers) or pd.DataFrame(),
    )
    alert = pd.DataFrame(
        [
            {
                "alert_time_utc": pd.Timestamp("2026-10-20T12:00:00Z"),
                "norad_id": 25338,
                "sat_name": pd.NA,
                "reporting_sat": ["not", "scalar"],
            }
        ]
    )

    overlay = sat_pipeline.build_sat_overlay_df(alert, use_tle=True)

    assert overlay.empty
    assert requested == [["25338"]]


def test_satellite_pipeline_uses_designator_when_reference_name_is_missing(
    monkeypatch,
):
    requested = []
    monkeypatch.setattr(
        sat_pipeline,
        "resolve_reporting_sat",
        lambda _alert: {
            "common_name": pd.NA,
            "designator": "NOAA-15",
            "norad_id": 25338,
        },
    )
    monkeypatch.setattr(
        sat_pipeline,
        "load_tle_snapshot",
        lambda identifiers: requested.append(identifiers) or pd.DataFrame(),
    )
    alert = pd.DataFrame(
        [{"alert_time_utc": pd.Timestamp("2026-10-20T12:00:00Z")}]
    )

    overlay = sat_pipeline.build_sat_overlay_df(alert, use_tle=True)

    assert overlay.empty
    assert requested == [["25338"]]


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


def test_satellite_popup_escapes_provider_display_values():
    unsafe_name = '<img src=x onerror="alert(1)">'
    unsafe_role = '<script>alert("role")</script>'

    popup = sat_pipeline._build_sat_popup(
        {
            "sat_name": unsafe_name,
            "norad_id": 25338,
            "role": unsafe_role,
            "tle_epoch_utc": pd.NaT,
            "tle_age_hours": float("nan"),
            "at_time_utc": pd.Timestamp("2026-10-20T12:00:00Z"),
            "next_pass_marker": None,
        }
    )

    assert unsafe_name not in popup
    assert unsafe_role not in popup
    assert '&lt;img src=x onerror=&quot;alert(1)&quot;&gt;' in popup
    assert '&lt;script&gt;alert(&quot;role&quot;)&lt;/script&gt;' in popup
    assert "<b>" in popup


def test_satellite_popup_tolerates_nullable_and_malformed_provider_values():
    popup = sat_pipeline._build_sat_popup(
        {
            "sat_name": pd.NA,
            "norad_id": "not-a-number",
            "role": pd.NA,
            "tle_epoch_utc": ["not", "a", "scalar"],
            "tle_age_hours": "unknown",
            "at_time_utc": pd.NaT,
            "next_pass_marker": pd.NA,
        }
    )

    assert popup == "<b>Unknown</b><br/>Role: <b></b>"
    assert "not-a-number" not in popup


def test_satellite_popup_preserves_numeric_text_norad_id():
    popup = sat_pipeline._build_sat_popup(
        {
            "sat_name": "NOAA 15",
            "norad_id": "25338",
            "role": "reported",
            "at_time_utc": "2026-10-20T12:00:00Z",
        }
    )

    assert "NOAA 15</b> (25338)" in popup
    assert "Time@alert: 2026-10-20 12:00Z" in popup


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
    def reject_stale_propagation(*_args, **_kwargs):
        pytest.fail("stale TLE reached orbital propagation")

    monkeypatch.setattr(sat_pipeline, "compute_subpoint_at", reject_stale_propagation)
    monkeypatch.setattr(sat_pipeline, "compute_short_track", reject_stale_propagation)
    monkeypatch.setattr(
        sat_pipeline, "find_next_pass_marker", reject_stale_propagation
    )
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


@pytest.mark.parametrize(
    ("alert_lat", "alert_lon"),
    [([], -75.5), (37.5, {}), (True, -75.5), (37.5, 181.0)],
)
def test_malformed_next_pass_coordinates_preserve_valid_satellite_overlay(
    monkeypatch, alert_lat, alert_lon
):
    tle = pd.DataFrame(
        [
            {
                "name": "NOAA 15",
                "norad_id": 25338,
                "tle_line1": "line 1",
                "tle_line2": "line 2",
                "epoch_utc": pd.Timestamp("2026-10-20T12:00:00Z"),
            }
        ]
    )
    monkeypatch.setattr(sat_pipeline, "load_tle_snapshot", lambda _ids: tle)
    monkeypatch.setattr(
        sat_pipeline, "compute_subpoint_at", lambda *_args: (38.0, -74.0, 850.0)
    )
    monkeypatch.setattr(
        sat_pipeline,
        "compute_short_track",
        lambda *_args, **_kwargs: (None, None, None),
    )
    monkeypatch.setattr(
        sat_pipeline,
        "find_next_pass_marker",
        lambda *_args, **_kwargs: pytest.fail(
            "malformed alert coordinates reached next-pass calculation"
        ),
    )
    alert = pd.DataFrame(
        [
            {
                "alert_time_utc": pd.Timestamp("2026-10-20T12:00:00Z"),
                "norad_id": 25338,
                "alert_lat_dd": alert_lat,
                "alert_lon_dd": alert_lon,
            }
        ]
    )

    overlay = sat_pipeline.build_sat_overlay_df(alert, use_tle=True)

    assert len(overlay) == 1
    assert overlay.iloc[0]["lat_dd"] == 38.0
    assert overlay.iloc[0]["next_pass_marker"] is None


@pytest.mark.parametrize("malformed_epoch", [[], {}, ["2026-10-20T12:00:00Z"]])
def test_malformed_tle_epoch_skips_propagation(monkeypatch, malformed_epoch):
    tle = pd.DataFrame(
        [
            {
                "name": "NOAA 15",
                "norad_id": 25338,
                "tle_line1": "line 1",
                "tle_line2": "line 2",
                "epoch_utc": malformed_epoch,
            }
        ]
    )
    monkeypatch.setattr(sat_pipeline, "load_tle_snapshot", lambda _ids: tle)
    monkeypatch.setattr(
        sat_pipeline,
        "compute_subpoint_at",
        lambda *_args: pytest.fail("malformed TLE epoch reached propagation"),
    )
    alert = pd.DataFrame(
        [
            {
                "alert_time_utc": pd.Timestamp("2026-10-20T12:00:00Z"),
                "norad_id": 25338,
            }
        ]
    )

    overlay = sat_pipeline.build_sat_overlay_df(alert, use_tle=True)

    assert overlay.empty


def test_subpoint_propagation_failure_returns_empty_overlay(monkeypatch, caplog):
    tle = pd.DataFrame(
        [
            {
                "name": "NOAA 15",
                "norad_id": 25338,
                "tle_line1": "line 1",
                "tle_line2": "line 2",
                "epoch_utc": pd.Timestamp("2026-10-20T12:00:00Z"),
            }
        ]
    )
    monkeypatch.setattr(sat_pipeline, "load_tle_snapshot", lambda _ids: tle)
    def fail_subpoint(*_args):
        raise ValueError("bad orbital elements")

    monkeypatch.setattr(sat_pipeline, "compute_subpoint_at", fail_subpoint)
    monkeypatch.setattr(
        sat_pipeline,
        "compute_short_track",
        lambda *_args, **_kwargs: pytest.fail(
            "track calculation ran without a valid subpoint"
        ),
    )
    alert = pd.DataFrame(
        [
            {
                "alert_time_utc": pd.Timestamp("2026-10-20T12:00:00Z"),
                "norad_id": 25338,
            }
        ]
    )

    overlay = sat_pipeline.build_sat_overlay_df(alert, use_tle=True)

    assert overlay.empty
    assert "subpoint propagation failure" in caplog.text


@pytest.mark.parametrize(
    "malformed_subpoint",
    [
        None,
        ([], -74.0, 850.0),
        (38.0, -74.0, float("inf")),
        (95.0, -74.0, 850.0),
    ],
)
def test_malformed_propagated_subpoint_returns_empty_overlay(
    monkeypatch, malformed_subpoint
):
    tle = pd.DataFrame(
        [
            {
                "name": "NOAA 15",
                "norad_id": 25338,
                "tle_line1": "line 1",
                "tle_line2": "line 2",
                "epoch_utc": pd.Timestamp("2026-10-20T12:00:00Z"),
            }
        ]
    )
    monkeypatch.setattr(sat_pipeline, "load_tle_snapshot", lambda _ids: tle)
    monkeypatch.setattr(
        sat_pipeline, "compute_subpoint_at", lambda *_args: malformed_subpoint
    )
    monkeypatch.setattr(
        sat_pipeline,
        "compute_short_track",
        lambda *_args, **_kwargs: pytest.fail(
            "track calculation ran without a valid subpoint"
        ),
    )
    alert = pd.DataFrame(
        [
            {
                "alert_time_utc": pd.Timestamp("2026-10-20T12:00:00Z"),
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
    assert cached["cache_hit"] is False


@pytest.mark.parametrize(
    "payload",
    [
        "error\nnot a TLE\nstill not a TLE\n",
        (
            "WRONG SATELLITE\n"
            "1 43055U 17079A   26293.00000000  .00000000  00000-0  00000-0 0  9999\n"
            "2 43055  56.0000 100.0000 0001000  10.0000 350.0000  2.00000000    00\n"
        ),
        (
            "MISMATCHED LINES\n"
            "1 25338U 98030A   26293.00000000  .00000000  00000-0  00000-0 0  9999\n"
            "2 43055  56.0000 100.0000 0001000  10.0000 350.0000  2.00000000    00\n"
        ),
    ],
)
def test_tle_resolver_does_not_cache_malformed_or_wrong_catalog_payloads(
    monkeypatch, caplog, payload
):
    cache = {}
    monkeypatch.setattr(sat_fetch_tle, "_TLE_CACHE_CATNR", cache)
    monkeypatch.setattr(sat_fetch_tle, "_TLE_CACHE_GROUP", {})
    monkeypatch.setattr(sat_fetch_tle, "_TLE_PROVIDER_UNAVAILABLE_UNTIL", 0.0)

    class Response:
        status_code = 200
        text = payload

    monkeypatch.setattr(sat_fetch_tle.requests, "get", lambda *_a, **_k: Response())

    snapshot = sat_fetch_tle.load_tle_snapshot(["25338"])

    assert snapshot.empty
    assert cache == {}
    assert "Ignoring malformed TLE response" in caplog.text


def test_tle_group_resolver_retries_after_malformed_response(monkeypatch, caplog):
    monkeypatch.setattr(sat_fetch_tle, "_TLE_CACHE_CATNR", {})
    monkeypatch.setattr(sat_fetch_tle, "_TLE_CACHE_GROUP", {})
    monkeypatch.setattr(sat_fetch_tle, "_TLE_PROVIDER_UNAVAILABLE_UNTIL", 0.0)

    responses = iter(
        [
            "BAD RECORD\ninvalid line one\ninvalid line two\n",
            (
                "TEST SATELLITE\n"
                "1 43055U 17079A   26293.00000000  .00000000  00000-0  00000-0 0  9999\n"
                "2 43055  56.0000 100.0000 0001000  10.0000 350.0000  2.00000000    00\n"
            ),
        ]
    )

    class Response:
        status_code = 200

        def __init__(self, text):
            self.text = text

    monkeypatch.setattr(
        sat_fetch_tle.requests,
        "get",
        lambda *_a, **_k: Response(next(responses)),
    )

    malformed = sat_fetch_tle.load_tle_snapshot(["galileo"])
    recovered = sat_fetch_tle.load_tle_snapshot(["galileo"])

    assert malformed.empty
    assert len(recovered) == 1
    assert recovered.iloc[0]["norad_id"] == 43055
    assert recovered.iloc[0]["cache_hit"] == False
    assert len(sat_fetch_tle._TLE_CACHE_GROUP["galileo"]) == 1
    assert "Ignoring malformed TLE record" in caplog.text


def test_tle_resolver_deduplicates_catalog_requests(monkeypatch):
    calls = []
    monkeypatch.setattr(sat_fetch_tle, "_TLE_CACHE_CATNR", {})
    monkeypatch.setattr(sat_fetch_tle, "_TLE_CACHE_GROUP", {})
    monkeypatch.setattr(sat_fetch_tle, "_TLE_PROVIDER_UNAVAILABLE_UNTIL", 0.0)

    class Response:
        status_code = 200
        text = (
            "NOAA 15\n"
            "1 25338U 98030A   26293.00000000  .00000000  00000-0  00000-0 0  9999\n"
            "2 25338  98.7000 100.0000 0001000  10.0000 350.0000 14.00000000    00\n"
        )

    def fake_get(url, **_kwargs):
        calls.append(url)
        return Response()

    monkeypatch.setattr(sat_fetch_tle.requests, "get", fake_get)

    snapshot = sat_fetch_tle.load_tle_snapshot(["25338", 25338, "025338"])

    assert len(snapshot) == 1
    assert snapshot.iloc[0]["norad_id"] == 25338
    assert len(calls) == 1


def test_tle_resolver_ignores_invalid_request_values(monkeypatch):
    monkeypatch.setattr(sat_fetch_tle, "_TLE_CACHE_CATNR", {})
    monkeypatch.setattr(sat_fetch_tle, "_TLE_CACHE_GROUP", {})
    monkeypatch.setattr(sat_fetch_tle, "_TLE_PROVIDER_UNAVAILABLE_UNTIL", 0.0)
    monkeypatch.setattr(
        sat_fetch_tle.requests,
        "get",
        lambda *_a, **_k: pytest.fail("invalid request value triggered network I/O"),
    )

    snapshot = sat_fetch_tle.load_tle_snapshot(
        [True, -1, 0, 25338.5, "BAD", "RANDOM", None, object()]
    )

    assert snapshot.empty


@pytest.mark.parametrize(
    ("designator", "group"),
    [("E1", "GALILEO"), ("R12", "GLONASS"), ("B3", "BEIDOU"), ("C7", "BEIDOU")],
)
def test_tle_resolver_accepts_bounded_group_designators(
    monkeypatch, designator, group
):
    calls = []
    monkeypatch.setattr(sat_fetch_tle, "_TLE_CACHE_CATNR", {})
    monkeypatch.setattr(sat_fetch_tle, "_TLE_CACHE_GROUP", {})
    monkeypatch.setattr(sat_fetch_tle, "_TLE_PROVIDER_UNAVAILABLE_UNTIL", 0.0)

    class Response:
        status_code = 404
        text = ""

    def fake_get(url, **_kwargs):
        calls.append(url)
        return Response()

    monkeypatch.setattr(sat_fetch_tle.requests, "get", fake_get)

    sat_fetch_tle.load_tle_snapshot([designator])

    assert calls == [
        f"https://celestrak.org/NORAD/elements/gp.php?GROUP={group}&FORMAT=TLE"
    ]


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


@pytest.mark.parametrize("step_s", [0, -1, float("nan"), "bad", True])
def test_short_track_rejects_invalid_sampling_step(monkeypatch, step_s):
    monkeypatch.setattr(
        sat_compute_groundtrack,
        "_make_sat",
        lambda *_a: pytest.fail("invalid sampling must be rejected before propagation"),
    )

    with pytest.raises(ValueError, match="step_s"):
        sat_compute_groundtrack.compute_short_track(
            "line 1",
            "line 2",
            pd.Timestamp("2026-10-20T12:00:00Z").to_pydatetime(),
            step_s=step_s,
        )


@pytest.mark.parametrize(
    "invalid_time", [None, "not-a-time", 12345, True, ["2026-10-20T12:00:00Z"]]
)
def test_short_track_rejects_invalid_center_time_before_propagation(
    monkeypatch, invalid_time
):
    monkeypatch.setattr(
        sat_compute_groundtrack,
        "_make_sat",
        lambda *_a: pytest.fail("invalid time must be rejected before propagation"),
    )

    with pytest.raises(ValueError, match="center_utc"):
        sat_compute_groundtrack.compute_short_track(
            "line 1", "line 2", invalid_time
        )


def test_subpoint_rejects_invalid_time_before_propagation(monkeypatch):
    monkeypatch.setattr(
        sat_compute_groundtrack,
        "_make_sat",
        lambda *_a: pytest.fail("invalid time must be rejected before propagation"),
    )

    with pytest.raises(ValueError, match="at_time_utc"):
        sat_compute_groundtrack.compute_subpoint_at(
            "line 1", "line 2", "not-a-time"
        )


@pytest.mark.parametrize("max_hours", [0, -1, float("inf"), "bad", False])
def test_next_pass_rejects_invalid_search_window(monkeypatch, max_hours):
    monkeypatch.setattr(
        sat_compute_groundtrack,
        "_make_sat",
        lambda *_a: pytest.fail("invalid sampling must be rejected before propagation"),
    )

    with pytest.raises(ValueError, match="max_hours"):
        sat_compute_groundtrack.find_next_pass_marker(
            "line 1",
            "line 2",
            alert_latlon=(37.5, -75.0),
            start_utc=pd.Timestamp("2026-10-20T12:00:00Z").to_pydatetime(),
            max_hours=max_hours,
        )


@pytest.mark.parametrize(
    "alert_latlon",
    [("bad", -75.0), (91.0, -75.0), (37.5, -181.0), (True, -75.0), [37.5]],
)
def test_next_pass_rejects_invalid_alert_position_before_propagation(
    monkeypatch, alert_latlon
):
    monkeypatch.setattr(
        sat_compute_groundtrack,
        "_make_sat",
        lambda *_a: pytest.fail(
            "invalid alert position must be rejected before propagation"
        ),
    )

    with pytest.raises(ValueError, match="alert_latlon"):
        sat_compute_groundtrack.find_next_pass_marker(
            "line 1",
            "line 2",
            alert_latlon=alert_latlon,
            start_utc="2026-10-20T12:00:00Z",
            max_hours=1,
        )


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


@pytest.mark.parametrize(
    "candidate_builder",
    [sat_pipeline._visible_candidates, sat_pipeline._upcoming_passes],
    ids=["visible", "upcoming"],
)
def test_secondary_candidate_paths_skip_malformed_provider_catalog_id(
    monkeypatch, candidate_builder
):
    reference = pd.DataFrame([{"type": "LEO", "norad_id": 25338}])
    malformed_tle = pd.DataFrame(
        [
            {
                "name": "Malformed satellite",
                "norad_id": [25338],
                "tle_line1": "line 1",
                "tle_line2": "line 2",
                "epoch_utc": "2026-10-20T11:00:00Z",
            }
        ]
    )
    monkeypatch.setattr(sat_pipeline, "load_sat_reference", lambda: reference)
    monkeypatch.setattr(
        sat_pipeline, "load_tle_snapshot", lambda _ids: malformed_tle
    )
    monkeypatch.setattr(
        sat_pipeline,
        "compute_subpoint_at",
        lambda *_args: pytest.fail("invalid provider catalog ID reached propagation"),
    )

    result = candidate_builder(
        37.5,
        -75.0,
        pd.Timestamp("2026-10-20T12:00:00Z"),
        exclude_norad=set(),
    )

    assert result.empty


def test_secondary_candidate_request_filters_invalid_reference_catalog_ids(
    monkeypatch,
):
    requested = []
    reference = pd.DataFrame(
        [
            {"type": "LEO", "norad_id": True},
            {"type": "LEO", "norad_id": 25338.5},
            {"type": "LEO", "norad_id": [28654]},
            {"type": "LEO", "norad_id": 25338},
            {"type": "LEO", "norad_id": "25338"},
        ]
    )
    monkeypatch.setattr(sat_pipeline, "load_sat_reference", lambda: reference)
    monkeypatch.setattr(
        sat_pipeline,
        "load_tle_snapshot",
        lambda identifiers: requested.extend(identifiers) or pd.DataFrame(),
    )

    result = sat_pipeline._visible_candidates(
        37.5,
        -75.0,
        pd.Timestamp("2026-10-20T12:00:00Z"),
        exclude_norad=set(),
    )

    assert result.empty
    assert requested == ["25338"]


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
