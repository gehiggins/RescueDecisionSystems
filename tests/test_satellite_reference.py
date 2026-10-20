from pathlib import Path

import pandas as pd

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
