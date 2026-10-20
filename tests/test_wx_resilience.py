import pandas as pd

from flask_app.app import wx_obs, wx_pipeline


def _station_observation():
    return pd.DataFrame(
        [
            {
                "source_id": "TEST-STATION",
                "valid_utc": pd.Timestamp("2026-10-19T18:00:00Z"),
                "source_type": "station",
                "provider": "meteostat",
                "lat": 37.5,
                "lon": -75.5,
            }
        ]
    )


def test_weather_aggregation_continues_when_open_meteo_raises(
    monkeypatch, caplog
):
    def fail_open_meteo(*_args, **_kwargs):
        raise RuntimeError("open meteo unavailable")

    monkeypatch.setattr(
        wx_obs,
        "fetch_open_meteo_obs",
        fail_open_meteo,
    )
    monkeypatch.setattr(
        wx_obs,
        "fetch_meteostat_obs_near",
        lambda *_args, **_kwargs: _station_observation(),
    )

    result = wx_obs.build_wx_obs_for_area(
        [(37.5, -75.5)],
        pd.Timestamp("2026-10-19T12:00:00Z"),
        pd.Timestamp("2026-10-19T18:00:00Z"),
    )

    assert result["source_id"].tolist() == ["TEST-STATION"]
    assert "open meteo unavailable" in caplog.text


def test_weather_aggregation_returns_empty_when_all_providers_raise(
    monkeypatch, caplog
):
    def fail_provider(*_args, **_kwargs):
        raise RuntimeError("provider unavailable")

    monkeypatch.setattr(wx_obs, "fetch_open_meteo_obs", fail_provider)
    monkeypatch.setattr(wx_obs, "fetch_meteostat_obs_near", fail_provider)

    result = wx_obs.build_wx_obs_for_area(
        [(37.5, -75.5)],
        pd.Timestamp("2026-10-19T12:00:00Z"),
        pd.Timestamp("2026-10-19T18:00:00Z"),
    )

    assert result.empty
    assert list(result.columns) == wx_obs._WX_OBS_COLS
    assert "Open-Meteo weather retrieval failed" in caplog.text
    assert "Meteostat weather retrieval failed" in caplog.text


def test_weather_reducer_preserves_source_identity():
    observations = pd.DataFrame(
        [
            {
                "source_id": "A",
                "valid_utc": pd.Timestamp("2026-10-19T17:00:00Z"),
            },
            {
                "source_id": "A",
                "valid_utc": pd.Timestamp("2026-10-19T18:00:00Z"),
            },
            {
                "source_id": "B",
                "valid_utc": pd.Timestamp("2026-10-19T16:00:00Z"),
            },
        ]
    )

    result = wx_obs.reduce_to_nearest_or_latest(
        observations,
        target_utc=pd.Timestamp("2026-10-19T18:00:00Z"),
        max_age_hours=24,
    )

    assert result["source_id"].tolist() == ["A", "B"]
    assert result.loc[result["source_id"] == "A", "valid_utc"].item() == (
        pd.Timestamp("2026-10-19T18:00:00Z")
    )


def test_malformed_provider_rows_do_not_discard_valid_observations(
    monkeypatch, caplog
):
    malformed = pd.DataFrame(
        [{"valid_utc": "not-a-time", "provider": "open-meteo"}]
    )
    monkeypatch.setattr(
        wx_obs,
        "fetch_open_meteo_obs",
        lambda *_args, **_kwargs: malformed,
    )
    monkeypatch.setattr(
        wx_obs,
        "fetch_meteostat_obs_near",
        lambda *_args, **_kwargs: _station_observation(),
    )

    result = wx_obs.build_wx_obs_for_area(
        [(37.5, -75.5)],
        pd.Timestamp("2026-10-19T12:00:00Z"),
        pd.Timestamp("2026-10-19T18:00:00Z"),
    )

    assert result["source_id"].tolist() == ["TEST-STATION"]
    assert "Discarded 1 malformed weather observation row" in caplog.text


def test_all_malformed_weather_rows_return_empty_schema(monkeypatch, caplog):
    malformed = pd.DataFrame(
        [
            {"source_id": "", "valid_utc": "2026-10-19T18:00:00Z"},
            {"source_id": "BAD-TIME", "valid_utc": "not-a-time"},
        ]
    )
    monkeypatch.setattr(
        wx_obs,
        "fetch_open_meteo_obs",
        lambda *_args, **_kwargs: malformed,
    )
    monkeypatch.setattr(
        wx_obs,
        "fetch_meteostat_obs_near",
        lambda *_args, **_kwargs: pd.DataFrame(),
    )

    result = wx_obs.build_wx_obs_for_area(
        [(37.5, -75.5)],
        pd.Timestamp("2026-10-19T12:00:00Z"),
        pd.Timestamp("2026-10-19T18:00:00Z"),
    )

    assert result.empty
    assert list(result.columns) == wx_obs._WX_OBS_COLS
    assert "Discarded 2 malformed weather observation row" in caplog.text


def test_weather_pipeline_discards_invalid_targets_and_keeps_valid_target(
    monkeypatch, caplog
):
    captured = {}

    def capture_targets(**kwargs):
        captured["ref_points"] = kwargs["ref_points"]
        return _station_observation()

    monkeypatch.setattr(wx_pipeline, "build_wx_obs_for_area", capture_targets)
    targets = pd.DataFrame(
        [
            {"lat": "bad", "lon": -75.0},
            {"lat": 91.0, "lon": -75.0},
            {"lat": 37.5, "lon": -75.5},
            {"lat": 37.5, "lon": -75.5},
        ]
    )

    result = wx_pipeline.run_wx_pipeline(targets)

    assert captured["ref_points"] == [(37.5, -75.5)]
    assert result["source_id"].tolist() == ["TEST-STATION"]
    assert "Discarded 2 invalid weather target coordinate" in caplog.text


def test_weather_pipeline_skips_providers_when_all_targets_invalid(
    monkeypatch, caplog
):
    monkeypatch.setattr(
        wx_pipeline,
        "build_wx_obs_for_area",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("weather provider called without a valid target")
        ),
    )
    targets = pd.DataFrame(
        [
            {"lat": pd.NA, "lon": -75.0},
            {"lat": float("inf"), "lon": -75.0},
            {"lat": 37.5, "lon": -181.0},
        ]
    )

    result = wx_pipeline.run_wx_pipeline(targets)

    assert result.empty
    assert list(result.columns) == wx_obs._WX_OBS_COLS
    assert "No valid weather target coordinates" in caplog.text


def test_weather_aggregator_filters_invalid_reference_points(
    monkeypatch, caplog
):
    calls = []

    def open_meteo(lat, lon, *_args, **_kwargs):
        calls.append(("open-meteo", lat, lon))
        return pd.DataFrame()

    def meteostat(lat, lon, *_args, **_kwargs):
        calls.append(("meteostat", lat, lon))
        return pd.DataFrame()

    monkeypatch.setattr(wx_obs, "fetch_open_meteo_obs", open_meteo)
    monkeypatch.setattr(wx_obs, "fetch_meteostat_obs_near", meteostat)

    result = wx_obs.build_wx_obs_for_area(
        [("bad", -75.0), (91.0, 0.0), (37.5,), None, (37.5, -75.5)],
        pd.Timestamp("2026-10-19T12:00:00Z"),
        pd.Timestamp("2026-10-19T18:00:00Z"),
    )

    assert result.empty
    assert calls == [
        ("open-meteo", 37.5, -75.5),
        ("meteostat", 37.5, -75.5),
    ]
    assert "Discarded 4 invalid weather provider reference point" in caplog.text


def test_weather_aggregator_skips_providers_when_all_references_invalid(
    monkeypatch, caplog
):
    monkeypatch.setattr(
        wx_obs,
        "fetch_open_meteo_obs",
        lambda *_a, **_k: (_ for _ in ()).throw(
            AssertionError("Open-Meteo called with no valid reference")
        ),
    )
    monkeypatch.setattr(
        wx_obs,
        "fetch_meteostat_obs_near",
        lambda *_a, **_k: (_ for _ in ()).throw(
            AssertionError("Meteostat called with no valid reference")
        ),
    )

    result = wx_obs.build_wx_obs_for_area(
        [{"lat": pd.NA, "lon": -75.0}, (float("inf"), 0.0)],
        pd.Timestamp("2026-10-19T12:00:00Z"),
        pd.Timestamp("2026-10-19T18:00:00Z"),
    )

    assert result.empty
    assert list(result.columns) == wx_obs._WX_OBS_COLS
    assert "Discarded 2 invalid weather provider reference point" in caplog.text
