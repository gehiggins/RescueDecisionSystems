import pandas as pd
import pytest

from flask_app.app import wx_obs, wx_pipeline
from rds_engine.context import classify_zone_context


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


def test_weather_reducer_returns_one_row_per_source_with_duplicate_input_index():
    observations = pd.DataFrame(
        [
            {"source_id": "A", "valid_utc": "2026-10-19T17:00:00Z"},
            {"source_id": "A", "valid_utc": "2026-10-19T18:00:00Z"},
            {"source_id": "B", "valid_utc": "2026-10-19T16:00:00Z"},
        ],
        index=[0, 0, 1],
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


def test_weather_reducer_discards_malformed_timestamps():
    observations = pd.DataFrame(
        [
            {"source_id": "A", "valid_utc": "not-a-time"},
            {"source_id": "A", "valid_utc": "2026-10-19T18:00:00Z"},
            {"source_id": "B", "valid_utc": None},
        ]
    )

    result = wx_obs.reduce_to_nearest_or_latest(
        observations,
        target_utc="2026-10-19T18:00:00Z",
    )

    assert result["source_id"].tolist() == ["A"]
    assert result["valid_utc"].tolist() == [pd.Timestamp("2026-10-19T18:00:00Z")]


@pytest.mark.parametrize("max_age_hours", [-1, float("inf"), "bad"])
def test_weather_reducer_rejects_invalid_age_window(max_age_hours):
    with pytest.raises(ValueError, match="max_age_hours"):
        wx_obs.reduce_to_nearest_or_latest(
            _station_observation(),
            target_utc="2026-10-19T18:00:00Z",
            max_age_hours=max_age_hours,
        )


def test_weather_reducer_rejects_invalid_target_timestamp():
    with pytest.raises(ValueError, match="target_utc"):
        wx_obs.reduce_to_nearest_or_latest(
            _station_observation(), target_utc="not-a-time"
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


@pytest.mark.parametrize(
    "hours_back", [None, "bad", float("nan"), float("inf"), -1, 0]
)
def test_weather_pipeline_defaults_invalid_time_window(
    monkeypatch, caplog, hours_back
):
    captured = {}

    def capture_window(**kwargs):
        captured.update(kwargs)
        return _station_observation()

    monkeypatch.setattr(wx_pipeline, "build_wx_obs_for_area", capture_window)

    result = wx_pipeline.run_wx_pipeline(
        pd.DataFrame([{"lat": 37.5, "lon": -75.5}]),
        hours_back=hours_back,
    )

    assert result["source_id"].tolist() == ["TEST-STATION"]
    assert captured["end_utc"] - captured["start_utc"] == pd.Timedelta(hours=6)
    assert "Invalid weather hours_back" in caplog.text


@pytest.mark.parametrize(
    ("radius_km", "max_stations"),
    [
        ("bad", 5),
        (float("inf"), 5),
        (0, 5),
        (50, "bad"),
        (50, float("nan")),
        (50, 0),
        (50, 2.5),
    ],
)
def test_weather_pipeline_defaults_invalid_station_search_parameters(
    monkeypatch, caplog, radius_km, max_stations
):
    captured = {}

    def capture_parameters(**kwargs):
        captured.update(kwargs)
        return _station_observation()

    monkeypatch.setattr(wx_pipeline, "build_wx_obs_for_area", capture_parameters)

    result = wx_pipeline.run_wx_pipeline(
        pd.DataFrame([{"lat": 37.5, "lon": -75.5}]),
        radius_km=radius_km,
        max_stations=max_stations,
    )

    assert result["source_id"].tolist() == ["TEST-STATION"]
    assert captured["radius_km"] == 50.0
    assert captured["max_stations"] == 5
    assert "Invalid weather" in caplog.text


def test_weather_aggregator_normalizes_direct_search_parameters(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        wx_obs,
        "fetch_open_meteo_obs",
        lambda *_args, **_kwargs: pd.DataFrame(),
    )

    def capture_meteostat(*_args, **kwargs):
        captured.update(kwargs)
        return pd.DataFrame()

    monkeypatch.setattr(wx_obs, "fetch_meteostat_obs_near", capture_meteostat)

    result = wx_obs.build_wx_obs_for_area(
        [(37.5, -75.5)],
        pd.Timestamp("2026-10-19T12:00:00Z"),
        pd.Timestamp("2026-10-19T18:00:00Z"),
        radius_km=-1,
        max_stations=2.5,
    )

    assert result.empty
    assert captured["radius_km"] == 50.0
    assert captured["max_stations"] == 5


@pytest.mark.parametrize(
    ("explicit_value", "policy_value", "expected"),
    [(False, True, False), (True, False, True), (None, True, True)],
)
def test_weather_aggregator_preserves_explicit_marine_choice(
    monkeypatch, explicit_value, policy_value, expected
):
    captured = {}

    class Plan:
        include_marine = policy_value
        spatial_radius_km = 50.0
        station_limit = 5
        rationale = "test policy"

    monkeypatch.setattr(wx_obs, "make_wx_data_plan", lambda *_a, **_k: Plan())
    monkeypatch.setattr(
        wx_obs,
        "fetch_open_meteo_obs",
        lambda *_args, **kwargs: captured.update(kwargs) or pd.DataFrame(),
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
        include_marine=explicit_value,
    )

    assert result.empty
    assert captured["include_marine"] is expected


@pytest.mark.parametrize(
    ("coast_distance", "expected_zone"),
    [("3", "coastal_waters"), ("15", "near_shore_onshore"), ("16", "inland")],
)
def test_weather_context_normalizes_numeric_text_distance(
    coast_distance, expected_zone
):
    context = classify_zone_context(
        lat=37.5,
        lon=-75.5,
        error_radius_km="12.5",
        coast_distance_km=coast_distance,
    )

    assert context.error_radius_km == 12.5
    assert context.coast_distance_km == float(coast_distance)
    assert context.zone == expected_zone


@pytest.mark.parametrize("invalid", ["bad", -1, float("nan"), float("inf"), True])
def test_weather_context_treats_invalid_optional_distance_as_unknown(invalid):
    context = classify_zone_context(
        lat=37.5,
        lon=-75.5,
        error_radius_km=invalid,
        coast_distance_km=invalid,
    )

    assert context.error_radius_km is None
    assert context.coast_distance_km is None
    assert context.zone == "unknown"


def test_weather_aggregator_uses_normalized_context_metadata(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        wx_obs,
        "fetch_open_meteo_obs",
        lambda *_args, **kwargs: captured.update(kwargs) or pd.DataFrame(),
    )
    monkeypatch.setattr(
        wx_obs,
        "fetch_meteostat_obs_near",
        lambda *_args, **_kwargs: pd.DataFrame(),
    )

    result = wx_obs.build_wx_obs_for_area(
        [
            {
                "lat": 37.5,
                "lon": -75.5,
                "error_radius_km": "12",
                "coast_distance_km": "unknown",
            }
        ],
        pd.Timestamp("2026-10-19T12:00:00Z"),
        pd.Timestamp("2026-10-19T18:00:00Z"),
        include_marine=None,
    )

    assert result.empty
    assert captured["include_marine"] is True


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
