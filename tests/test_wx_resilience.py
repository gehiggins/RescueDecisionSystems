import pandas as pd

from flask_app.app import wx_obs


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
