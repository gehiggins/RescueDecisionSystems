"""Offline regression coverage for the optional NDBC buoy adapter."""

import pandas as pd
import pytest

from flask_app.app import fetcher_ndbc_buoy


def test_embedded_ndbc_coordinates_are_normalized():
    station = pd.Series(
        {
            "station_id": "TEST",
            "name": "Test Buoy",
            "owner": "Agency\n37.500 N 74.250 W (37°30' N 74°15' W)",
        }
    )

    assert fetcher_ndbc_buoy._coordinates_from_metadata(station) == (
        37.5,
        -74.25,
    )


def test_single_buoy_uses_preferred_source_then_fallback(monkeypatch):
    calls = []
    monkeypatch.setattr(
        fetcher_ndbc_buoy,
        "parse_5day2_data",
        lambda station_id: calls.append(("5day2", station_id)) or None,
    )
    monkeypatch.setattr(
        fetcher_ndbc_buoy,
        "parse_realtime2_data",
        lambda station_id: calls.append(("realtime2", station_id))
        or {"observation_time": "2026-10-20T18:00:00Z", "wave_height": 1.2},
    )

    result = fetcher_ndbc_buoy.fetch_single_buoy("44009", "5day2")

    assert calls == [("5day2", "44009"), ("realtime2", "44009")]
    assert result.iloc[0]["source"] == "realtime2"
    assert result.iloc[0]["wave_height"] == pytest.approx(1.2)


def test_buoy_fetch_enriches_nearest_station_observation(monkeypatch):
    metadata = pd.DataFrame(
        [
            {
                "station_id": "NEAR",
                "name": "Near Buoy\n37.500 N 74.500 W",
                "preferred_data_source": "realtime2",
                "owner": "NOAA",
            },
            {
                "station_id": "FAR",
                "name": "Far Buoy\n20.000 N 150.000 W",
                "preferred_data_source": "none",
            },
        ]
    )
    monkeypatch.setattr(fetcher_ndbc_buoy, "load_buoy_metadata", lambda: metadata)
    monkeypatch.setattr(
        fetcher_ndbc_buoy,
        "fetch_single_buoy",
        lambda station_id, _preferred: pd.DataFrame(
            [
                {
                    "observation_time": "2026-10-20T18:00:00Z",
                    "temperature": 20.0,
                    "wind_speed": 6.0,
                    "wind_direction": 90.0,
                    "wave_height": 1.5,
                    "source": "realtime2",
                    "timelate": 0.5,
                }
            ]
        )
        if station_id == "NEAR"
        else pd.DataFrame(),
    )

    result = fetcher_ndbc_buoy.fetch_ndbc_buoy_data(37.5, -74.5, "A")

    near = result[result["station_id"] == "NEAR"].iloc[0]
    assert near["latitude"] == pytest.approx(37.5)
    assert near["longitude"] == pytest.approx(-74.5)
    assert near["distance_nm"] == pytest.approx(0.0)
    assert near["wave_height"] == pytest.approx(1.5)
    assert near["position_label"] == "A"
    assert result[result["station_id"] == "FAR"].iloc[0]["source"] == (
        "metadata_only"
    )


@pytest.mark.parametrize(
    ("latitude", "longitude"),
    [(91, 0), (0, 181), (float("nan"), 0), ("bad", 0)],
)
def test_buoy_fetch_rejects_invalid_reference_coordinates(
    monkeypatch, latitude, longitude
):
    monkeypatch.setattr(
        fetcher_ndbc_buoy,
        "load_buoy_metadata",
        lambda: pytest.fail("metadata loaded for invalid coordinates"),
    )

    result = fetcher_ndbc_buoy.fetch_ndbc_buoy_data(latitude, longitude, "A")

    assert result.empty
