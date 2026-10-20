"""Offline coverage for the NDBC metadata maintenance script."""

import importlib
from pathlib import Path

import pandas as pd

import generate_ndbc_station_metadata as generator


class _StationListResponse:
    text = '<a href="station_page.php?station=44009">44009</a>'

    @staticmethod
    def raise_for_status():
        return None


class _MetadataResponse:
    status_code = 200
    text = (
        '<h1>Test Station</h1>'
        '<p>Owned and maintained by NOAA Test Office</p>'
    )


class _HeadResponse:
    status_code = 200


def test_import_has_no_completion_side_effect(capsys):
    importlib.reload(generator)

    assert capsys.readouterr().out == ""


def test_station_metadata_uses_builtin_parser_and_request_timeouts(monkeypatch):
    calls = []
    monkeypatch.setattr(
        generator.requests,
        "get",
        lambda url, timeout: calls.append(("get", url, timeout))
        or _MetadataResponse(),
    )
    monkeypatch.setattr(
        generator.requests,
        "head",
        lambda url, timeout: calls.append(("head", url, timeout))
        or _HeadResponse(),
    )

    result = generator.fetch_station_metadata("44009", request_timeout=4)

    assert result["station_id"] == "44009"
    assert result["name"] == "Test Station"
    assert result["owner"] == "NOAA Test Office"
    assert result["preferred_data_source"] == "realtime2"
    assert len(calls) == 3
    assert all(call[2] == 4 for call in calls)


def test_main_writes_requested_outputs_without_live_network(monkeypatch, tmp_path):
    calls = {}

    def get_station_list(url, timeout):
        calls["request"] = (url, timeout)
        return _StationListResponse()

    def station_metadata(station_id, request_timeout):
        calls["station"] = (station_id, request_timeout)
        return {
            "station_id": station_id,
            "name": "Test buoy",
            "owner": "NOAA",
            "deployment_notes": "Test deployment",
            "has_realtime2": True,
            "has_fiveday2": False,
            "no_recent_data": False,
            "preferred_data_source": "realtime2",
        }

    monkeypatch.setattr(generator.requests, "get", get_station_list)
    monkeypatch.setattr(generator, "fetch_station_metadata", station_metadata)
    full_output = tmp_path / "nested" / "full.csv"
    summary_output = tmp_path / "nested" / "summary.csv"

    result = generator.main(
        [
            "--full-output",
            str(full_output),
            "--summary-output",
            str(summary_output),
            "--delay-seconds",
            "0",
            "--request-timeout",
            "3",
        ]
    )

    assert result == 0
    assert calls["station"] == ("44009", 3.0)
    assert pd.read_csv(full_output)["station_id"].astype(str).tolist() == ["44009"]
    summary = pd.read_csv(summary_output)
    assert summary.to_dict("records") == [
        {
            "owner": "NOAA",
            "preferred_data_source": "realtime2",
            "station_count": 1,
        }
    ]
    assert Path(generator.FULL_METADATA_FILE).is_absolute()
