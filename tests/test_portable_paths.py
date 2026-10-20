from pathlib import Path


def test_buoy_metadata_defaults_to_repository_data(monkeypatch):
    from flask_app.app import fetcher_ndbc_buoy

    monkeypatch.delenv("RDS_DATA_FOLDER", raising=False)
    repository_root = Path(__file__).resolve().parents[1]

    assert fetcher_ndbc_buoy._metadata_path() == (
        repository_root / "data" / "reference" / "ndbc_station_metadata_full.csv"
    )


def test_buoy_metadata_honors_configured_data_folder(monkeypatch, tmp_path):
    from flask_app.app import fetcher_ndbc_buoy

    monkeypatch.setenv("RDS_DATA_FOLDER", str(tmp_path))

    assert fetcher_ndbc_buoy._metadata_path() == (
        tmp_path / "reference" / "ndbc_station_metadata_full.csv"
    )


def test_pipeline_sample_path_is_repository_relative():
    from flask_app.app import pipeline_controller

    sample_path = pipeline_controller._default_sample_message_path()

    assert sample_path.name == "sample_sarsat_message.txt"
    assert sample_path.is_file()
