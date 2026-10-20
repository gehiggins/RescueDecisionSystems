"""Regression coverage for developer-facing smoke scripts."""

from pathlib import Path
import subprocess
import sys


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def _run_script(*arguments):
    return subprocess.run(
        [sys.executable, *arguments],
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )


def test_parser_smoke_uses_active_parser_and_committed_sample():
    result = _run_script("test_parser_run.py")

    assert result.returncode == 0, result.stderr
    assert "2DCE6 12345 ABCFF" in result.stdout
    assert "98372" in result.stdout
    assert "37.76" in result.stdout


def test_satellite_smoke_defaults_to_offline_contract_check():
    result = _run_script("run_sat_smoke.py")

    assert result.returncode == 0, result.stderr
    assert "Satellite offline contract smoke passed" in result.stdout


def test_gis_smoke_generates_html_without_configuration(tmp_path):
    output = tmp_path / "gis-smoke.html"

    result = _run_script("test_gis_plot.py", "--output", str(output))

    assert result.returncode == 0, result.stderr
    assert "GIS map smoke passed" in result.stdout
    assert output.is_file()
    assert "leaflet" in output.read_text(encoding="utf-8").lower()


def test_weather_smoke_defaults_to_offline_contract_check():
    result = _run_script("test_weather_fetch.py")

    assert result.returncode == 0, result.stderr
    assert "Weather offline contract smoke passed" in result.stdout


def test_tle_smoke_defaults_to_offline_reference_check():
    result = _run_script("_tle_smoketest.py")

    assert result.returncode == 0, result.stderr
    assert "TLE offline contract smoke passed" in result.stdout


def test_satellite_gis_smoke_generates_map_without_network(tmp_path):
    output = tmp_path / "satellite-gis-smoke.html"

    result = _run_script("_sat_gis_smoketest.py", "--output", str(output))

    assert result.returncode == 0, result.stderr
    assert "Satellite GIS offline contract smoke passed" in result.stdout
    assert output.is_file()
    assert "leaflet" in output.read_text(encoding="utf-8").lower()


def test_offshore_weather_smoke_defaults_to_offline_contract_check():
    result = _run_script("test_fetch_offshore_buoys.py")

    assert result.returncode == 0, result.stderr
    assert "Offshore weather offline contract smoke passed" in result.stdout


def test_noaa_shore_diagnostic_defaults_to_offline_contract_check():
    result = _run_script("flask_app/test_noaa_api.py")

    assert result.returncode == 0, result.stderr
    assert "NOAA shore diagnostic offline contract passed" in result.stdout


def test_meteostat_timezone_probe_runs_without_optional_dependency():
    result = _run_script("ms_fetch_probe.py")

    assert result.returncode == 0, result.stderr
    assert "Meteostat timezone probe passed" in result.stdout
    assert "Naive index" in result.stdout
    assert "UTC-aware index" in result.stdout
