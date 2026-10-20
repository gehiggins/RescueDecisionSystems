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
