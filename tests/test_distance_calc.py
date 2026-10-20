from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import LineString

from flask_app.app import distance_calc


def test_default_coastline_path_is_repository_relative():
    expected = (
        Path(__file__).resolve().parents[1]
        / "data"
        / "shapefiles"
        / "coastline"
        / "ne_10m_coastline.shp"
    )

    assert distance_calc.DEFAULT_COASTLINE_PATH == expected
    assert distance_calc.DEFAULT_COASTLINE_PATH.is_file()


@pytest.mark.parametrize(
    "latitude, longitude",
    [("bad", 0), (0, None), (float("nan"), 0), (91, 0), (0, 181)],
)
def test_invalid_coordinates_are_rejected_before_reading_file(
    monkeypatch, tmp_path, latitude, longitude
):
    monkeypatch.setattr(
        distance_calc.gpd,
        "read_file",
        lambda *_args, **_kwargs: pytest.fail("provider file should not be read"),
    )

    with pytest.raises(ValueError):
        distance_calc.compute_distance_to_shore(
            latitude, longitude, coastline_path=tmp_path / "coastline.shp"
        )


def test_distance_is_calculated_in_projected_metres(monkeypatch, tmp_path):
    placeholder = tmp_path / "coastline.shp"
    placeholder.touch()
    coastline = gpd.GeoDataFrame(
        geometry=[LineString([(0.0, -1.0), (0.0, 1.0)])],
        crs="EPSG:4326",
    )
    monkeypatch.setattr(distance_calc.gpd, "read_file", lambda _path: coastline)

    distance_km = distance_calc.compute_distance_to_shore(
        0.0, 1.0, coastline_path=placeholder
    )

    assert distance_km == pytest.approx(111.3, rel=0.02)


def test_missing_coastline_file_has_clear_error(tmp_path):
    missing = tmp_path / "missing.shp"

    with pytest.raises(FileNotFoundError, match="Coastline shapefile not found"):
        distance_calc.compute_distance_to_shore(0, 0, coastline_path=missing)
