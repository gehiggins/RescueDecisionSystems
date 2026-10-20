import pytest

from flask_app.app.utils_coordinates import (
    coordinate_pair_to_dd,
    convert_lat_lon_to_decimal,
    is_valid_coordinate,
    parse_any_coordinate,
    to_latlon_polyline,
)


@pytest.mark.parametrize(
    ("coordinate", "expected"),
    [
        ("90 00.000N", 90.0),
        ("90 00.000S", -90.0),
        ("180 00.000E", 180.0),
        ("180 00.000W", -180.0),
    ],
)
def test_coordinate_parser_accepts_geographic_boundaries(coordinate, expected):
    assert parse_any_coordinate(coordinate) == expected
    assert is_valid_coordinate(coordinate) is True


@pytest.mark.parametrize(
    "coordinate",
    [
        "90 00.001N",
        "91 00.000S",
        "180 00.001E",
        "181 00.000W",
    ],
)
def test_coordinate_parser_rejects_values_beyond_geographic_boundaries(
    coordinate,
):
    with pytest.raises(ValueError, match="out of range"):
        parse_any_coordinate(coordinate)
    assert is_valid_coordinate(coordinate) is False


def test_pair_and_conversion_helpers_reject_out_of_range_coordinates():
    assert coordinate_pair_to_dd("91 00.000N 075 30.000W") == (None, None)
    assert convert_lat_lon_to_decimal("91 00.000N", "075 30.000W") == (
        None,
        None,
    )


def test_polyline_conversion_filters_invalid_vertices():
    result = to_latlon_polyline(
        [
            [-75.0, 37.5],
            [-181.0, 37.5],
            [-75.0, 91.0],
            [float("inf"), 37.5],
            ["bad", 37.5],
            [1],
            None,
        ]
    )

    assert result == [[37.5, -75.0]]
