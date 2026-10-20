import numpy as np
import pandas as pd
import pytest

from flask_app.app.sat_compute_footprint import (
    annotate_footprint_radius,
    horizon_radius_km,
)


@pytest.mark.parametrize("invalid_radius", [-1, 0, np.inf, "bad", None])
def test_invalid_footprint_radius_is_recomputed_from_altitude(invalid_radius):
    source = pd.DataFrame(
        [{"footprint_radius_km": invalid_radius, "altitude_km": 850.0}]
    )

    result = annotate_footprint_radius(source)

    assert result.loc[0, "footprint_radius_km"] == pytest.approx(
        horizon_radius_km(850.0)
    )


def test_valid_numeric_footprint_radius_is_normalized_and_preserved():
    source = pd.DataFrame(
        [{"footprint_radius_km": "1200.5", "altitude_km": 850.0}]
    )

    result = annotate_footprint_radius(source)

    assert result.loc[0, "footprint_radius_km"] == 1200.5


def test_unusable_radius_and_altitude_remain_missing_without_mutating_input():
    source = pd.DataFrame(
        [{"footprint_radius_km": "bad", "altitude_km": "also-bad"}]
    )

    result = annotate_footprint_radius(source)

    assert source.loc[0, "footprint_radius_km"] == "bad"
    assert pd.isna(result.loc[0, "footprint_radius_km"])


def test_footprint_annotation_requires_dataframe():
    with pytest.raises(TypeError, match="pandas DataFrame"):
        annotate_footprint_radius([{"altitude_km": 850.0}])
