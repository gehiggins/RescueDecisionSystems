# tests/test_utils_time_geo_weather.py
import pandas as pd
import numpy as np
import math

# Ensure project paths are configured (setup_imports mutates sys.path)
import app.setup_imports  # noqa: F401

from app.utils_time import (
    ensure_utc, ensure_utc_index, window_slice, coerce_utc_range, now_utc,
    clamp_index_to_range,
)
from app.utils_geo import haversine_km, haversine_nm
from app.utils_weather import (
    celsius_to_fahrenheit,
    dewpoint_magnus_c,
    meters_per_second_to_knots,
)


def test_ensure_utc_and_index():
    t1 = ensure_utc("2025-01-01T12:34:56Z")
    assert t1 is not None and str(t1.tz) == "UTC"

    idx = pd.Index(["2025-01-01 00:00", "2025-01-01 01:00"])
    didx = ensure_utc_index(idx)
    assert isinstance(didx, pd.DatetimeIndex)
    assert str(didx.tz) == "UTC"


def test_window_slice_and_range():
    s, e = coerce_utc_range("2025-01-01 00:00Z", "2025-01-01 03:00Z")
    df = pd.DataFrame({"x": [1, 2, 3, 4]},
                      index=pd.date_range("2025-01-01", periods=4, freq="h", tz="UTC"))
    out = window_slice(df, s, e)
    assert len(out) == 4  # inclusive window


def test_haversine_km_nm_close_enough():
    # SF Ferry Building -> Golden Gate Bridge (rough checks)
    sf = (37.7955, -122.3937)
    ggb = (37.8199, -122.4783)
    km = haversine_km(*sf, *ggb)
    nm = haversine_nm(*sf, *ggb)
    assert 7 <= km <= 12
    assert 4 <= nm <= 7


def test_dewpoint_magnus_c_reasonable():
    # ~9.3C for 20C @ 50% RH (allow tolerance)
    dp = dewpoint_magnus_c(20.0, 50.0)
    assert abs(dp - 9.3) < 1.0


def test_weather_numeric_helpers_reject_malformed_and_nonfinite_values():
    invalid_values = (None, "bad", np.inf, [20.0])

    for value in invalid_values:
        assert np.isnan(dewpoint_magnus_c(value, 50.0))
        assert np.isnan(dewpoint_magnus_c(20.0, value))
        assert np.isnan(celsius_to_fahrenheit(value))
        assert np.isnan(meters_per_second_to_knots(value))


def test_weather_numeric_helpers_accept_numeric_strings():
    assert abs(dewpoint_magnus_c("20", "50") - 9.3) < 1.0
    assert celsius_to_fahrenheit("20") == 68.0
    assert meters_per_second_to_knots("10") == 19.4384


def test_now_utc_monotonic():
    t1 = now_utc(); t2 = now_utc()
    assert t2 >= t1


def test_scalar_utc_helper_rejects_array_like_input():
    assert ensure_utc(["2025-01-01", "2025-01-02"]) is None


def test_utc_index_accepts_mixed_offsets_and_invalid_values():
    idx = pd.Index(
        [
            "2025-01-01T00:00:00-08:00",
            "2025-07-01T00:00:00-07:00",
            "invalid",
        ]
    )

    result = ensure_utc_index(idx)

    assert str(result.tz) == "UTC"
    assert result[0] == pd.Timestamp("2025-01-01T08:00:00Z")
    assert result[1] == pd.Timestamp("2025-07-01T07:00:00Z")
    assert pd.isna(result[2])


def test_window_slice_normalizes_naive_boundaries():
    frame = pd.DataFrame(
        {"value": [1, 2, 3]},
        index=pd.date_range("2025-01-01", periods=3, freq="h", tz="UTC"),
    )

    result = window_slice(frame, "2025-01-01 01:00", "2025-01-01 02:00")

    assert result["value"].tolist() == [2, 3]


def test_invalid_window_boundaries_are_treated_as_open():
    frame = pd.DataFrame(
        {"value": [1, 2]},
        index=pd.date_range("2025-01-01", periods=2, freq="h", tz="UTC"),
    )

    result = window_slice(frame, "invalid", ["not", "scalar"])

    assert result["value"].tolist() == [1, 2]


def test_clamp_index_normalizes_naive_boundaries():
    idx = pd.date_range("2025-01-01", periods=4, freq="h", tz="UTC")

    result = clamp_index_to_range(
        idx,
        "2025-01-01 01:00",
        "2025-01-01 02:00",
    )

    assert result.tolist() == idx[1:3].tolist()

