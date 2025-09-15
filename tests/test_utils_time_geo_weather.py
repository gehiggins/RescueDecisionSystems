# tests/test_utils_time_geo_weather.py
import pandas as pd
import numpy as np
import math

# Ensure project paths are configured (setup_imports mutates sys.path)
import app.setup_imports  # noqa: F401

from app.utils_time import (
    ensure_utc, ensure_utc_index, window_slice, coerce_utc_range, now_utc
)
from app.utils_geo import haversine_km, haversine_nm
from app.utils_weather import dewpoint_magnus_c


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
                      index=pd.date_range("2025-01-01", periods=4, freq="H", tz="UTC"))
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


def test_now_utc_monotonic():
    t1 = now_utc(); t2 = now_utc()
    assert t2 >= t1

