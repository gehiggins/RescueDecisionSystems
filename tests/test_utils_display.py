import pytest
import numpy as np
from app.utils_display import (
    derive_local_tz,
    to_dual_time,
    m_to_ft,
    ms_to_kt,
    c_to_f,
    format_us_display,
    is_maritime,
)
from datetime import datetime

def test_derive_local_tz_respects_env():
    assert derive_local_tz(0, 0, op_tz_env="America/New_York") == "America/New_York"
    assert derive_local_tz(0, 0, op_tz_env="") == "UTC"

def test_derive_local_tz_fallback(monkeypatch):
    # Simulate timezonefinder not installed
    monkeypatch.setitem(__import__('sys').modules, "timezonefinder", None)
    assert derive_local_tz(37.77, -122.42) == "UTC"

def test_to_dual_time_str_and_datetime():
    ts_str = "2025-09-15T12:00:00Z"
    ts_dt = datetime(2025, 9, 15, 12, 0, 0)
    utc_iso, local_iso = to_dual_time(ts_str, "UTC")
    assert utc_iso == local_iso
    utc_iso2, local_iso2 = to_dual_time(ts_dt, "UTC")
    assert utc_iso2 == local_iso2

def test_m_to_ft_ms_to_kt_c_to_f():
    assert m_to_ft(1.0) == pytest.approx(3.28084)
    assert m_to_ft(None) is None
    assert m_to_ft(np.nan) is None

    assert ms_to_kt(1.0) == pytest.approx(1.94384)
    assert ms_to_kt(None) is None
    assert ms_to_kt(np.nan) is None

    assert c_to_f(0.0) == pytest.approx(32.0)
    assert c_to_f(100.0) == pytest.approx(212.0)
    assert c_to_f(None) is None
    assert c_to_f(np.nan) is None

def test_format_us_display_wave_wind_temp():
    out = format_us_display(wave_height_m=2.0, wind_ms=5.0, temp_C=20.0)
    assert "wave_height_display" in out and out["wave_height_display"].endswith("ft")
    assert "wind_display" in out and out["wind_display"].endswith("kt")
    assert "temp_display" in out and "°F" in out["temp_display"] and "°C" in out["temp_display"]

def test_format_us_display_none_inputs():
    out = format_us_display()
    assert out == {}
    out2 = format_us_display(wave_height_m=None, wind_ms=None, temp_C=None)
    assert out2 == {}

def test_is_maritime_stub():
    assert is_maritime(37.77, -122.42) is False
    assert isinstance(is_maritime(0, 0), bool)