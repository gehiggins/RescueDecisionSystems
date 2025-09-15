# tests/smoke/test_wx_providers_smoke.py
import pytest
import pandas as pd

# Ensure project paths are configured
import app.setup_imports  # noqa: F401

from app.utils_time import now_utc
from app.wx_fetch_open_meteo import fetch_open_meteo_obs

# Meteostat is optional in some envs; skip gracefully if missing
try:
    from app.wx_fetch_meteostat import fetch_meteostat_obs_near
    HAS_METEOSTAT = True
except Exception:
    HAS_METEOSTAT = False


@pytest.mark.live
def test_open_meteo_live_smoke():
    lat, lon = 37.7749, -122.4194
    end = now_utc().replace(minute=0, second=0, microsecond=0)
    start = end - pd.Timedelta(hours=6)
    df = fetch_open_meteo_obs(lat, lon, start, end, include_marine=True)
    assert df is not None
    # Itâ€™s okay if occasionally empty; assert schema presence instead
    for col in ["valid_utc", "lat", "lon", "provider", "source_type"]:
        assert col in df.columns


@pytest.mark.live
@pytest.mark.skipif(not HAS_METEOSTAT, reason="Meteostat not installed/available")
def test_meteostat_live_smoke():
    lat, lon = 37.7749, -122.4194
    end = now_utc().replace(minute=0, second=0, microsecond=0)
    start = end - pd.Timedelta(hours=6)
    df = fetch_meteostat_obs_near(lat, lon, start, end, radius_km=50, max_stations=3)
    assert df is not None
    # Same schema checks
    for col in ["valid_utc", "lat", "lon", "provider", "source_type"]:
        assert col in df.columns

