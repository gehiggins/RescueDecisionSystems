# ms_fetch_probe.py
# Purpose: Isolate wx_fetch_meteostat.fetch_meteostat_obs_near() by patching its Hourly class
# to return crafted DataFrames (naive vs tz-aware). No network, no Open-Meteo, no pipeline.

import types
import pandas as pd
import numpy as np

# Import the module under test
from flask_app.app import wx_fetch_meteostat as mod

print("[probe] Using wx_fetch_meteostat from:", mod.__file__)

# --- Build two deterministic fake Hourly classes ---------------------------------
class FakeHourlyNaive:
    """Returns a DataFrame with a *naive* DatetimeIndex in the requested window."""
    def __init__(self, sid, start, end):
        self.sid, self.start, self.end = sid, start, end
    def fetch(self):
        # create 3 within-window hours around 'end'
        base = pd.Timestamp(self.end).replace(minute=0, second=0, microsecond=0)
        idx = [base - pd.Timedelta(hours=h) for h in [2,1,0]]  # naive
        df = pd.DataFrame(
            {"temp": [20.1, 19.4, 18.7], "dwpt": [15.0, 14.8, 14.5], "rhum": [75, 77, 79],
             "wspd": [5.0, 6.0, 7.0], "wdir": [270, 275, 280], "pres": [1014,1013,1013], "prcp":[0.0,0.0,0.0]},
            index=pd.DatetimeIndex(idx)  # NAIVE on purpose
        )
        return df

class FakeHourlyAware:
    """Returns a DataFrame with a *tz-aware (UTC)* DatetimeIndex in the requested window."""
    def __init__(self, sid, start, end):
        self.sid, self.start, self.end = sid, start, end
    def fetch(self):
        base = pd.Timestamp(self.end, tz="UTC").replace(minute=0, second=0, microsecond=0)
        idx = [base - pd.Timedelta(hours=h) for h in [2,1,0]]  # tz-aware UTC
        df = pd.DataFrame(
            {"temp": [21.2, 20.3, 19.1], "dwpt": [16.0, 15.5, 15.2], "rhum": [70, 72, 74],
             "wspd": [4.0, 5.0, 5.5], "wdir": [260, 265, 270], "pres": [1015,1014,1014], "prcp":[0.0,0.0,0.0]},
            index=pd.DatetimeIndex(idx)
        )
        return df

def run_one(fake_hourly_cls, label):
    print(f"\n[probe] Scenario: {label}")
    # Patch the imported Hourly symbol inside the module under test
    orig_hourly = mod.Hourly
    try:
        mod.Hourly = fake_hourly_cls  # monkeypatch
        # Inputs (fixed): downtown SF-ish, short window around now
        lat, lon = 37.7749, -122.4194
        end_utc = pd.Timestamp.utcnow().tz_localize("UTC")
        start_utc = end_utc - pd.Timedelta(hours=6)
        df = mod.fetch_meteostat_obs_near(lat, lon, start_utc, end_utc, radius_km=25.0, max_stations=1)
        print("[probe] Result rows:", len(df))
        if not df.empty:
            # Show key columns only for brevity
            print(df[["provider","source_id","valid_utc","temp_c","wind_ms","pressure_hpa"]].head(3).to_string(index=False))
        else:
            print("[probe] EMPTY result")
    except Exception as e:
        print("[probe] EXCEPTION:", repr(e))
    finally:
        mod.Hourly = orig_hourly

if __name__ == "__main__":
    # Replace station discovery with a single fake station right at the target point to eliminate distance math.
    # We’ll stub mod.Stations().nearby(...).fetch(...) to return one row with the fields your code expects.
    class FakeStations:
        def nearby(self, lat, lon):
            return self
        def fetch(self, n):
            return pd.DataFrame(
                [{
                    "id": "FAKE123",
                    "name": "Fake Station",
                    "country": "US",
                    "region": "CA",
                    "latitude": 37.7749,
                    "longitude": -122.4194,
                    "elevation": 10.0,
                    "timezone": "UTC"
                }],
            ).set_index("id")

    # Monkeypatch Stations inside the module under test
    mod.Stations = FakeStations

    # Run both scenarios
    run_one(FakeHourlyNaive, "Naive index returned by Hourly.fetch (should NOT error)")
    run_one(FakeHourlyAware, "UTC-aware index returned by Hourly.fetch (should NOT error)")
