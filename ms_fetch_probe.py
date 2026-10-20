# ms_fetch_probe.py
# Purpose: Isolate wx_fetch_meteostat.fetch_meteostat_obs_near() by patching its Hourly class
# to return crafted DataFrames (naive vs tz-aware). No network, no Open-Meteo, no pipeline.

import pandas as pd

# Import the module under test
from flask_app.app import wx_fetch_meteostat as mod

# --- Build two deterministic fake Hourly classes ---------------------------------
class FakeHourlyNaive:
    """Returns a DataFrame with a *naive* DatetimeIndex in the requested window."""
    def __init__(self, sid, start, end):
        self.sid, self.start, self.end = sid, start, end
    def fetch(self):
        # create 3 within-window hours around 'end'
        base = pd.Timestamp(self.end).tz_localize(None).replace(
            minute=0, second=0, microsecond=0
        )
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
        base = pd.to_datetime(self.end, utc=True).replace(
            minute=0, second=0, microsecond=0
        )
        idx = [base - pd.Timedelta(hours=h) for h in [2,1,0]]  # tz-aware UTC
        df = pd.DataFrame(
            {"temp": [21.2, 20.3, 19.1], "dwpt": [16.0, 15.5, 15.2], "rhum": [70, 72, 74],
             "wspd": [4.0, 5.0, 5.5], "wdir": [260, 265, 270], "pres": [1015,1014,1014], "prcp":[0.0,0.0,0.0]},
            index=pd.DatetimeIndex(idx)
        )
        return df

def run_one(fake_hourly_cls, label):
    print(f"\n[probe] Scenario: {label}")
    # Meteostat is an optional dependency, so the production module may not
    # define Hourly at all. This probe supplies its own deterministic provider.
    missing = object()
    orig_hourly = getattr(mod, "Hourly", missing)
    orig_has_meteostat = mod._HAS_METEOSTAT
    try:
        mod.Hourly = fake_hourly_cls
        mod._HAS_METEOSTAT = True
        # Inputs (fixed): downtown SF-ish, short window around now
        lat, lon = 37.7749, -122.4194
        end_utc = pd.Timestamp.now(tz="UTC")
        start_utc = end_utc - pd.Timedelta(hours=6)
        df = mod.fetch_meteostat_obs_near(lat, lon, start_utc, end_utc, radius_km=25.0, max_stations=1)
        print("[probe] Result rows:", len(df))
        if df.empty:
            raise RuntimeError(f"Meteostat probe returned no rows for {label}")
        # Show key columns only for brevity
        print(df[["provider","source_id","valid_utc","temp_c","wind_ms","pressure_hpa"]].head(3).to_string(index=False))
        return df
    finally:
        mod._HAS_METEOSTAT = orig_has_meteostat
        if orig_hourly is missing:
            delattr(mod, "Hourly")
        else:
            mod.Hourly = orig_hourly

def main():
    print("[probe] Using wx_fetch_meteostat from:", mod.__file__)
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

    missing = object()
    original_stations = getattr(mod, "Stations", missing)
    try:
        mod.Stations = FakeStations
        run_one(FakeHourlyNaive, "Naive index returned by Hourly.fetch")
        run_one(FakeHourlyAware, "UTC-aware index returned by Hourly.fetch")
    finally:
        if original_stations is missing:
            delattr(mod, "Stations")
        else:
            mod.Stations = original_stations
    print("Meteostat timezone probe passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
