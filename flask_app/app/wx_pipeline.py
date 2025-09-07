
from flask_app.setup_imports import *
from datetime import datetime, timezone, timedelta
from .wx_obs import build_wx_obs_for_area

LOG = logging.getLogger(__name__)

def run_wx_pipeline(alert_targets_df: pd.DataFrame,
                    hours_back: int = 6,
                    radius_km: float = 50.0,
                    max_stations: int = 5,
                    include_marine: bool = True) -> pd.DataFrame:
    """
    alert_targets_df columns expected: site_id, target, lat, lon  (A/B live elsewhere)
    """
    if alert_targets_df is None or alert_targets_df.empty:
        LOG.warning("No alert targets provided; wx pipeline skipped.")
        return pd.DataFrame()

    # Unique A/B coordinates for the case
    pts = []
    for _, r in alert_targets_df.iterrows():
        if pd.notna(r.get("lat")) and pd.notna(r.get("lon")):
            pts.append((float(r["lat"]), float(r["lon"])))
    pts = list(dict.fromkeys(pts))  # unique order-preserving

    end_ts = datetime.now(timezone.utc)
    start_ts = end_ts - timedelta(hours=hours_back)

    df_wx_obs = build_wx_obs_for_area(
        ref_points=pts,
        start_utc=start_ts,
        end_utc=end_ts,
        radius_km=radius_km,
        max_stations=max_stations,
        include_marine=include_marine,
    )
    return df_wx_obs






#Wx pipeline smoke test runner
if __name__ == "__main__":
    """
    Canonical smoke run for wx_pipeline.
    - Builds a tiny alert_targets_df from env vars
    - Calls run_wx_pipeline(...)
    - Prints a preview + provider breakdown + time span
    - Emits a single 'Empty wx_obs' warning ONLY if no rows
    Env:
      WX_SMOKE_LAT / WX_SMOKE_LON
      WX_SMOKE_LAT_B / WX_SMOKE_LON_B (optional)
      WX_SMOKE_HOURS (default 6)
      WX_SMOKE_RADIUS_KM (default 25)
      WX_SMOKE_MAX_STATIONS (default 5)
      WX_SMOKE_INCLUDE_MARINE ("1"/"0", default "1")
      RDS_LOG_LEVEL (default "INFO")
    """
    import os
    import pandas as pd

    # Logging level
    log_level = os.getenv("RDS_LOG_LEVEL", "INFO").upper()
    try:
        logging.getLogger().setLevel(getattr(logging, log_level))
    except Exception:
        logging.getLogger().setLevel(logging.INFO)

    LOG.info("wx_pipeline smoke start")

    try:
        # Read env
        lat_a = float(os.getenv("WX_SMOKE_LAT", "37.7749"))
        lon_a = float(os.getenv("WX_SMOKE_LON", "-122.4194"))
        lat_b = os.getenv("WX_SMOKE_LAT_B")
        lon_b = os.getenv("WX_SMOKE_LON_B")

        hours_back = int(os.getenv("WX_SMOKE_HOURS", "6"))
        radius_km = float(os.getenv("WX_SMOKE_RADIUS_KM", "25"))
        max_stations = int(os.getenv("WX_SMOKE_MAX_STATIONS", "5"))
        include_marine = os.getenv("WX_SMOKE_INCLUDE_MARINE", "1").strip().lower() not in ("0","false","no")

        # Targets DF
        rows = [{"site_id": "SMOKE", "target": "A", "lat": lat_a, "lon": lon_a}]
        if lat_b is not None and lon_b is not None:
            rows.append({"site_id": "SMOKE", "target": "B", "lat": float(lat_b), "lon": float(lon_b)})
        alert_targets_df = pd.DataFrame(rows, columns=["site_id","target","lat","lon"])

        LOG.info("Targets: %s", alert_targets_df.to_dict(orient="records"))
        LOG.info("Params: hours_back=%s radius_km=%.1f max_stations=%s include_marine=%s",
                 hours_back, radius_km, max_stations, include_marine)

        # Run
        df = run_wx_pipeline(
            alert_targets_df=alert_targets_df,
            hours_back=hours_back,
            radius_km=radius_km,
            max_stations=max_stations,
            include_marine=include_marine,
        )

        # Report (single source of truth)
        n = 0 if df is None else int(getattr(df, "__len__", lambda: 0)())
        if n == 0:
            LOG.warning("Empty wx_obs (no rows). Check provider availability, params, or environment.")
        else:
            LOG.info("wx_obs rows=%s", n)
            try:
                print(df.head(10).to_string(index=False))
            except Exception:
                try:
                    print(df.head(10))
                except Exception:
                    pass
            try:
                LOG.info("By provider:\n%s", df.groupby("provider").size().to_string())
            except Exception:
                pass
            try:
                LOG.info("Time span: %s → %s",
                         df["valid_utc"].min(), df["valid_utc"].max())
            except Exception:
                pass
    except Exception as e:
        LOG.exception("Smoke run failed: %s", e)

    LOG.info("wx_pipeline smoke done")
