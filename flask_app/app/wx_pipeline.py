from app.setup_imports import *
from datetime import datetime, timezone, timedelta
from .wx_obs import build_wx_obs_for_area
from rds_engine.context import classify_zone_context
from rds_engine.policy_wx import make_wx_data_plan
import os
import pandas as pd
from app.utils_time import now_utc

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

    # Decide include_marine via rds_engine (global for this call)
    wx_mode = os.getenv("RDS_WX_POLICY_MODE", "auto")
    _include_marine = include_marine  # default from function arg
    _radius_km = radius_km
    _max_stations = max_stations

    if pts:
        lat0, lon0 = pts[0]
        _ctx = classify_zone_context(lat=lat0, lon=lon0, error_radius_km=None, coast_distance_km=None)
        _plan = make_wx_data_plan(_ctx, mode=wx_mode)
        _include_marine = _plan.include_marine if include_marine is None else include_marine
        _radius_km = radius_km if radius_km is not None else _plan.spatial_radius_km
        _max_stations = max_stations if max_stations is not None else _plan.station_limit
        LOG.info("wx policy (global): include_marine=%s radius_km=%.1f stations=%s rationale=%s",
                 _include_marine, _radius_km, _max_stations, _plan.rationale)
        LOG.info("wx policy rationale: %s", _plan.rationale)

    df_wx_obs = build_wx_obs_for_area(
        ref_points=pts,
        start_utc=start_ts,
        end_utc=end_ts,
        radius_km=_radius_km,
        max_stations=_max_stations,
        include_marine=_include_marine,
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
      WX_SMOKE_ERR_KM (optional float)
      RDS_LOG_LEVEL (default "INFO")
    """
    # Logging level
    log_level = os.getenv("RDS_LOG_LEVEL", "INFO").upper()
    try:
        logging.getLogger().setLevel(getattr(logging, log_level))
    except Exception:
        logging.getLogger().setLevel(logging.INFO)

    LOG.info("wx_pipeline smoke start")

    try:
        # Read env
        lat = float(os.getenv("WX_SMOKE_LAT", "37.7749"))
        lon = float(os.getenv("WX_SMOKE_LON", "-122.4194"))
        radius_km = float(os.getenv("WX_SMOKE_RADIUS_KM", "25"))
        max_stations = int(os.getenv("WX_SMOKE_MAX_STATIONS", "5"))
        include_marine = os.getenv("WX_SMOKE_INCLUDE_MARINE", "1").strip().lower() not in ("0","false","no")

        hours_back = int(os.getenv("WX_SMOKE_HOURS", "6"))
        end_ts = now_utc().replace(minute=0, second=0, microsecond=0)
        start_ts = end_ts - pd.Timedelta(hours=hours_back)

        err_env = os.getenv("WX_SMOKE_ERR_KM")
        err_km = float(err_env) if err_env not in (None, "", "None") else None

        pts = [{"site_id": "SMOKE", "target": "A", "lat": lat, "lon": lon, **({"error_radius_km": err_km} if err_km is not None else {})}]

        # Compute policy from first point
        from rds_engine.context import classify_zone_context
        from rds_engine.policy_wx import make_wx_data_plan
        wx_mode = os.getenv("RDS_WX_POLICY_MODE", "auto")
        _ctx = classify_zone_context(lat=lat, lon=lon, error_radius_km=err_km, coast_distance_km=None)
        _plan = make_wx_data_plan(_ctx, mode=wx_mode)
        _include_marine = _plan.include_marine
        _radius_km = radius_km if radius_km is not None else _plan.spatial_radius_km
        _max_stations = max_stations if max_stations is not None else _plan.station_limit
        LOG.info("wx policy (global): include_marine=%s radius_km=%s stations=%s rationale=%s",
                 _include_marine, _radius_km, _max_stations, _plan.rationale)
        LOG.info("Params: hours_back=%s radius_km=%s max_stations=%s include_marine=%s",
                 hours_back, _radius_km, _max_stations, _include_marine)

        df_wx_obs = build_wx_obs_for_area(
            ref_points=pts,
            start_utc=start_ts,
            end_utc=end_ts,
            radius_km=_radius_km,
            max_stations=_max_stations,
            include_marine=_include_marine,
        )

        n = 0 if df_wx_obs is None else int(getattr(df_wx_obs, "__len__", lambda: 0)())
        if n == 0:
            LOG.warning("Empty wx_obs (no rows). Check provider availability, params, or environment.")
        else:
            LOG.info("wx_obs rows=%s", n)
            try:
                print(df_wx_obs.head(10).to_string(index=False))
            except Exception:
                try:
                    print(df_wx_obs.head(10))
                except Exception:
                    pass
            try:
                LOG.info("By provider:\n%s", df_wx_obs.groupby("provider").size().to_string())
            except Exception:
                pass
            try:
                LOG.info("Time span: %s â†’ %s",
                         df_wx_obs["valid_utc"].min(), df_wx_obs["valid_utc"].max())
            except Exception:
                pass

        # Optional map output
        if os.getenv("WX_SMOKE_SAVE_MAP") == "1" and n > 0:
            try:
                import folium
                import os as _os

                # Center map on first point
                center_lat, center_lon = df_wx_obs.iloc[0]["lat"], df_wx_obs.iloc[0]["lon"]
                fmap = folium.Map(location=[center_lat, center_lon], zoom_start=8)

                for _, row in df_wx_obs.iterrows():
                    lat, lon = row["lat"], row["lon"]
                    provider = row.get("provider", "")
                    valid_utc = row.get("valid_utc", "")
                    wind_ms = row.get("wind_ms", "")
                    wave_height_m = row.get("wave_height_m", float("nan"))
                    wave_period_s = row.get("wave_period_s", float("nan"))

                    popup = f"Provider: {provider}<br>UTC: {valid_utc}<br>Wind (m/s): {wind_ms}"
                    if not pd.isna(wave_height_m):
                        popup += f"<br>Wave Height (m): {wave_height_m}"
                    if not pd.isna(wave_period_s):
                        popup += f"<br>Wave Period (s): {wave_period_s}"

                    folium.Marker(
                        location=[lat, lon],
                        popup=folium.Popup(popup, max_width=300)
                    ).add_to(fmap)

                # Ensure out/ folder exists
                _os.makedirs("out", exist_ok=True)
                fmap.save("out/wx_map.html")
                LOG.info("Saved wx_map.html with %d markers.", n)
            except Exception as e:
                LOG.warning("Map save failed: %s", e)

    except Exception as e:
        LOG.exception("Smoke run failed: %s", e)

    LOG.info("wx_pipeline smoke done")

