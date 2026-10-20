from app.setup_imports import *
from datetime import datetime, timezone, timedelta
from .wx_obs import (
    build_wx_obs_for_area,
    _empty_wx_obs,
    _normalize_search_parameters,
)
from rds_engine.context import classify_zone_context
from rds_engine.policy_wx import make_wx_data_plan
import os
import math
import pandas as pd
from app.utils_time import now_utc

LOG = logging.getLogger(__name__)

_DEFAULT_HOURS_BACK = 6.0


def _normalize_hours_back(hours_back):
    try:
        normalized = float(hours_back)
    except (TypeError, ValueError):
        normalized = float("nan")
    if not math.isfinite(normalized) or normalized <= 0:
        LOG.warning(
            "Invalid weather hours_back=%r; using default %.1f",
            hours_back,
            _DEFAULT_HOURS_BACK,
        )
        return _DEFAULT_HOURS_BACK
    return normalized

def _optional_distance_km(value):
    """Return finite non-negative point metadata, or ``None`` when unusable."""
    if value is None or isinstance(value, bool):
        return None
    try:
        normalized = float(value)
    except (TypeError, ValueError):
        return None
    return normalized if math.isfinite(normalized) and normalized >= 0 else None


def run_wx_pipeline(alert_targets_df: pd.DataFrame,
                    hours_back: int = 6,
                    radius_km: float = 50.0,
                    max_stations: int = 5,
                    include_marine: bool = True) -> pd.DataFrame:
    """
    alert_targets_df columns expected: site_id, target, lat, lon  (A/B live elsewhere)
    """
    if not isinstance(alert_targets_df, pd.DataFrame) or alert_targets_df.empty:
        LOG.warning("No alert targets provided; wx pipeline skipped.")
        return _empty_wx_obs()

    # Unique A/B coordinates for the case
    pts = []
    points_by_coordinates = {}
    rejected_points = 0
    for _, r in alert_targets_df.iterrows():
        try:
            lat = float(r.get("lat"))
            lon = float(r.get("lon"))
        except (TypeError, ValueError):
            rejected_points += 1
            continue
        if (
            not math.isfinite(lat)
            or not math.isfinite(lon)
            or not -90.0 <= lat <= 90.0
            or not -180.0 <= lon <= 180.0
        ):
            rejected_points += 1
            continue
        coordinates = (lat, lon)
        point = points_by_coordinates.get(coordinates)
        if point is None:
            point = {"lat": lat, "lon": lon}
            points_by_coordinates[coordinates] = point
            pts.append(point)
        for field in ("error_radius_km", "coast_distance_km"):
            if field in r:
                distance = _optional_distance_km(r.get(field))
                if distance is None:
                    continue
                previous = _optional_distance_km(point.get(field))
                if previous is None:
                    point[field] = distance
                elif field == "error_radius_km":
                    point[field] = max(previous, distance)
                else:
                    point[field] = min(previous, distance)
    if rejected_points:
        LOG.warning("Discarded %s invalid weather target coordinate(s)", rejected_points)
    if not pts:
        LOG.warning("No valid weather target coordinates; wx pipeline skipped.")
        return _empty_wx_obs()

    hours_back = _normalize_hours_back(hours_back)
    radius_km, max_stations = _normalize_search_parameters(
        radius_km, max_stations
    )
    end_ts = datetime.now(timezone.utc)
    start_ts = end_ts - timedelta(hours=hours_back)

    # Preserve ``None`` so the aggregator can apply the existing policy to
    # each A/B point using its own uncertainty and coast-distance metadata.
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

