# ============================== RDS STANDARD HEADER ==============================
# Script Name: gis_map_inputs_builder.py
# Last Updated (UTC): 2025-09-15
# Update Summary:
#   - Initial implementation: builds unified GIS map input DataFrame from positions, weather, and stations.
# Description:
#   - Pure transforms to produce a single DataFrame for GIS mapping, including alert positions, range rings, weather, and station layers.
# Data Handling Notes:
#   - No file I/O; all logic is in-memory and stateless. Handles missing columns gracefully.

import math
import logging
from html import escape as html_escape
import pandas as pd
import numpy as np
from typing import Optional
from app.utils_display import format_us_display, to_dual_time, derive_local_tz

LOG = logging.getLogger(__name__)

def _is_missing(v):
    if v is None:
        return True
    try:
        return bool(pd.isna(v))
    except (TypeError, ValueError):
        return False


def _coalesce(*values, default=None):
    for value in values:
        if not _is_missing(value):
            return value
    return default


def _satellite_type(row) -> str:
    return str(
        _coalesce(row.get("sat_type"), row.get("type"), default="")
    ).lower()


def _satellite_name(row, default="Satellite") -> str:
    return str(_coalesce(row.get("sat_name"), default=default))

def _fmt(v, fmt=None, dash="—"):
    if _is_missing(v):
        return dash
    try:
        return format(v, fmt) if fmt else str(v)
    except Exception:
        return dash

def _fmt_num(v, decimals):
    try:
        if _is_missing(v):
            return "—"
        return f"{float(v):.{decimals}f}"
    except Exception:
        return "—"

def _latlon_str(lat, lon):
    return f"Lat: {_fmt_num(lat, 5)}, Lon: {_fmt_num(lon, 5)}"


def _html_text(value, default=""):
    """Escape one external display value while preserving builder markup."""
    value = _coalesce(value, default=default)
    return html_escape(str(value), quote=True)


def _valid_coordinate_pair(lon, lat):
    """Return whether values form a finite coordinate within Earth bounds."""
    try:
        lon_value = float(lon)
        lat_value = float(lat)
    except (TypeError, ValueError):
        return False
    return (
        math.isfinite(lon_value)
        and math.isfinite(lat_value)
        and -180.0 <= lon_value <= 180.0
        and -90.0 <= lat_value <= 90.0
    )


def _positive_finite(value):
    """Return a positive finite geometry value, or ``None`` when unusable."""
    try:
        normalized = float(value)
    except (TypeError, ValueError):
        return None
    return normalized if math.isfinite(normalized) and normalized > 0 else None

def build_gis_map_inputs_df(
    positions_df: pd.DataFrame,
    wx_df: Optional[pd.DataFrame] = None,
    stations_df: Optional[pd.DataFrame] = None,
    op_tz_env: Optional[str] = None,
    shore_nm: float = 5.0,
    sat_overlays: Optional[list] = None,
    alert_time_utc=None,
) -> pd.DataFrame:
    """
    Return a single DataFrame (gis_map_inputs_df) with rows for:
      - alert_positions
      - range_rings (encode as Circle: geometry={'type':'Circle','center':[lon,lat],'radius_m':N})
      - weather (point)
      - stations (point)
    Schema columns (at minimum):
      ['site_id','layer','geom_type','geometry','ts_utc','ts_local','local_tz',
       'label','popup_html','style_hint','source_table','source_id','is_maritime',
       'range_ring_meters','wave_height_m','wind_ms','temp_C',
       'wave_height_display','wind_display','temp_display']
    """
    rows = []

    # Determine center from positions_df (A first, else B)
    center_lat, center_lon = None, None
    site_id = None
    if positions_df is not None and not positions_df.empty:
        for _, r in positions_df.iterrows():
            if _valid_coordinate_pair(r.get("lon_dd"), r.get("lat_dd")):
                center_lat, center_lon = float(r["lat_dd"]), float(r["lon_dd"])
                site_id = r.get("site_id")
                break
    if center_lat is None or center_lon is None:
        LOG.warning("build_gis_map_inputs_df: No valid center found in positions_df; using (0,0)")
        center_lat, center_lon = 0.0, 0.0

    local_tz = derive_local_tz(center_lat, center_lon, op_tz_env)
    maritime_flag = False
    try:
        from app.utils_display import is_maritime  # present stub
    except Exception:
        def is_maritime(lat, lon, shore_nm=5.0):  # safe fallback
            return False
    maritime_flag = is_maritime(center_lat, center_lon, shore_nm)

    # --- Alert positions layer ---
    if positions_df is not None and not positions_df.empty:
        for _, r in positions_df.iterrows():
            lat = r.get("lat_dd")
            lon = r.get("lon_dd")
            label = r.get("role", "Alert")
            ts = r.get("ts_utc", None)
            ts_utc, ts_local = to_dual_time(ts, local_tz) if ts is not None else (None, None)
            popup_fields = [f"<b>{_html_text(label)}</b>", _latlon_str(lat, lon)]
            popup_html = "<br>".join(popup_fields)
            rows.append({
                "site_id": r.get("site_id"),
                "layer": "alert_position",
                "geom_type": "Point",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "ts_utc": ts_utc,
                "ts_local": ts_local,
                "local_tz": local_tz,
                "label": label,
                "popup_html": popup_html,
                "style_hint": {"weight": 3, "opacity": 0.9},
                "source_table": "positions",
                "source_id": _coalesce(r.get("site_id"), default=""),
                "is_maritime": maritime_flag,
                "range_ring_meters": None,
                "wave_height_m": None,
                "wind_ms": None,
                "temp_C": None,
                "wave_height_display": "None",
                "wind_display": "None",
                "temp_display": "None",
            })

    # --- Range rings layer (if present) ---
    if positions_df is not None and not positions_df.empty and "range_ring_meters" in positions_df.columns:
        for _, r in positions_df.iterrows():
            ring_m = r.get("range_ring_meters")
            lat = r.get("lat_dd")
            lon = r.get("lon_dd")
            try:
                ring_m = float(ring_m)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(ring_m) or ring_m <= 0:
                continue
            label = f"Range Ring {_fmt_num(ring_m, 0)} m"
            radius_line = f"Radius: {int(ring_m)} m" if ring_m is not None else "Radius: —"
            popup_html = "<br>".join([f"<b>{label}</b>", _latlon_str(lat, lon), radius_line])
            rows.append({
                "site_id": r.get("site_id"),
                "layer": "range_ring",
                "geom_type": "Circle",
                "geometry": {"type": "Circle", "center": [lon, lat], "radius_m": ring_m},
                "ts_utc": None,
                "ts_local": None,
                "local_tz": local_tz,
                "label": label,
                "popup_html": popup_html,
                "style_hint": {"weight": 2, "opacity": 0.5, "dash": "2,6"},
                "source_table": "positions",
                "source_id": _coalesce(r.get("site_id"), default=""),
                "is_maritime": maritime_flag,
                "range_ring_meters": ring_m,
                "wave_height_m": None,
                "wind_ms": None,
                "temp_C": None,
                "wave_height_display": "None",
                "wind_display": "None",
                "temp_display": "None",
            })

    # --- Weather layer ---
    if wx_df is not None and not wx_df.empty:
        for _, w in wx_df.iterrows():
            lat = w.get("lat_dd")
            lon = w.get("lon_dd")
            ts = w.get("obs_time", None)
            ts_utc, ts_local = to_dual_time(ts, local_tz) if ts is not None else (None, None)
            wave_m = w.get("wave_height_m")
            wind_ms = w.get("wind_ms")
            temp_C = w.get("temp_C")
            # Weather observations may arrive in long form (obs_type/value) or
            # wide form (one measurement per column); normalize both here.
            obs_type = str(_coalesce(w.get("obs_type"), default=""))
            obs_value = w.get("obs_value")
            if _is_missing(wave_m) and obs_type == "wave_height_m":
                wave_m = obs_value
            if _is_missing(wind_ms) and obs_type == "wind_ms":
                wind_ms = obs_value
            if _is_missing(temp_C) and obs_type in {"temp_C", "temp_c"}:
                temp_C = obs_value
            display = format_us_display(
                wave_height_m=wave_m,
                wind_ms=wind_ms,
                temp_C=temp_C
            )
            wave_display = display.get("wave_height_display", "None")
            wind_display = display.get("wind_display", "None")
            temp_display = display.get("temp_display", "None")
            popup_fields = [f"<b>Weather</b>", _latlon_str(lat, lon)]
            if ts_utc: popup_fields.append(f"UTC: {ts_utc}")
            if ts_local: popup_fields.append(f"Local: {ts_local}")
            popup_fields += [
                f"Waves: {_html_text(wave_display)}",
                f"Wind: {_html_text(wind_display)}",
                f"Temp: {_html_text(temp_display)}",
            ]
            popup_html = "<br>".join(popup_fields)
            source_type = _coalesce(w.get("source_type"), default="")
            rows.append({
                "site_id": site_id,
                "layer": "weather",
                "geom_type": "Point",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                # If model/interpolated, treat as “spot”; else let renderer default
                "icon_key": "wx_spot" if str(source_type).lower() == "model_interpolated" else "wx_station",
                "ts_utc": ts_utc,
                "ts_local": ts_local,
                "local_tz": local_tz,
                "label": "Weather",
                "popup_html": popup_html,
                "style_hint": {"weight": 2, "opacity": 0.8},
                "source_table": "weather",
                "source_id": _coalesce(w.get("station_id"), default=""),
                "is_maritime": maritime_flag,
                "range_ring_meters": None,
                "wave_height_m": wave_m,
                "wind_ms": wind_ms,
                "temp_C": temp_C,
                "wave_height_display": wave_display,
                "wind_display": wind_display,
                "temp_display": temp_display,
            })

    # --- Stations layer ---
    if stations_df is not None and not stations_df.empty:
        for _, s in stations_df.iterrows():
            lat = s.get("lat_dd")
            lon = s.get("lon_dd")
            label = _coalesce(s.get("name"), default="Station")
            station_type = _coalesce(s.get("type"), default="N/A")
            wave_display = s.get("wave_height_display", "None")
            wind_display = s.get("wind_display", "None")
            temp_display = s.get("temp_display", "None")
            popup_fields = [
                f"<b>{_html_text(label)}</b>",
                _latlon_str(lat, lon),
                f"Type: {_html_text(station_type)}",
                f"Waves: {_html_text(wave_display)}",
                f"Wind: {_html_text(wind_display)}",
                f"Temp: {_html_text(temp_display)}"
            ]
            popup_html = "<br>".join(popup_fields)
            rows.append({
                "site_id": site_id,
                "layer": "station",
                "geom_type": "Point",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "icon_key": _icon_key_for_station(station_type),
                "ts_utc": None,
                "ts_local": None,
                "local_tz": local_tz,
                "label": label,
                "popup_html": popup_html,
                "style_hint": {"weight": 2, "opacity": 0.7},
                "source_table": "stations",
                "source_id": _coalesce(
                    s.get("station_id"), s.get("source_id"), s.get("id"), default=""
                ),
                "is_maritime": maritime_flag,
                "range_ring_meters": None,
                "wave_height_m": None,
                "wind_ms": None,
                "temp_C": None,
                "wave_height_display": wave_display,
                "wind_display": wind_display,
                "temp_display": temp_display,
            })

    # --- Satellite overlays ---
    if sat_overlays is not None:
        # Normalize to iterable of overlay dicts
        sat_items = []
        if isinstance(sat_overlays, pd.DataFrame):
            # Convert rows → overlay dicts (Circle, LineString, Point)
            for _, r in sat_overlays.iterrows():
                role = str(_coalesce(r.get("role"), default=""))
                style = _sat_style_for_role(role)
                label = _satellite_name(r)
                try:
                    norad_id = int(r.get("norad_id"))
                except (TypeError, ValueError):
                    norad_id = None
                if norad_id is not None:
                    label += f" ({norad_id})"
                if role:
                    label += f" – {role}"
                popup_html = _coalesce(r.get("popup_html"), default=None)
                sat_type = _satellite_type(r)

                # A valid subpoint remains useful even when footprint radius
                # metadata is missing or malformed.
                lat = r.get("lat_dd")
                lon = r.get("lon_dd")
                if _valid_coordinate_pair(lon, lat):
                    lat = float(lat)
                    lon = float(lon)
                    try:
                        radius_km = float(r.get("footprint_radius_km"))
                    except (TypeError, ValueError):
                        radius_km = float("nan")
                    if math.isfinite(radius_km) and radius_km > 0:
                        sat_items.append({
                            "type": "Circle",
                            "center": [lon, lat],
                            "radius_m": radius_km * 1000.0,
                            "label": label,
                            "style": style,
                            "popup_html": popup_html,
                        })

                    sat_items.append({
                        "type": "Point",
                        "coordinates": [lon, lat],
                        "label": f"{label} – subpoint",
                        "popup_html": popup_html,
                        "icon_key": _icon_key_for_sat(r),
                        "sat_type": sat_type,
                    })
                # Track (optional)
                tc = r.get("track_coords")
                if isinstance(tc, (list, tuple)) and len(tc) > 1:
                    clean_track = []
                    for point in tc:
                        try:
                            lon, lat = point
                        except (TypeError, ValueError):
                            continue
                        if _valid_coordinate_pair(lon, lat):
                            clean_track.append([float(lon), float(lat)])
                    if len(clean_track) > 1:
                        sat_items.append({
                            "type": "LineString",
                            "coordinates": clean_track,
                            "label": f"{label} – track",
                            "style": {"color": "#0b84f3", "weight": 1, "opacity": 0.6, "dashArray": "2,6"},
                            "sat_type": sat_type,  # [updated]
                        })
                # Next-pass marker (optional)
                npm = r.get("next_pass_marker")
                if not isinstance(npm, dict):
                    npm = {}
                coords = npm.get("coordinates")
                if (
                    isinstance(coords, (list, tuple))
                    and len(coords) == 2
                    and _valid_coordinate_pair(coords[0], coords[1])
                ):
                    sat_items.append({
                        "type": "Point",
                        "coordinates": [float(coords[0]), float(coords[1])],
                        "label": f"{label} – next pass",
                        "popup_html": popup_html,
                        "icon_key": _icon_key_for_sat(r),
                        "sat_type": sat_type,  # [updated]
                    })
        else:
            # Already a list of overlay dicts
            sat_items = list(sat_overlays)

        for o in sat_items:
            if not isinstance(o, dict):
                LOG.warning("Skipping malformed satellite overlay entry: %r", o)
                continue
            # ImageOverlay passthrough
            if o.get("type") == "ImageOverlay" and not _is_missing(o.get("bounds")):
                rows.append({
                    "site_id": positions_df["site_id"].iloc[0] if positions_df is not None and not positions_df.empty else None,
                    "layer": "satellite_overlay",
                    "geom_type": "ImageOverlay",
                    "geometry": {
                        "type": "ImageOverlay",
                        "bounds": o["bounds"],
                        "image_path": o.get("image_path", ""),
                        "opacity": o.get("opacity", 0.6),
                        "name": o.get("name", "overlay")
                    },
                    "ts_utc": None, "ts_local": None, "local_tz": None,
                    "label": o.get("name", "Satellite Overlay"),
                    "popup_html": o.get("popup_html")
                })
                continue

            # Circle footprint
            center = o.get("center")
            radius_m = _positive_finite(o.get("radius_m"))
            if (
                o.get("type") == "Circle"
                and isinstance(center, (list, tuple))
                and len(center) == 2
                and _valid_coordinate_pair(center[0], center[1])
                and radius_m is not None
            ):
                rows.append({
                    "site_id": positions_df["site_id"].iloc[0] if positions_df is not None and not positions_df.empty else None,
                    "layer": "satellite_overlay",
                    "geom_type": "Circle",
                    "geometry": {
                        "type": "Circle",
                        "center": [float(center[0]), float(center[1])],
                        "radius_m": radius_m,
                        "style": o.get("style", {})
                    },
                    "ts_utc": None, "ts_local": None, "local_tz": None,
                    "label": o.get("label",""),
                    "popup_html": o.get("popup_html"),
                    "icon_key": o.get("icon_key"),                      # [updated]
                    "sat_type": str(_coalesce(o.get("sat_type"), default="")).lower()  # [updated]
                })
                continue

            # Track line
            coordinates = o.get("coordinates")
            if (
                o.get("type") == "LineString"
                and isinstance(coordinates, (list, tuple))
                and len(coordinates) > 1
            ):
                clean_coordinates = []
                for point in coordinates:
                    if (
                        isinstance(point, (list, tuple))
                        and len(point) == 2
                        and _valid_coordinate_pair(point[0], point[1])
                    ):
                        clean_coordinates.append(
                            [float(point[0]), float(point[1])]
                        )
                if len(clean_coordinates) < 2:
                    continue
                rows.append({
                    "site_id": positions_df["site_id"].iloc[0] if positions_df is not None and not positions_df.empty else None,
                    "layer": "satellite_overlay",
                    "geom_type": "LineString",
                    "geometry": {
                        "type": "LineString",
                        "coordinates": clean_coordinates,
                        "style": o.get("style", {})
                    },
                    "ts_utc": None, "ts_local": None, "local_tz": None,
                    "label": o.get("label",""),
                    "popup_html": o.get("popup_html"),
                    "icon_key": o.get("icon_key"),                      # [updated]
                    "sat_type": str(_coalesce(o.get("sat_type"), default="")).lower()  # [updated]
                })
                continue

            # Next-pass marker
            coordinates = o.get("coordinates")
            if (
                o.get("type") == "Point"
                and isinstance(coordinates, (list, tuple))
                and len(coordinates) == 2
                and _valid_coordinate_pair(coordinates[0], coordinates[1])
            ):
                rows.append({
                    "site_id": positions_df["site_id"].iloc[0] if positions_df is not None and not positions_df.empty else None,
                    "layer": "satellite_overlay",
                    "geom_type": "Point",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [
                            float(coordinates[0]), float(coordinates[1])
                        ],
                    },
                    "ts_utc": None, "ts_local": None, "local_tz": None,
                    "label": o.get("label","Next pass"),
                    "popup_html": o.get("popup_html"),
                    "icon_key": o.get("icon_key"),                      # [updated]
                    "sat_type": str(_coalesce(o.get("sat_type"), default="")).lower()  # [updated]
                })

    # --- Satellite overlay (footprint + short track + optional next-pass) ---
    try:
        from app.sat_pipeline import build_sat_overlay_df

        # Build minimal alert_df for SAT pipeline from available context
        _lat = positions_df.iloc[0]['lat_dd'] if positions_df is not None and not positions_df.empty else None
        _lon = positions_df.iloc[0]['lon_dd'] if positions_df is not None and not positions_df.empty else None
        # Satellite geometry is time-dependent. Never substitute the current
        # time for a missing or malformed alert timestamp because that would
        # present a plausible-looking position for the wrong instant.
        if not pd.api.types.is_scalar(alert_time_utc) or isinstance(
            alert_time_utc, (bool, int, float, np.number)
        ):
            _alert_time = pd.NaT
        else:
            _alert_time = pd.to_datetime(
                alert_time_utc, utc=True, errors="coerce"
            )
        _has_alert_time = not pd.isna(_alert_time)
        _alert = pd.DataFrame([{
            'alert_id': site_id,
            'alert_time_utc': _alert_time,
            'alert_lat_dd': _lat,
            'alert_lon_dd': _lon,
            # Optional: upstream may add 'norad_id' when testing old alerts
        }])

        import os
        _test_norad = os.getenv("RDS_SAT_TEST_NORAD")

        if _test_norad and _test_norad.isdigit():
            _alert.loc[0, 'norad_id'] = int(_test_norad)

        # UPDATED: honor RDS_SAT_TYPES instead of hard-coding LEO  # [updated]
        types_env = (os.getenv("RDS_SAT_TYPES","LEO") or "LEO").split(",")  # [updated]
        # A satellite position is only meaningful for a known alert time. It
        # also keeps this transform offline for callers that are only building
        # provided position/weather/station layers.
        if not _has_alert_time:
            _sat_df = pd.DataFrame()
        else:
            _sat_df = build_sat_overlay_df(_alert,  # [updated]
                                   types=tuple(t.strip().upper() for t in types_env if t.strip()),  # [updated]
                                   use_tle=True, fallback_to_nearest=True)  # [updated]

        if _sat_df is not None and not _sat_df.empty:
            # Footprint circle (guardrail: require center + radius)
            for _, r in _sat_df.iterrows():
                sat_name = _satellite_name(r, default="SAT")
                sat_type = _satellite_type(r)
                # Footprint circle
                lat = r.get('lat_dd'); lon = r.get('lon_dd'); rad_km = r.get('footprint_radius_km')
                if pd.notna(lat) and pd.notna(lon) and pd.notna(rad_km):
                    rows.append({
                        "site_id": site_id,
                        "layer": "satellite_overlay",
                        "geom_type": "Circle",
                        "geometry": {
                            "type": "Circle",
                            "center": [float(lon), float(lat)],
                            "radius_m": float(rad_km) * 1000.0
                        },
                        "label": f"{sat_name} (TLE age {r.get('tle_age_hours', 'NA')}h)",
                        "popup_html": f"TLE epoch: {r.get('tle_epoch_utc', 'NA')}",
                        "style_hint": {"weight": 1, "opacity": 0.6},
                        # fields the renderer expects:
                        "lat_dd": float(lat),
                        "lon_dd": float(lon),
                        "footprint_radius_km": float(rad_km),
                        "icon_key": _icon_key_for_sat(r),
                        "sat_type": sat_type,  # [updated]
                    })

                    # Subpoint marker at footprint center (always show a SAT icon/marker)  # [updated]
                    rows.append({
                        "site_id": site_id,
                        "layer": "satellite_overlay",
                        "geom_type": "Point",
                        "geometry": {"type": "Point", "coordinates": [float(lon), float(lat)]},
                        "label": f"{sat_name} – subpoint",
                        "popup_html": _coalesce(r.get("popup_html"), default=None),
                        "icon_key": _icon_key_for_sat(r),
                        "lat_dd": float(lat), "lon_dd": float(lon),
                        "footprint_radius_km": None,
                        "sat_type": sat_type,  # [updated]
                    })

                # Short forward track
                _track = r.get("track_coords")
                if isinstance(_track, (list, tuple)) and len(_track) > 1:
                    # Drop any vertices with NaN/None and coerce to float
                    _clean = []
                    for pt in _track:
                        try:
                            lon, lat = pt  # stored as [lon, lat]
                            if _valid_coordinate_pair(lon, lat):
                                _clean.append((float(lon), float(lat)))
                        except Exception:
                            continue
                    if len(_clean) > 1:
                        first_lon, first_lat = _clean[0]
                        rows.append({
                            "site_id": site_id,
                            "layer": "satellite_overlay",
                            "geom_type": "LineString",
                            "geometry": {"type": "LineString", "coordinates": _clean},
                            "label": "Satellite track",
                            "popup_html": "Forward track",
                            "style_hint": {"dash": "4,6", "opacity": 0.6},
                            # keep columns non-null so Folium doesn't crash:
                            "lat_dd": float(first_lat),
                            "lon_dd": float(first_lon),
                            "footprint_radius_km": None,
                            "sat_type": sat_type,  # [updated]
                        })

                # Next-pass marker (prefer npm['coordinates'] if present)
                npm = r.get("next_pass_marker")
                if not isinstance(npm, dict):
                    npm = {}
                coords = npm.get("coordinates")
                if (
                    isinstance(coords, (list, tuple))
                    and len(coords) == 2
                    and _valid_coordinate_pair(coords[0], coords[1])
                ):
                    _sat_type = sat_type
                    _icon_key = "sat_geo" if "geo" in _sat_type else ("sat_meo" if "meo" in _sat_type else "sat_leo")
                    rows.append({
                        "site_id": site_id,
                        "layer": "satellite_overlay",
                        "geom_type": "Point",
                        "geometry": {"type": "Point", "coordinates": [float(coords[0]), float(coords[1])]},
                        "label": "Next pass",
                        "popup_html": r.get("popup_html"),
                        "style_hint": {"icon": "pin"},
                        "lat_dd": float(coords[1]),
                        "lon_dd": float(coords[0]),
                        "footprint_radius_km": None,
                        "icon_key": _icon_key,
                        "sat_type": _sat_type,
                    })
    except Exception as _sat_e:
        LOG.warning(f"[SAT] overlay inject skipped: {_sat_e}")

    # -- drop invalid geometries (no NaNs) --
    def _valid_coords_pair(p):
        try:
            lon, lat = p
        except (TypeError, ValueError):
            return False
        return _valid_coordinate_pair(lon, lat)

    def _is_valid_row(row):
        g = row.get("geometry") or {}
        t = row.get("geom_type")
        if t == "Point":
            coords = g.get("coordinates")
            return isinstance(coords, (list, tuple)) and len(coords) == 2 and _valid_coords_pair(coords)
        if t == "Circle":
            center = g.get("center"); rad = g.get("radius_m")
            try:
                valid_radius = math.isfinite(float(rad)) and float(rad) > 0
            except (TypeError, ValueError):
                valid_radius = False
            return (
                isinstance(center, (list, tuple))
                and len(center) == 2
                and _valid_coords_pair(center)
                and valid_radius
            )
        if t == "LineString":
            coords = g.get("coordinates")
            if not isinstance(coords, (list, tuple)) or len(coords) < 2:
                return False
            # all vertices valid
            return all(_valid_coords_pair(pt) for pt in coords)
        # other types pass through
        return True

    df_out = pd.DataFrame(rows)
    if not df_out.empty:
        df_out = df_out[df_out.apply(_is_valid_row, axis=1)].reset_index(drop=True)

    # Ensure contract columns exist
    contract_cols = [
        'site_id','layer','geom_type','geometry','ts_utc','ts_local','local_tz',
        'label','popup_html','style_hint','source_table','source_id','is_maritime',
        'range_ring_meters','wave_height_m','wind_ms','temp_C',
        'wave_height_display','wind_display','temp_display'
    ]
    for col in contract_cols:
        if col not in df_out.columns:
            df_out[col] = None

    # -- derive lat_dd/lon_dd from geometry for all rows --
    if 'lat_dd' not in df_out.columns:
        df_out['lat_dd'] = np.nan
    if 'lon_dd' not in df_out.columns:
        df_out['lon_dd'] = np.nan

    # Preserve the full output schema for empty inputs without asking pandas to
    # expand rows that do not exist.
    if df_out.empty:
        return df_out

    def _derive_latlon(row):
        g = row.get('geometry') or {}
        t = row.get('geom_type')
        try:
            if t == 'Point':
                lon, lat = g.get('coordinates', (None, None))
                return lat, lon
            if t == 'Circle':
                lon, lat = g.get('center', (None, None))
                return lat, lon
            if t == 'LineString':
                coords = g.get('coordinates', [])
                if isinstance(coords, (list, tuple)) and len(coords) > 0:
                    lon, lat = coords[0]
                    return lat, lon
        except Exception:
            pass
        return row.get('lat_dd'), row.get('lon_dd')

    _latlon = df_out.apply(_derive_latlon, axis=1, result_type='expand')
    df_out['lat_dd'] = df_out['lat_dd'].fillna(_latlon[0])
    df_out['lon_dd'] = df_out['lon_dd'].fillna(_latlon[1])

    # Drop rows that still have NaN location where a location is required
    need_loc = df_out['geom_type'].isin(['Point','Circle','LineString'])
    df_out = df_out[~(need_loc & (df_out['lat_dd'].isna() | df_out['lon_dd'].isna()))].reset_index(drop=True)

    # -- normalize geometry: coerce JSON/text -> dict --
    import json
    def _norm_geom(g):
        if isinstance(g, str):
            gs = g.strip()
            if (gs.startswith("{") and gs.endswith("}")) or (gs.startswith("[") and gs.endswith("]")):
                try:
                    return json.loads(gs)
                except Exception:
                    return None
            return None
        return g
    if 'geometry' in df_out.columns:
        df_out['geometry'] = df_out['geometry'].apply(_norm_geom)

    return df_out

# --- SAT role→style mapping for renderer (Folium expects these keys) ---
def _sat_style_for_role(role: str) -> dict:
    role = (role or "").lower()
    if role == "reported":
        return {"color": "#0b84f3", "weight": 1, "fill": True,  "fillOpacity": 0.15}
    if role == "nearby_not_detected":
        return {"color": "#0b84f3", "weight": 1, "fill": False, "fillOpacity": 0.0}
    if role == "upcoming":
        return {"color": "#0b84f3", "weight": 1, "fill": False, "fillOpacity": 0.0, "dashArray": "6,6"}
    return {"color": "#0b84f3", "weight": 1, "fill": False, "fillOpacity": 0.0}

from app.utils_display import format_us_display, to_dual_time, derive_local_tz

# --- ICON KEY HELPERS ---
def _icon_key_for_station(stype: str) -> str:
    s = (stype or "").lower()
    if "buoy" in s:
        return "wx_buoy"
    return "wx_station"

def _icon_key_for_sat(row: dict) -> str:
    t = _satellite_type(row)  # prefer baseline-merged 'sat_type'
    if "geo" in t:
        return "sat_geo"
    if "meo" in t:
        return "sat_meo"
    if "leo" in t:
        return "sat_leo"
    # Last-resort fallback only if type is missing everywhere:
    alt_km = _coalesce(row.get("alt_km"), row.get("altitude_km"), default=None)
    try:
        alt_km = float(alt_km) if alt_km is not None else None
    except Exception:
        alt_km = None
    if alt_km is None:
        return "sat_leo"
    if alt_km > 15000:
        return "sat_geo"
    if alt_km > 2000:
        return "sat_meo"
    return "sat_leo"
# --- END ICON KEY HELPERS ---
