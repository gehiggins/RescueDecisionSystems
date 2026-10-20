# pipeline_controller.py - SARSAT Alert Processing Pipeline
# 2025-03-07 (Fully Integrated with finalize_alert_df.py)
#
# Description:
# Manages the end-to-end processing of SARSAT alert messages, including:
# - Pre-parsing coordinates
# - Parsing SARSAT messages
# - Validating and finalizing alert data
# - Fetching weather data
# - Storing alerts in the database
# - Generating GIS maps
#
# External Data Sources:
# - SARSAT Messages (raw distress alerts)
# - NOAA Shore Weather Data (fetcher_noaa_shore.py)
# - NDBC Offshore Buoy Data (fetcher_ndbc_buoy.py)
#
# Internal Variables:
# - `alert_df`: Structured DataFrame containing parsed SARSAT alert data.
# - `pre_scan_results`: Coordinates extracted via `preparse_coordinate_mapper.py`.
# - `combined_weather_df`: Merged shore and buoy weather data.
#
# Produced DataFrames:
# - `alert_df`: Fully structured SARSAT alert data, validated before storage.
# - `weather_df`: Processed weather observations linked to SARSAT alert positions.
#
# Data Handling Notes:
# - Ensures SARSAT messages are parsed **before** weather fetching.
# - Uses **pre-parsed coordinates** for structured extraction.
# - **Finalizes alert_df** using `finalize_alert_df.py` before committing to the database.
# - Logs **missing or malformed fields** for debugging.
#
# Map Step (Final Output):
# - Role: Orchestrates parse â†’ finalize â†’ (call GIS) â†’ [DB/weather etc.]. It calls the mapping function; it does not render maps itself.
# - Map Step Inputs: one finalized alert row (alert_df.iloc[0]) passed to GIS. (No new ground-truth DataFrame created here in this step.)
# - Map Step Output: HTML at data/maps/<site_id>/gis_map_<site_id>.html and prints:
#   âœ… Interactive map saved: data/maps/<site_id>/gis_map_<site_id>.html (current implementation uses generate_gis_map_html(...)).
# - Notes: DB connectivity is not required for the map smoke; map step should proceed even if DB is unavailable (as seen in prior runs).
# [RDS-ANCHOR: PREAMBLE_END]

import sys
import os
import logging
import json
import math
import re
from pathlib import Path
LOG = logging.getLogger(__name__)

_MAP_ID_MAX_CHARS = 80

# Ensure the `flask_app` directory is added to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from app.setup_imports import *
from app.debug_outputs import dump_dataframe
from app.gis_map_inputs_builder import build_gis_map_inputs_df
from app.gis_mapping import generate_gis_map_html_from_dfs
from app.preparse_coordinate_mapper import pre_scan_for_coordinates
from app.utils import log_error_and_continue, load_sample_message, get_current_utc_timestamp
from app.fetcher_noaa_shore import fetch_noaa_shore_data
from app.fetcher_ndbc_buoy import fetch_ndbc_buoy_data
from app.fetcher_noaa_weather_alerts import fetch_weather_alerts_zone
from app.finalize_alert_df import finalize_alert_df
from app.parser_sarsat_msg import parse_sarsat_message
from app.wx_pipeline import run_wx_pipeline


def _safe_map_site_id(value):
    """Return a bounded filename component for generated-map directories."""
    normalized = re.sub(r"[^A-Za-z0-9_-]+", "_", str(value)).strip("_-")
    return normalized[:_MAP_ID_MAX_CHARS] or "unknown"


def _present_scalar(value):
    """Return a non-missing scalar without evaluating ``pd.NA`` as boolean."""
    if value is None or not pd.api.types.is_scalar(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return value


def _first_present(*values):
    for value in values:
        normalized = _present_scalar(value)
        if normalized is not None:
            return normalized
    return None


def _coerce_position(lat, lon):
    """Return a finite in-range latitude/longitude pair, or ``None``."""
    try:
        lat, lon = float(lat), float(lon)
    except (TypeError, ValueError):
        return None
    if (
        not math.isfinite(lat)
        or not math.isfinite(lon)
        or not -90.0 <= lat <= 90.0
        or not -180.0 <= lon <= 180.0
    ):
        return None
    return lat, lon


def _coerce_range_ring(value):
    try:
        value = float(value)
    except (TypeError, ValueError):
        return 0.0
    return value if math.isfinite(value) and value > 0 else 0.0


def _get_existing_alerts_for_sequence():
    """Return prior alert sequence data when a database is configured.

    Database persistence is optional in the current prototype.  Importing the
    database module without ``DATABASE_URL`` raises before the alert pipeline
    can parse or map anything, so keep that dependency behind this boundary.
    """
    empty_alerts = pd.DataFrame(columns=["site_id", "alert_sequence_number"])
    if not os.getenv("DATABASE_URL"):
        LOG.info("DATABASE_URL is not configured; starting alert sequencing locally.")
        return empty_alerts

    try:
        from app.database import get_existing_alerts

        return get_existing_alerts()
    except Exception as exc:
        LOG.warning("Database lookup unavailable; starting alert sequencing locally: %s", exc)
        return empty_alerts

def load_sat_overlays_for_site(site_id: str):
    """
    Look for PNG overlays in data/sat_overlays/<site_id>/ with <name>.bounds.json
    Bounds JSON schema: [[south, west], [north, east]]
    Returns a list of dicts: {"image_path": str, "bounds": [[s,w],[n,e]], "opacity": float, "name": str}
    """
    overlays_root = Path(__file__).resolve().parents[2] / "data" / "sat_overlays"
    base = overlays_root / _safe_map_site_id(site_id)
    if not base.exists():
        return []

    overlays = []
    for png in base.glob("*.png"):
        j = png.with_suffix(".bounds.json")
        if not j.exists() or png.is_symlink() or j.is_symlink():
            continue
        try:
            bounds = json.loads(j.read_text(encoding="utf-8"))
            if not _valid_overlay_bounds(bounds):
                LOG.warning("Skipping satellite overlay with invalid bounds: %s", j)
                continue
            overlays.append({
                "type": "ImageOverlay",
                "image_path": str(png).replace("\\", "/"),
                "bounds": bounds,
                "opacity": 0.6,
                "name": png.stem,
            })
        except Exception:
            continue
    return overlays


def _valid_overlay_bounds(bounds):
    """Validate ``[[south, west], [north, east]]`` image bounds."""
    try:
        if len(bounds) != 2 or any(len(point) != 2 for point in bounds):
            return False
        south, west = (float(value) for value in bounds[0])
        north, east = (float(value) for value in bounds[1])
    except (TypeError, ValueError):
        return False
    return (
        all(math.isfinite(value) for value in (south, west, north, east))
        and -90 <= south < north <= 90
        and -180 <= west < east <= 180
    )


def _append_model_spot_predictions(df_wx_obs, wx_df):
    """Add model/interpolated weather points to the map-ready weather rows."""
    if (
        not isinstance(df_wx_obs, pd.DataFrame)
        or df_wx_obs.empty
        or "source_type" not in df_wx_obs.columns
    ):
        return wx_df

    spot = df_wx_obs[
        df_wx_obs["source_type"].isin(["model", "model_interpolated"])
    ].copy()
    if spot.empty:
        return wx_df

    spot = spot.rename(
        columns={
            "lat": "lat_dd",
            "lon": "lon_dd",
            "valid_utc": "obs_time",
            "temp_c": "temp_C",
        }
    )
    keep_cols = [
        column
        for column in [
            "lat_dd",
            "lon_dd",
            "obs_time",
            "wind_ms",
            "wave_height_m",
            "temp_C",
            "source_type",
        ]
        if column in spot.columns
    ]
    spot = spot[keep_cols]
    spot["source_type"] = "model_interpolated"

    existing = wx_df if isinstance(wx_df, pd.DataFrame) else pd.DataFrame()
    return pd.concat([existing, spot], ignore_index=True)


def _weather_source_ids(weather_rows):
    """Build stable identifiers for physical station rows from provider output."""
    ids = pd.Series(pd.NA, index=weather_rows.index, dtype="object")
    available = [
        column
        for column in ["station_id", "source_id", "id"]
        if column in weather_rows.columns
    ]
    if available:
        ids = weather_rows[available].astype("object").bfill(axis=1).iloc[:, 0]

    missing = ids.isna() | ids.astype(str).str.strip().eq("")
    if missing.any():
        provider = weather_rows.get(
            "provider", pd.Series("weather", index=weather_rows.index)
        ).fillna("weather")
        latitude = weather_rows.get(
            "lat", pd.Series(pd.NA, index=weather_rows.index)
        )
        longitude = weather_rows.get(
            "lon", pd.Series(pd.NA, index=weather_rows.index)
        )
        generated = (
            provider.astype(str)
            + ":"
            + latitude.astype(str)
            + ":"
            + longitude.astype(str)
        )
        ids.loc[missing] = generated.loc[missing]
    return ids.rename("sid")

def process_sarsat_alert(raw_alert_message, maps_root=None):
    """Run the full alert pipeline and return its parsed data and map result."""

    try:
        logging.info(f"{get_current_utc_timestamp()} ðŸš€ Starting SARSAT alert processing pipeline")

        # Step 1: Pre-Parse Coordinates
        pre_scan_results = pre_scan_for_coordinates(raw_alert_message)
        logging.info(f"{get_current_utc_timestamp()} ðŸ” Pre-scan detected {len(pre_scan_results)} coordinate pairs.")

        # Step 2: Parse SARSAT Message
        parsed_data = parse_sarsat_message(raw_alert_message, pre_scan_results=pre_scan_results)
        if parsed_data is None:
            logging.error(f"{get_current_utc_timestamp()} âŒ SARSAT message parsing failed â€” skipping further processing.")
            return {"status": "error", "error": "Message parsing failed"}

        alert_df = pd.DataFrame([parsed_data])

        LOG.debug(
            "Parsed alert ring fields:\n%s",
            alert_df[
                [
                    "site_id",
                    "expected_error_nm",
                    "range_ring_meters_a",
                    "range_ring_meters_b",
                    "position_method",
                    "format_type_a",
                    "format_type_b",
                ]
            ].to_string(index=False),
        )
        dump_dataframe(alert_df, "debug_final_alert_df.csv")

        logging.info(f"{get_current_utc_timestamp()} ðŸ“ Parsed Positions - A: {parsed_data.get('latitude_a')}, "
                     f"B: {parsed_data.get('latitude_b')}, Status A: {parsed_data.get('position_status_a')}, "
                     f"Status B: {parsed_data.get('position_status_b')}")

        # Step 3: Finalize Alert Data
        # Fetch existing alerts from the database for sequence number assignment
        existing_alerts_db = _get_existing_alerts_for_sequence()

        alert_df = finalize_alert_df(alert_df, existing_alerts_db)  # Pass it to finalize_alert_df()
        logging.info(f"{get_current_utc_timestamp()} âœ… Finalized alert data.")

        LOG.debug("Finalized alert DataFrame:\n%s", alert_df.to_string(index=False))

        """
        # Step 4: Save Alert to Database
        try:
            save_alert_to_db(alert_df)
            logging.info(f"{get_current_utc_timestamp()} âœ… Alert saved with ID: {parsed_data.get('site_id')}")
        except Exception as e:
            log_error_and_continue(f"{get_current_utc_timestamp()} âŒ Failed to save alert to DB: {e}")

        # Step 5: Fetch Weather Data
        for position in ['A', 'B']:
            lat = parsed_data.get(f'latitude_{position.lower()}')
            lon = parsed_data.get(f'longitude_{position.lower()}')

            if pd.isna(lat) or pd.isna(lon):
                logging.info(f"{get_current_utc_timestamp()} ðŸ“ Skipping weather fetch for Position {position} (No valid coordinates)")
                continue

            logging.info(f"{get_current_utc_timestamp()} ðŸŒ¦ï¸ Fetching weather for Position {position} ({lat}, {lon})")

            weather_shore_df = fetch_noaa_shore_data(lat, lon, position_label=position)
            weather_buoy_df = fetch_ndbc_buoy_data(lat, lon, position_label=position)

            combined_weather_df = pd.concat([weather_shore_df, weather_buoy_df], ignore_index=True)

            logging.debug(f"âœ… Combined weather DataFrame columns for Position {position}: {combined_weather_df.columns.tolist()}")
            logging.debug(f"âœ… Combined weather DataFrame for Position {position} (first 5 rows):\n{combined_weather_df.head()}")

            if not isinstance(combined_weather_df, pd.DataFrame):
                raise TypeError(f"Unexpected type for combined_weather_df at Position {position}: {type(combined_weather_df)}")

            if not combined_weather_df.empty:
                save_weather_to_db(combined_weather_df, parsed_data.get('site_id'), position)
                logging.info(f"{get_current_utc_timestamp()} âœ… Combined weather data saved for Position {position}")
            else:
                logging.warning(f"{get_current_utc_timestamp()} âš ï¸ No complete weather data available for Position {position}")

            # Step 6: Fetch Weather Alerts
            try:
                alerts_df = fetch_weather_alerts_zone(lat, lon)
                if not alerts_df.empty:
                    logging.info(f"{get_current_utc_timestamp()} ðŸš¨ {len(alerts_df)} weather alerts retrieved for Position {position}")
                else:
                    logging.info(f"{get_current_utc_timestamp()} âœ… No active weather alerts for Position {position}")
            except Exception as e:
                logging.error(f"{get_current_utc_timestamp()} âŒ Weather alerts fetch failed for Position {position}: {e}")
        """

        # Step 7: Generate GIS Map (HTML, online tiles)
        alert_row = alert_df.iloc[0]
        site_id_raw = alert_row.get('site_id')
        site_id_safe = None
        fallback_reason = None
        # Priority logic for site_id_safe
        site_id_value = _present_scalar(site_id_raw)
        if site_id_value is not None and str(site_id_value).strip().lower() not in ["none", "nan"]:
            site_id_safe = str(site_id_value)
        elif _present_scalar(alert_row.get('beacon_id')) is not None:
            site_id_safe = f"BEACON_{str(alert_row.get('beacon_id'))}"
            fallback_reason = "site_id missing; using beacon_id"
        elif _present_scalar(alert_row.get('alert_sequence_number')) is not None:
            try:
                site_id_safe = f"ALERT_{int(alert_row.get('alert_sequence_number'))}"
                fallback_reason = "site_id missing; using alert_sequence_number"
            except Exception:
                pass
        if not site_id_safe:
            from datetime import datetime
            site_id_safe = f"SMOKE_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
            fallback_reason = "site_id missing; using UTC fallback"
        site_id_safe = _safe_map_site_id(site_id_safe)
        if fallback_reason:
            logging.warning(f"[RDS] GIS map step fallback: {fallback_reason} (site_id_safe={site_id_safe})")

        # Load satellite overlays for this site
        sat_overlays = load_sat_overlays_for_site(site_id_safe)

        LOG.debug("Satellite overlays loaded: %s", len(sat_overlays or []))

        # Build ephemeral positions_df (A row, and B row if present)
        rows = []
        # A
        lat_a = _first_present(
            alert_row.get('position_lat_dd_a'), alert_row.get('latitude_a')
        )
        lon_a = _first_present(
            alert_row.get('position_lon_dd_a'), alert_row.get('longitude_a')
        )
        rr_a  = alert_row.get('range_ring_meters_a')
        position_a = _coerce_position(lat_a, lon_a)
        if position_a is not None:
            lat_a, lon_a = position_a
            rows.append({
                'site_id': site_id_safe, 'role': 'A',
                'lat_dd': lat_a, 'lon_dd': lon_a,
                'range_ring_meters': _coerce_range_ring(rr_a),
                'position_status': alert_row.get('position_status_a'),
                'method': _first_present(
                    alert_row.get('position_method'),
                    alert_row.get('position_method_a'),
                ),
                'range_ring_source': alert_row.get('range_ring_source_a'),
                'ee_nm': alert_row.get('expected_error_nm')
            })

        # B (optional)
        lat_b = _first_present(
            alert_row.get('position_lat_dd_b'), alert_row.get('latitude_b')
        )
        lon_b = _first_present(
            alert_row.get('position_lon_dd_b'), alert_row.get('longitude_b')
        )
        rr_b  = alert_row.get('range_ring_meters_b')
        position_b = _coerce_position(lat_b, lon_b)
        if position_b is not None:
            lat_b, lon_b = position_b
            rows.append({
                'site_id': site_id_safe, 'role': 'B',
                'lat_dd': lat_b, 'lon_dd': lon_b,
                'range_ring_meters': _coerce_range_ring(rr_b),
                'position_status': alert_row.get('position_status_b'),
                'method': _first_present(
                    alert_row.get('position_method'),
                    alert_row.get('position_method_b'),
                ),
                'range_ring_source': alert_row.get('range_ring_source_b'),
                'ee_nm': alert_row.get('expected_error_nm')
            })

        positions_df = pd.DataFrame(rows)

        # --- Weather wiring (minimal) ---
        wx_targets = []
        for _, r in positions_df.iterrows():
            if pd.notna(r.get("lat_dd")) and pd.notna(r.get("lon_dd")):
                wx_targets.append({
                    "site_id": r.get("site_id"),
                    "target": r.get("role"),
                    "lat": float(r["lat_dd"]),
                    "lon": float(r["lon_dd"]),
                })
        wx_targets_df = pd.DataFrame(wx_targets)

        # Fetch unified weather observations (policy handled inside wx sub-pipeline)
        df_wx_obs = run_wx_pipeline(
            alert_targets_df=wx_targets_df,
            hours_back=6,
            radius_km=25.0,
            max_stations=5,
            include_marine=None,  # let policy decide per-point
        )

        # --- Collapse to one row per station with latest values ---
        wx_df = pd.DataFrame()
        station_obs = pd.DataFrame()
        if isinstance(df_wx_obs, pd.DataFrame) and not df_wx_obs.empty:
            if "source_type" in df_wx_obs.columns:
                station_obs = df_wx_obs[df_wx_obs["source_type"] == "station"].copy()
            else:
                station_obs = df_wx_obs.copy()

        if not station_obs.empty:
            sid_series = _weather_source_ids(station_obs)

            agg = (
                pd.concat([station_obs, sid_series], axis=1)
                .sort_values("valid_utc")
                .groupby("sid", dropna=False)
                .agg({
                    "lat": "last",
                    "lon": "last",
                    "valid_utc": "last",
                    "wind_ms": "last",
                    "wave_height_m": "last",
                    "temp_c": "last"
                })
                .reset_index()
                .rename(columns={
                    "sid": "station_id",
                    "lat": "lat_dd",
                    "lon": "lon_dd",
                    "valid_utc": "obs_time",
                    "temp_c": "temp_C"   # <-- add this
                })
            )
            agg["source_type"] = "station"
            wx_df = agg

        # ---- Append model/interpolated SPOT predictions (for ring-dot icon) ----
        wx_df = _append_model_spot_predictions(df_wx_obs, wx_df)

        # Existing stations skeleton (ID/Name/Type/lat/lon)
        st_rows = []
        if isinstance(df_wx_obs, pd.DataFrame) and not df_wx_obs.empty:
            if "source_type" in df_wx_obs.columns:
                ms = df_wx_obs[df_wx_obs["source_type"] == "station"]
            else:
                ms = df_wx_obs
            for _, s in ms.iterrows():
                st_rows.append({
                    "station_id": _first_present(
                        s.get("station_id"), s.get("source_id"), s.get("id")
                    ),
                    "name": s.get("name"),
                    "type": _first_present(s.get("provider")) or "station",
                    "lat_dd": s.get("lat"),
                    "lon_dd": s.get("lon"),
                })
        stations_df = pd.DataFrame(st_rows) if st_rows else pd.DataFrame()

        # Merge latest wx values into stations_df
        if (
            not stations_df.empty
            and not wx_df.empty
            and "station_id" in stations_df.columns
            and "station_id" in wx_df.columns
        ):
            merge_cols = [c for c in ["station_id","lat_dd","lon_dd","wind_ms","wave_height_m","temp_C","obs_time"] if c in wx_df.columns]
            stations_df = stations_df.merge(
                wx_df[merge_cols],
                on="station_id",
                how="left"
            )

        # --- Unified GIS map inputs ---
        op_tz_env = os.getenv("RDS_OPERATOR_TZ")
        shore_nm = 5.0

        gis_map_inputs_df = build_gis_map_inputs_df(
            positions_df,
            wx_df=wx_df,
            stations_df=stations_df,
            op_tz_env=op_tz_env,
            shore_nm=shore_nm,
            sat_overlays=sat_overlays,
            alert_time_utc=alert_row.get("detect_time"),
        )

        try:
            n_sat = int(gis_map_inputs_df[gis_map_inputs_df["layer"]=="satellite_overlay"].shape[0])
        except Exception:
            n_sat = 0
        LOG.debug("Satellite overlay map rows: %s", n_sat)

        LOG.info(f"GIS map inputs row counts by layer: {gis_map_inputs_df['layer'].value_counts().to_dict()}")

        # --- Map render (write to a real HTML file path) ---
        from pathlib import Path

        # Reuse the normalized identifier at the final write boundary. Using
        # the raw alert value here would undo the earlier path-safety check.
        site_id = site_id_safe
        if maps_root is None:
            maps_root = Path(__file__).resolve().parents[2] / "data" / "maps"
        out_dir = Path(maps_root) / site_id
        out_dir.mkdir(parents=True, exist_ok=True)
        out_html = out_dir / f"gis_map_{site_id}.html"

        result = generate_gis_map_html_from_dfs(
            gis_map_inputs_df,
            alert_row.to_dict(),
            str(out_html)
        )


        LOG.info(f"Available layers: {result.get('layers', [])}")

        LOG.info(f"Map HTML output path: {result.get('map_html_path')}")
        return {
            "status": "ok",
            "parsed_data": alert_row.to_dict(),
            "map_html_path": result.get("map_html_path"),
            "layers": result.get("layers", []),
            "weather_rows": int(
                gis_map_inputs_df[gis_map_inputs_df["layer"] == "weather"].shape[0]
            ),
            "satellite_rows": n_sat,
        }
                
    except Exception as e:
        log_error_and_continue(f"{get_current_utc_timestamp()} âŒ Pipeline processing error: {e}")
        return {"status": "error", "error": str(e)}
    
def _default_sample_message_path():
    return Path(__file__).resolve().parents[2] / "sample_sarsat_message.txt"


def main(argv=None):
    """Run the alert pipeline against an explicitly selected message file."""
    import argparse

    parser = argparse.ArgumentParser(description="Process one SARSAT message")
    parser.add_argument(
        "message_file",
        nargs="?",
        type=Path,
        default=_default_sample_message_path(),
        help="message file to process (defaults to the repository sample)",
    )
    args = parser.parse_args(argv)
    process_sarsat_alert(load_sample_message(args.message_file))


if __name__ == "__main__":
    main()



