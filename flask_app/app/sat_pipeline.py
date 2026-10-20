# ============================== RDS STANDARD HEADER ==============================
# Script Name: sat_pipeline.py
# Last Updated (UTC): 2025-09-04
# Update Summary:
# - New orchestrator for satellite overlays. MVP returns sat_overlay_df for reporting sat.
# - Uses sat_fetch_tle for reference/resolve; sat_compute_footprint for footprint radius.
# - TLE path enabled by default (use_tle=True).
# Description:
# - Purpose: One-shot builder for satellite overlays per alert (no background jobs).
#   Produces operator-facing DataFrames:
#     1) sat_overlay_df (authoritative per-satellite overlay rows)
#     2) sat_tracks_df (optional; empty in MVP)
# - Primary Inputs:
#   * alert_df (authoritative alert data incl. SAT hint and A/B positions)
#   * SARSAT satellite reference CSV via sat_fetch_tle.load_sat_reference()
# - Primary Outputs:
#   * sat_overlay_df columns:
#       ['sat_id','name','type','owner','constellation','altitude_km',
#        'lat_dd','lon_dd','footprint_radius_km','snapshot_utc','visible_for']
#   * sat_tracks_df columns (not populated in MVP):
#       ['sat_id','when_utc','lat_dd','lon_dd','segment']
# - External Data Sources:
#   * Local CSVs (reference; optional designator map). No network I/O in this module.
# - Internal Variables:
#   * snapshot_utc (datetime UTC at start of run)
#   * scope: 'reporting' (MVP) | 'visible' | 'incoming' (future)
#   * types: ('LEO',) by default; can include 'MEO','GEO' later
# - Produced DataFrames:
#   * sat_overlay_df (authoritative), sat_tracks_df (optional)
# - Data Handling Notes:
#   * DataFrame-first; NaN for missing; no exceptions for empty results (warn and return empty).
#   * Distances in km internally; map handles display conversions.
# ===============================================================================

from app.setup_imports import *
from datetime import datetime, timezone
from html import escape as html_escape
from typing import Optional, Tuple
import logging
import math
import os
from pathlib import Path

from app.sat_fetch_tle import load_sat_reference, resolve_reporting_sat, load_tle_snapshot
from app.sat_compute_groundtrack import compute_subpoint_at, compute_short_track, find_next_pass_marker
from app.sat_compute_footprint import annotate_footprint_radius
import pandas as pd, numpy as np

LOG = logging.getLogger(__name__)

DEFAULT_MAX_TLE_AGE_HOURS = 168.0


def _positive_catalog_id(value):
    """Return a positive whole NORAD catalog ID, or ``None`` when unusable."""
    if value is None or isinstance(value, (bool, np.bool_)):
        return None
    if not pd.api.types.is_scalar(value):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(numeric) or numeric <= 0 or not numeric.is_integer():
        return None
    return int(numeric)


def _optional_text(value):
    """Return stripped scalar text without evaluating nullable values as booleans."""
    if value is None or not pd.api.types.is_scalar(value):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        return None
    text = str(value).strip()
    return text or None


def _coordinate_pair(lat, lon):
    """Return a finite in-range scalar coordinate pair, or ``None``."""
    if isinstance(lat, (bool, np.bool_)) or isinstance(lon, (bool, np.bool_)):
        return None
    if not pd.api.types.is_scalar(lat) or not pd.api.types.is_scalar(lon):
        return None
    try:
        lat_value = float(lat)
        lon_value = float(lon)
    except (TypeError, ValueError, OverflowError):
        return None
    if (
        not math.isfinite(lat_value)
        or not math.isfinite(lon_value)
        or not -90.0 <= lat_value <= 90.0
        or not -180.0 <= lon_value <= 180.0
    ):
        return None
    return lat_value, lon_value


def _positive_env_int(name: str, default: int) -> int:
    """Read a positive integer setting, falling back on invalid input."""
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        value = default
    if value <= 0:
        LOG.warning("Invalid %s; using %s", name, default)
        return default
    return value


def _positive_env_float(name: str, default: float) -> float:
    """Read a finite positive float setting, falling back on invalid input."""
    try:
        value = float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        value = default
    if not math.isfinite(value) or value <= 0:
        LOG.warning("Invalid %s; using %s", name, default)
        return default
    return value


def _max_tle_age_hours() -> float:
    """Maximum event-to-TLE epoch difference allowed for map propagation."""
    try:
        configured = float(
            os.getenv("RDS_SAT_MAX_TLE_AGE_HOURS", str(DEFAULT_MAX_TLE_AGE_HOURS))
        )
    except (TypeError, ValueError):
        configured = DEFAULT_MAX_TLE_AGE_HOURS
    if not math.isfinite(configured) or configured <= 0:
        LOG.warning(
            "Invalid RDS_SAT_MAX_TLE_AGE_HOURS; using %.0f hours",
            DEFAULT_MAX_TLE_AGE_HOURS,
        )
        return DEFAULT_MAX_TLE_AGE_HOURS
    return configured


def _tle_age_hours(at_time_utc, tle_epoch_utc) -> float:
    at_time = pd.to_datetime(at_time_utc, utc=True, errors="coerce")
    epoch = pd.to_datetime(tle_epoch_utc, utc=True, errors="coerce")
    if pd.isna(at_time) or pd.isna(epoch):
        return float("nan")
    return float(abs((at_time - epoch).total_seconds()) / 3600.0)


def _tle_is_fresh(at_time_utc, tle_epoch_utc) -> bool:
    age = _tle_age_hours(at_time_utc, tle_epoch_utc)
    return not math.isnan(age) and age <= _max_tle_age_hours()


# ----------------------- Public API -----------------------

def build_sat_overlay_df(
    alert_df: pd.DataFrame,
    types: Tuple[str, ...] = None,   # [updated] default via RDS_SAT_TYPES
    use_tle: bool = True,
    scope: str = "reporting",
    fallback_to_nearest: bool = False
) -> pd.DataFrame:
    """
    Orchestrate the one-shot build of sat_overlay_df for the current alert.

    MVP behavior:
      - scope='reporting': return the single reporting satellite (if resolvable),
        or fallback to first active LEO in the reference.
      - use_tle=False: lat/lon left NaN; footprint uses benchmark radius from reference.

    Future:
      - use_tle=True -> compute subpoints (and tracks) from TLEs.
      - scope='visible' -> filter to sats that can see A/B now (requires subpoints).
      - scope='incoming' -> sats that will see A/B within X minutes.

    :param alert_df: authoritative alert DataFrame for the current case.
    :param scope: 'reporting' (MVP) | 'visible' | 'incoming'
    :param types: satellite classes to include (('LEO',), ('LEO','MEO'), etc.)
    :param catalog_manifest_id: optional manifest id for reference CSV path resolution.
    :param use_tle: if True (future), will compute subpoints and tracks (not in MVP).
    :return: sat_overlay_df (authoritative overlay rows)
    """
    COLS = [
        "sat_name","norad_id","at_time_utc","lat_dd","lon_dd","alt_km",
        "footprint_radius_km","tle_epoch_utc","tle_age_hours","source",
        "track_coords","track_start_utc","track_end_utc","next_pass_marker",
        "_variant","distance_km",
        # keep these in final DF:
        "role","popup_html","sat_type"
    ]
    if not isinstance(alert_df, pd.DataFrame) or alert_df.empty:
        return pd.DataFrame(columns=COLS)

    raw_alert_time = alert_df.iloc[0].get("alert_time_utc")
    if not pd.api.types.is_scalar(raw_alert_time):
        LOG.warning("Skipping satellite overlay: alert_time_utc is not scalar")
        return pd.DataFrame(columns=COLS)
    at_time_utc = pd.to_datetime(raw_alert_time, utc=True, errors="coerce")
    if not isinstance(at_time_utc, pd.Timestamp) or pd.isna(at_time_utc):
        LOG.warning("Skipping satellite overlay: alert_time_utc is invalid")
        return pd.DataFrame(columns=COLS)

    sat_ref = load_sat_reference()
    # [updated] allow env override: RDS_SAT_TYPES="LEO,MEO,GEO"
    if types is None:
        import os as _os  # [updated]
        types = tuple(s.strip().upper() for s in _os.getenv("RDS_SAT_TYPES","LEO,MEO,GEO").split(",") if s.strip())  # [updated]
    if types:
        TYPE_MAP = {"LEOSAR":"LEO","GEOSAR":"GEO","MEOSAR":"MEO","LEO":"LEO","GEO":"GEO","MEO":"MEO"}  # [updated]
        want = {TYPE_MAP.get(str(t).upper(), str(t).upper()) for t in types}  # [updated]
        sat_ref["type_norm"] = sat_ref["type"].str.upper().map(TYPE_MAP)  # [updated]
        sat_ref = sat_ref[sat_ref["type_norm"].isin(want)]  # [updated]

    row = resolve_reporting_sat(alert_df)

    # Prefer a NORAD provided in the alert row (test/ingest convenience)
    alert_norad_id = _positive_catalog_id(
        alert_df.iloc[0].get("norad_id", np.nan)
    )
    if alert_norad_id is not None:
        # A resolved reference is a pandas Series, whose truth value is
        # intentionally ambiguous and which does not support ``setdefault``.
        # Convert it before applying the explicit alert override.
        row = {} if row is None else dict(row)
        row["norad_id"] = alert_norad_id
        row.setdefault(
            "common_name",
            _optional_text(alert_df.iloc[0].get("sat_name"))
            or _optional_text(alert_df.iloc[0].get("reporting_sat"))
            or "SAT",
        )

    if row is None:
        row = {}  # defer return so fallback/overrides can run

    name = (
        _optional_text(row.get("common_name"))
        or _optional_text(row.get("designator"))
        or "Unknown"
    )
    row_norad_id = _positive_catalog_id(row.get("norad_id"))
    norad_id = row_norad_id if row_norad_id is not None else np.nan
    source = "reference"
    lat = lon = alt_km = np.nan
    r_km = np.nan
    tle_epoch = pd.NaT
    tle_age_hours = np.nan
    track_coords = None
    track_start_utc = None
    track_end_utc = None
    next_pass_marker = None

    # TLE path
    tle_df = None
    if use_tle and row_norad_id is not None:
        tle_df = load_tle_snapshot([str(row_norad_id)]); source = "celestrak:catnr"
    elif use_tle:
        d0 = str(row.get("designator",""))[:1].upper()
        group = "galileo" if d0=="E" else ("glonass" if d0=="R" else ("beidou" if d0 in ("B","C") else None))
        if group: tle_df = load_tle_snapshot([group]); source = "celestrak:group"

    if use_tle and tle_df is not None and not tle_df.empty:
        t = tle_df.iloc[0]
        tle1, tle2 = t["tle_line1"], t["tle_line2"]
        tle_epoch = pd.to_datetime(t["epoch_utc"], utc=True, errors="coerce")
        if pd.notna(tle_epoch):
            tle_age_hours = float(abs((at_time_utc - tle_epoch).total_seconds())/3600.0)
        # Reject stale elements before any propagation work. Calculating a
        # subpoint, track, or pass from elements that cannot be displayed is
        # both unnecessary and potentially misleading during diagnostics.
        if _tle_is_fresh(at_time_utc, tle_epoch):
            lat, lon, alt_km = compute_subpoint_at(
                tle1, tle2, at_time_utc.to_pydatetime()
            )
            # forward track (+60 min)
            try:
                track_coords, track_start_utc, track_end_utc = compute_short_track(
                    tle1, tle2, at_time_utc.to_pydatetime(), forward_min=30, step_s=60
                )
            except Exception:
                track_coords, track_start_utc, track_end_utc = None, None, None
            # next-pass marker (if alert lat/lon provided)
            if {"alert_lat_dd","alert_lon_dd"}.issubset(alert_df.columns):
                la = alert_df.iloc[0].get("alert_lat_dd")
                lo = alert_df.iloc[0].get("alert_lon_dd")
                alert_position = _coordinate_pair(la, lo)
                if alert_position is not None:
                    try:
                        next_pass_marker = find_next_pass_marker(
                            tle1,
                            tle2,
                            alert_position,
                            at_time_utc.to_pydatetime(),
                            max_hours=12,
                        )
                    except Exception:
                        next_pass_marker = None
        # prefer provider name/CATNR when present
        name = _optional_text(t.get("name")) or name
        provider_norad_id = _positive_catalog_id(t.get("norad_id"))
        if provider_norad_id is not None:
            norad_id = provider_norad_id

    if use_tle and not _tle_is_fresh(at_time_utc, tle_epoch):
        if pd.notna(tle_epoch):
            LOG.warning(
                "Skipping satellite %s: TLE age %.1f hours exceeds %.1f-hour limit",
                norad_id,
                _tle_age_hours(at_time_utc, tle_epoch),
                _max_tle_age_hours(),
            )
        lat = lon = alt_km = np.nan
        track_coords = track_start_utc = track_end_utc = None
        next_pass_marker = None

    # assemble
    overlay = pd.DataFrame([{
        "sat_name": name, "norad_id": norad_id, "at_time_utc": at_time_utc,
        "lat_dd": lat, "lon_dd": lon, "alt_km": alt_km,
        "footprint_radius_km": r_km,
        "tle_epoch_utc": tle_epoch, "tle_age_hours": tle_age_hours,
        "source": source,
        "track_coords": track_coords,
        "track_start_utc": track_start_utc,
        "track_end_utc": track_end_utc,
        "next_pass_marker": next_pass_marker,
    }])
    # compute radius from alt_km -> annotate_footprint_radius expects 'altitude_km'
    overlay["altitude_km"] = overlay["alt_km"]
    overlay = annotate_footprint_radius(overlay).drop(columns=["altitude_km"])
    overlay = overlay.dropna(subset=["lat_dd","lon_dd","footprint_radius_km"])

    # Tag primary as 'reported'
    if not overlay.empty:
        overlay["role"] = "reported"
        overlay["popup_html"] = overlay.apply(lambda r: _build_sat_popup(r), axis=1)

    if overlay.empty and fallback_to_nearest and use_tle:
        try:
            alert_lat = float(alert_df.iloc[0].get("alert_lat_dd"))
            alert_lon = float(alert_df.iloc[0].get("alert_lon_dd"))
            if math.isfinite(alert_lat) and math.isfinite(alert_lon):
                overlay = _fallback_nearest_leo(
                    alert_lat,
                    alert_lon,
                    at_time_utc,
                    types=types,
                )
        except Exception as exc:
            LOG.warning("Nearest-satellite fallback skipped: %s", exc)
            overlay = pd.DataFrame(columns=COLS)
        # The nearest lookup can legitimately return no rows when the provider
        # is unavailable. Keep the overlay contract intact so the independent
        # visible/upcoming enrichment paths can still run safely.
        overlay = overlay.reindex(columns=COLS)
        if not overlay.empty:
            # A proximity fallback is contextual guidance, not the reporting
            # satellite asserted by the incoming alert.
            overlay["role"] = "suggested"
            overlay["popup_html"] = overlay.apply(
                lambda r: _build_sat_popup(r), axis=1
            )

    # Nearby-not-detected & upcoming (optional window via env)
    extra = []
    try:
        la = float(alert_df.iloc[0].get("alert_lat_dd"))
        lo = float(alert_df.iloc[0].get("alert_lon_dd"))
    except (TypeError, ValueError):
        la = lo = float("nan")

    if use_tle and math.isfinite(la) and math.isfinite(lo):
        exclude = set(
            int(value)
            for value in overlay["norad_id"].dropna().astype(int).tolist()
        )
        nearby_top_n = _positive_env_int("RDS_SAT_TOPN_NEARBY", 6)
        show_all = os.getenv("RDS_SAT_SHOW_ALL_VISIBLE", "0") == "1"
        max_cand = _positive_env_int("RDS_SAT_MAX_CANDIDATES", 256)
        try:
            extra.append(
                _visible_candidates(
                    la,
                    lo,
                    at_time_utc,
                    exclude_norad=exclude,
                    max_candidates=max_cand,
                    top_n=(10**9 if show_all else nearby_top_n),
                )
            )
        except Exception as exc:
            LOG.warning("Visible-satellite enrichment skipped: %s", exc)

        max_hours = _positive_env_float("RDS_SAT_MAX_HOURS", 12.0)
        upcoming_top_n = _positive_env_int("RDS_SAT_TOPN_UPCOMING", 4)
        try:
            extra.append(
                _upcoming_passes(
                    la,
                    lo,
                    at_time_utc,
                    exclude_norad=exclude,
                    max_hours=max_hours,
                    max_candidates=max_cand,
                    top_n=upcoming_top_n,
                )
            )
        except Exception as exc:
            LOG.warning("Upcoming-pass enrichment skipped: %s", exc)

    if extra:
        extra_frames = [
            frame
            for frame in extra
            if isinstance(frame, pd.DataFrame) and not frame.empty
        ]
        extra_df = (
            pd.concat(extra_frames, ignore_index=True)
            if extra_frames
            else pd.DataFrame()
        )
        if not extra_df.empty:
            # Preserve contextual fields such as role and satellite type even
            # when the primary/reporting overlay is empty.
            combined_columns = list(
                dict.fromkeys([*overlay.columns, *extra_df.columns])
            )
            overlay = pd.concat(
                [
                    overlay.reindex(columns=combined_columns),
                    extra_df.reindex(columns=combined_columns),
                ],
                ignore_index=True,
            )

    from pathlib import Path

    if os.getenv("RDS_SAT_DEBUG_DUMP") == "1" and not overlay.empty:
        try:
            outp = Path("data/overlays") / f"{alert_df.iloc[0]['alert_id']}_sat_overlay.parquet"
            outp.parent.mkdir(parents=True, exist_ok=True)

            # --- SAT TYPE + ICON NORMALIZATION (before dump/return) ---------------------  # [updated]
            import numpy as _np  # [updated]
            import pandas as _pd  # [updated]
            TYPE_MAP = {"LEOSAR":"LEO","MEOSAR":"MEO","GEOSAR":"GEO","LEO":"LEO","MEO":"MEO","GEO":"GEO"}  # [updated]

            if "sat_type" not in overlay.columns:  # [updated]
                overlay["sat_type"] = _pd.NA  # [updated]

            # fill from type_norm, then from type mapping (if present)  # [updated]
            if "type_norm" in overlay.columns:
                overlay["sat_type"] = overlay["sat_type"].fillna(overlay["type_norm"])  # [updated]
            if "type" in overlay.columns:
                overlay["sat_type"] = overlay["sat_type"].fillna(overlay["type"].astype(str).str.upper().map(lambda x: TYPE_MAP.get(x, x)))  # [updated]

            # derive from altitude where still missing  # [updated]
            if "alt_km" in overlay.columns:
                _m = overlay["sat_type"].isna() & overlay["alt_km"].notna()  # [updated]
                overlay.loc[_m, "sat_type"] = _np.where(overlay.loc[_m, "alt_km"] > 15000, "GEO",
                                             _np.where(overlay.loc[_m, "alt_km"] > 2000, "MEO", "LEO"))  # [updated]

            overlay["sat_type"] = overlay["sat_type"].fillna("LEO").str.upper()  # [updated]

            # icon_key for renderer (maps to sat_* icons)  # [updated]
            if "icon_key" not in overlay.columns:
                overlay["icon_key"] = _pd.NA  # [updated]
            overlay.loc[overlay["sat_type"]=="LEO","icon_key"] = "sat_leo"  # [updated]
            overlay.loc[overlay["sat_type"]=="MEO","icon_key"] = "sat_meo"  # [updated]
            overlay.loc[overlay["sat_type"]=="GEO","icon_key"] = "sat_geo"  # [updated]
            # -----------------------------------------------------------------------------  # [updated]

            overlay.to_parquet(outp, index=False)
        except Exception as e:
            LOG.warning(f"[SAT] parquet dump skipped: {e}")

    # --- Merge baseline 'type' as 'sat_type' for icon selection downstream ---
    try:
        ref = load_sat_reference()
        sat_types = (ref[["norad_id","type"]]
                     .dropna(subset=["norad_id"])
                     .copy())
        sat_types["norad_id"] = sat_types["norad_id"].astype(int)
        sat_types = sat_types.rename(columns={"type": "reference_sat_type"})
        if not overlay.empty and "norad_id" in overlay.columns:
            overlay["norad_id"] = overlay["norad_id"].astype("Int64")
            overlay = overlay.merge(sat_types, on="norad_id", how="left")
            if "sat_type" not in overlay.columns:
                overlay["sat_type"] = overlay["reference_sat_type"]
            else:
                overlay["sat_type"] = overlay["sat_type"].fillna(
                    overlay["reference_sat_type"]
                )
            overlay = overlay.drop(columns=["reference_sat_type"])
    except Exception:
        # non-fatal: icon helper will still have a fallback
        pass
    # --- end merge ---

    return overlay.reindex(columns=COLS)


def build_sat_tracks_df(
    overlay_df: pd.DataFrame,
    minutes_past: int = 10,
    minutes_future: int = 10,
    step_s: int = 60,
    use_tle: bool = False,
) -> pd.DataFrame:
    """
    Optional. Build dashed/dotted ground tracks for satellites in overlay_df.
    MVP: returns empty until use_tle=True path is implemented.

    :return: sat_tracks_df with columns ['sat_id','when_utc','lat_dd','lon_dd','segment']
    """
    if not use_tle:
        logging.info("[sat_pipeline] Tracks disabled in MVP (use_tle=False). Returning empty DF.")
        return _empty_tracks_df()

    # Future: call sat_compute_groundtrack.compute_tracks(...)
    return _empty_tracks_df()


# Backward-compat alias for anything already wired to prior naming:
build_satellites_df = build_sat_overlay_df


# ----------------------- Helpers -----------------------

def _to_float(x) -> float:
    try:
        return float(x)
    except Exception:
        return float("nan")


def _empty_tracks_df() -> pd.DataFrame:
    return pd.DataFrame(columns=["sat_id","when_utc","lat_dd","lon_dd","segment"])

def _haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dlmb/2)**2
    return 2*r*math.asin(math.sqrt(a))

def _fallback_nearest_leo(
    alert_lat,
    alert_lon,
    at_time_utc,
    max_candidates=12,
    top_n=3,
    types=None,
):
    # [updated] honor RDS_SAT_TYPES (default "LEO")
    ref = load_sat_reference()
    import os as _os  # [updated]
    TYPE_MAP = {"LEOSAR":"LEO","MEOSAR":"MEO","GEOSAR":"GEO"}  # [updated]
    if types is None:
        types = _os.getenv("RDS_SAT_TYPES", "LEO").split(",")
    _types = tuple(
        TYPE_MAP.get(str(value).strip().upper(), str(value).strip().upper())
        for value in types
        if str(value).strip()
    )
    ref["type_norm"] = ref["type"].str.upper().map(lambda x: TYPE_MAP.get(x, x))  # [updated]
    cand = ref[ref["type_norm"].isin(_types) & pd.notna(ref["norad_id"])] .copy()  # [updated]
    if cand.empty: 
        return pd.DataFrame()
    catnrs = [str(int(x)) for x in cand["norad_id"].head(max_candidates).tolist()]
    tles = load_tle_snapshot(catnrs)  # returns many rows
    rows = []
    for _, t in tles.iterrows():
        try:
            tle_epoch = pd.to_datetime(t.get("epoch_utc"), utc=True, errors="coerce")
            if not _tle_is_fresh(at_time_utc, tle_epoch):
                continue
            lat, lon, alt_km = compute_subpoint_at(t["tle_line1"], t["tle_line2"], at_time_utc.to_pydatetime())
            dist_km = _haversine_km(alert_lat, alert_lon, lat, lon)
            rows.append({
                "sat_name": t.get("name"),
                "norad_id": t.get("norad_id"),
                "at_time_utc": at_time_utc,
                "lat_dd": lat, "lon_dd": lon, "alt_km": alt_km,
                "footprint_radius_km": np.nan,  # filled later
                "tle_epoch_utc": tle_epoch,
                "tle_age_hours": np.nan,
                "sat_type": (str(ref.loc[ref["norad_id"]==t.get("norad_id"), "type_norm"].iloc[0]) if not ref.empty else None),  # [updated]
                "source": "fallback:nearest",  # [updated]
                "track_coords": None,
                "track_start_utc": None,
                "track_end_utc": None,
                "next_pass_marker": None,
                "_variant": "suggested",
                "distance_km": dist_km,
            })
        except Exception:
            continue
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).sort_values("distance_km").head(top_n)
    # compute footprint from alt
    df["altitude_km"] = df["alt_km"]
    out = annotate_footprint_radius(df).drop(columns=["altitude_km"])
    # compute tle_age_hours where epoch exists
    mask = pd.notna(out["tle_epoch_utc"])
    out.loc[mask, "tle_age_hours"] = (
        out.loc[mask, "at_time_utc"] - out.loc[mask, "tle_epoch_utc"]
    ).dt.total_seconds().abs() / 3600.0
    return out

# --- RDS SAT ROLE HELPERS ---
def _sat_role_style(role: str) -> str:
    # Style is applied downstream; this is a hint for clarity/tests
    return {"reported": "solid", "nearby_not_detected": "outline", "upcoming": "dashed"}.get(role, "solid")


def _popup_scalar(value, default="") -> str:
    """Return display text for a scalar provider value without truth-testing ``pd.NA``."""
    if value is None or not pd.api.types.is_scalar(value):
        return default
    try:
        if pd.isna(value):
            return default
    except (TypeError, ValueError):
        return default
    return str(value)


def _popup_norad_id(value) -> Optional[str]:
    """Return a whole-number NORAD identifier, or omit malformed provider data."""
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric) or not numeric.is_integer() or numeric < 0:
        return None
    return str(int(numeric))


def _popup_timestamp(value) -> Optional[pd.Timestamp]:
    """Normalize a nullable timestamp for popup formatting."""
    if value is None or not pd.api.types.is_scalar(value):
        return None
    try:
        parsed = pd.to_datetime(value, utc=True, errors="coerce")
    except (TypeError, ValueError, OverflowError):
        return None
    return None if pd.isna(parsed) else parsed


def _build_sat_popup(row: dict) -> str:
    # Satellite names originate in provider/reference data. Escape display
    # values while retaining the small amount of markup created here.
    nm = html_escape(_popup_scalar(row.get("sat_name"), "Unknown"), quote=True)
    nid = _popup_norad_id(row.get("norad_id"))
    role = html_escape(_popup_scalar(row.get("role")), quote=True)
    tle_epoch = _popup_timestamp(row.get("tle_epoch_utc"))
    try:
        tle_age = float(row.get("tle_age_hours"))
    except (TypeError, ValueError):
        tle_age = float("nan")
    nxt = row.get("next_pass_marker")
    if not isinstance(nxt, dict):
        nxt = {}
    nxt_when = _popup_timestamp(nxt.get("when_utc"))
    try:
        nxt_elev = float(nxt.get("max_elev_deg"))
    except (TypeError, ValueError):
        nxt_elev = float("nan")
    parts = [f"<b>{nm}</b>{' ('+nid+')' if nid is not None else ''}",
             f"Role: <b>{role}</b>"]
    if tle_epoch is not None:
        parts.append(f"TLE epoch: {tle_epoch.strftime('%Y-%m-%d %H:%MZ')}")
    if math.isfinite(tle_age):
        parts.append(f"TLE age: {round(tle_age,1)} h")
    alert_time = _popup_timestamp(row.get("at_time_utc"))
    if alert_time is not None:
        parts.append(f"Time@alert: {alert_time.strftime('%Y-%m-%d %H:%MZ')}")
    if nxt_when is not None:
        np_txt = f"Next pass: {nxt_when.strftime('%Y-%m-%d %H:%MZ')}"
        if math.isfinite(nxt_elev):
            np_txt += f" • max elev {round(nxt_elev,1)}°"
        parts.append(np_txt)
    return "<br/>".join(parts)

def _visible_candidates(alert_lat: float, alert_lon: float, at_time_utc: pd.Timestamp,
                        exclude_norad: set[int], max_candidates: int = 32, top_n: int = 3) -> pd.DataFrame:
    """Pick LEOs whose footprint contains the alert point at alert time (no detect)."""
    ref = load_sat_reference()
    import os as _os  # [updated]
    _mode_types = [s.strip().upper() for s in _os.getenv("RDS_SAT_TYPES","LEO").split(",") if s.strip()]  # [updated]
    TYPE_MAP = {"LEOSAR":"LEO","MEOSAR":"MEO","GEOSAR":"GEO"}  # [updated]
    ref["type_norm"] = ref["type"].str.upper().map(lambda x: TYPE_MAP.get(x, x))  # [updated]
    cand = ref[ref["type_norm"].isin(_mode_types) & pd.notna(ref["norad_id"])] .copy()  # [updated]
    if cand.empty:
        return pd.DataFrame()
    catnrs = [str(int(x)) for x in cand["norad_id"].tolist()][:max_candidates]
    tles = load_tle_snapshot(catnrs)
    rows = []
    for _, t in tles.iterrows():
        nid = t.get("norad_id")
        if pd.notna(nid) and int(nid) in exclude_norad:
            continue
        try:
            tle_epoch = pd.to_datetime(t.get("epoch_utc"), utc=True, errors="coerce")
            if not _tle_is_fresh(at_time_utc, tle_epoch):
                continue
            lat, lon, alt_km = compute_subpoint_at(t["tle_line1"], t["tle_line2"], at_time_utc.to_pydatetime())
            dist_km = _haversine_km(alert_lat, alert_lon, lat, lon)
            r = pd.DataFrame([{"sat_name": t.get("name"),
                               "norad_id": nid,
                               "at_time_utc": at_time_utc,
                               "lat_dd": lat, "lon_dd": lon, "alt_km": alt_km,
                               "tle_epoch_utc": tle_epoch,
                               "source": "celestrak:catnr"}])
            r["altitude_km"] = r["alt_km"]
            r = annotate_footprint_radius(r).drop(columns=["altitude_km"])
            r["distance_km"] = float(dist_km)
            rows.append(r.iloc[0].to_dict())
        except Exception:
            continue
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["tle_age_hours"] = (
        df["at_time_utc"] - df["tle_epoch_utc"]
    ).dt.total_seconds().abs() / 3600.0
    df = df[pd.notna(df["footprint_radius_km"]) & pd.notna(df["distance_km"])]
    df = df[df["distance_km"] <= df["footprint_radius_km"]].copy()
    df["role"] = "nearby_not_detected"
    df["popup_html"] = df.apply(lambda r: _build_sat_popup(r), axis=1)
    return df.sort_values("distance_km").head(top_n)

def _upcoming_passes(alert_lat: float, alert_lon: float, at_time_utc: pd.Timestamp,
                     exclude_norad: set[int], max_hours: float = 12.0,
                     max_candidates: int = 64, top_n: int = 2) -> pd.DataFrame:
    """Find next pass within window; center footprint at pass time, add next_pass_marker."""
    ref = load_sat_reference()
    import os as _os  # [updated]
    _mode_types = [s.strip().upper() for s in _os.getenv("RDS_SAT_TYPES","LEO").split(",") if s.strip()]  # [updated]
    TYPE_MAP = {"LEOSAR":"LEO","MEOSAR":"MEO","GEOSAR":"GEO"}  # [updated]
    ref["type_norm"] = ref["type"].str.upper().map(lambda x: TYPE_MAP.get(x, x))  # [updated]
    cand = ref[ref["type_norm"].isin(_mode_types) & pd.notna(ref["norad_id"])] .copy()  # [updated]
    if cand.empty:
        return pd.DataFrame()
    catnrs = [str(int(x)) for x in cand["norad_id"].tolist()][:max_candidates]
    tles = load_tle_snapshot(catnrs)
    rows = []
    for _, t in tles.iterrows():
        nid = t.get("norad_id")
        if pd.notna(nid) and int(nid) in exclude_norad:
            continue
        try:
            tle_epoch = pd.to_datetime(t.get("epoch_utc"), utc=True, errors="coerce")
            if not _tle_is_fresh(at_time_utc, tle_epoch):
                continue
            # --- [fixed] upcoming pass search — must be inside the same try:
            marker = find_next_pass_marker(t["tle_line1"], t["tle_line2"],
                                           alert_latlon=(float(alert_lat), float(alert_lon)),
                                           start_utc=at_time_utc.to_pydatetime(),
                                           max_hours=float(max_hours))
            if not marker:
                continue
            when = pd.to_datetime(marker.get("when_utc"), utc=True, errors="coerce")
            if pd.isna(when):
                continue
            # subpoint & footprint at pass time
            plat, plon, alt_km = compute_subpoint_at(t["tle_line1"], t["tle_line2"], when.to_pydatetime())
            r = pd.DataFrame([{"sat_name": t.get("name"),
                               "norad_id": nid,
                               "at_time_utc": when,
                               "lat_dd": plat, "lon_dd": plon, "alt_km": alt_km,
                               "tle_epoch_utc": tle_epoch,
                               "source": "celestrak:catnr",
                               "next_pass_marker": marker}])
            r["altitude_km"] = r["alt_km"]
            r = annotate_footprint_radius(r).drop(columns=["altitude_km"])
            r["role"] = "upcoming"
            r["popup_html"] = r.apply(lambda rr: _build_sat_popup(rr), axis=1)
            rows.append(r.iloc[0].to_dict())
        except Exception:
            continue
    if not rows:
        return pd.DataFrame()
    return (pd.DataFrame(rows)
            .sort_values("at_time_utc")
            .head(top_n))
# --- RDS SAT ROLE HELPERS end ---

