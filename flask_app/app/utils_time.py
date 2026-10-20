# utils_time.py â€” Time & UTC utilities for Rescue Decision Systems
# Last Updated (UTC): 2025-09-11
# Update Summary:
# â€¢ New module: centralized UTC normalization and time window helpers.
# â€¢ Mirrors ensure_utc() semantics from wx_fetch_meteostat.py.
# â€¢ Adds safe range coercion, hourly alignment, and DataFrame window slicing.
#
# Description:
# â€¢ Provides consistent, reusable helpers for tz-naive vs tz-aware timestamps,
#   UTC conversions, hourly floor/ceil, and clamping/slicing to time windows.
#
# External Data Sources:
# â€¢ None
#
# Internal Variables:
# â€¢ N/A (stateless utility functions)
#
# Produced DataFrames:
# â€¢ N/A (helpers operate on scalars/indices/frames passed in)
#
# Data Handling Notes:
# â€¢ All functions returning timestamps produce tz-aware UTC pd.Timestamp.
# â€¢ Invalid inputs â†’ None/NaN-preserving behavior without throwing when possible.

from app.setup_imports import *  # pandas as pd, numpy as np, logging, etc.

LOG = logging.getLogger(__name__)


# ---------- Core UTC helpers ----------

def ensure_utc(ts) -> Optional[pd.Timestamp]:
    """
    Normalize any timestamp-like input to a tz-aware UTC pandas Timestamp.

    Behavior:
      â€¢ If ts is None/NaT/invalid â†’ returns None.
      â€¢ If ts is tz-naive â†’ localize to UTC.
      â€¢ If ts is tz-aware â†’ convert to UTC.

    Mirrors the proven logic used in the Meteostat path so downstream code
    can rely on consistent semantics.
    """
    # Pandas accepts numbers as nanoseconds since the Unix epoch. RDS inputs
    # are human/provider timestamps, so accepting a numeric field would turn
    # malformed data into a plausible but incorrect 1970 time.
    if not pd.api.types.is_scalar(ts) or isinstance(
        ts, (bool, int, float, np.number)
    ):
        return None
    try:
        t = pd.to_datetime(ts, errors="coerce")
    except (TypeError, ValueError):
        return None
    # This helper is intentionally scalar. Treat array-like inputs as invalid
    # instead of triggering ambiguous truth-value errors in the NaT check.
    if not isinstance(t, (pd.Timestamp, datetime)):
        return None
    if t is None or pd.isna(t):
        return None
    t = pd.Timestamp(t)
    if getattr(t, "tzinfo", None) is None:
        return t.tz_localize("UTC")
    return t.tz_convert("UTC")


def is_utc(ts) -> bool:
    """Return True iff ts is tz-aware and in UTC."""
    t = ensure_utc(ts)
    return t is not None and getattr(t, "tz", None) is not None


def now_utc() -> pd.Timestamp:
    """Current time as tz-aware UTC pandas Timestamp."""
    return pd.Timestamp.now(tz="UTC")


# ---------- Index normalization ----------

def ensure_utc_index(idx: pd.Index) -> pd.DatetimeIndex:
    """
    Convert any datetime-like index to tz-aware UTC DatetimeIndex.
    Non-datetime indexes are attempted via to_datetime(coerce).
    """
    # ``utc=True`` handles naive, aware, and mixed-offset values in one pass.
    # Without it, daylight-saving transitions can produce an object index that
    # has no tz_localize/tz_convert methods.
    dt = pd.to_datetime(idx, errors="coerce", utc=True)
    return pd.DatetimeIndex(dt)


# ---------- Window & alignment helpers ----------

def coerce_utc_range(start, end, allow_swap: bool = True) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    """
    Convert (start, end) to UTC timestamps. Optionally swap if start > end.

    Returns (start_utc, end_utc) possibly with one or both None if invalid.
    """
    s = ensure_utc(start)
    e = ensure_utc(end)
    if s is not None and e is not None and allow_swap and s > e:
        s, e = e, s
    return s, e


def floor_to_hour_utc(ts) -> Optional[pd.Timestamp]:
    """Floor a timestamp to the start of its hour in UTC."""
    t = ensure_utc(ts)
    return None if t is None else t.replace(minute=0, second=0, microsecond=0)


def ceil_to_hour_utc(ts) -> Optional[pd.Timestamp]:
    """Ceil a timestamp to the start of the next hour (if not already on the hour) in UTC."""
    t = ensure_utc(ts)
    if t is None:
        return None
    floored = floor_to_hour_utc(t)
    return floored if t == floored else floored + pd.Timedelta(hours=1)


def align_hourly_range(start, end) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    """
    Floor start to the hour, ceil end to the hour, both UTC.
    Useful when expected data cadence is hourly.
    """
    s, e = coerce_utc_range(start, end)
    return floor_to_hour_utc(s), ceil_to_hour_utc(e)


def clamp_index_to_range(idx: pd.DatetimeIndex,
                         start_utc: Optional[pd.Timestamp],
                         end_utc: Optional[pd.Timestamp]) -> pd.DatetimeIndex:
    """Return the subset of UTC index within [start_utc, end_utc]."""
    if idx.tz is None or str(idx.tz) != "UTC":
        idx = ensure_utc_index(idx)
    mask = pd.Series(True, index=idx)
    start_utc = ensure_utc(start_utc)
    end_utc = ensure_utc(end_utc)
    if start_utc is not None:
        mask &= idx >= start_utc
    if end_utc is not None:
        mask &= idx <= end_utc
    return idx[mask.values]


def window_slice(df: pd.DataFrame,
                 start_utc: Optional[pd.Timestamp],
                 end_utc: Optional[pd.Timestamp]) -> pd.DataFrame:
    """
    Slice a DataFrame with datetime index to [start_utc, end_utc] (inclusive).
    Index is normalized to UTC if needed. Returns a new (possibly empty) frame.
    """
    if df is None or df.empty:
        return df
    if not isinstance(df.index, pd.DatetimeIndex):
        try:
            df = df.copy()
            df.index = pd.to_datetime(df.index, errors="coerce")
        except Exception:
            LOG.warning("window_slice: non-datetime index; returning original df")
            return df

    df = df.copy()
    df.index = ensure_utc_index(df.index)

    s = ensure_utc(start_utc)
    e = ensure_utc(end_utc)

    if s is not None:
        df = df[df.index >= s]
    if e is not None:
        df = df[df.index <= e]

    return df


# ---------- Parsing helpers ----------

def safe_to_datetime(obj) -> Optional[pd.Timestamp]:
    """
    Best-effort parse â†’ tz-aware UTC Timestamp or None.
    """
    return ensure_utc(obj)


def parse_iso_utc(s: str) -> Optional[pd.Timestamp]:
    """
    Parse common ISO-like strings directly to UTC Timestamp.
    """
    return ensure_utc(s)

