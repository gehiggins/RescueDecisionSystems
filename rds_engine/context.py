from __future__ import annotations

import math

from .types import Context


def _coordinate(value, *, name: str, minimum: float, maximum: float) -> float:
    """Return one finite Earth coordinate or reject the context input."""
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a finite number")
    try:
        normalized = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be a finite number") from exc
    if not math.isfinite(normalized) or not minimum <= normalized <= maximum:
        raise ValueError(f"{name} is outside valid Earth bounds")
    return normalized


def _optional_distance_km(value) -> float | None:
    """Normalize optional context distances; invalid or negative means unknown."""
    if value is None or isinstance(value, bool):
        return None
    try:
        normalized = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(normalized) or normalized < 0:
        return None
    return normalized

def classify_zone_context(
    lat: float,
    lon: float,
    error_radius_km: float | None = None,
    coast_distance_km: float | None = None,
) -> Context:
    """
    Minimal, data-only context classifier.
    No I/O. No coastline dataset (yet). Sets a coarse zone label.

    Zones (coarse, easy to extend later):
      - "coastal_waters": coast_distance_km <= 3
      - "near_shore_onshore": 3 < coast_distance_km <= 15
      - "inland": coast_distance_km > 15
      - "unknown": coast_distance_km not provided
    """
    lat = _coordinate(lat, name="lat", minimum=-90.0, maximum=90.0)
    lon = _coordinate(lon, name="lon", minimum=-180.0, maximum=180.0)
    error_radius_km = _optional_distance_km(error_radius_km)
    coast_distance_km = _optional_distance_km(coast_distance_km)
    ctx = Context(
        lat=lat,
        lon=lon,
        error_radius_km=error_radius_km,
        coast_distance_km=coast_distance_km,
    )

    if coast_distance_km is None:
        ctx.zone = "unknown"
        ctx.notes.append("No coast distance available; zone=unknown")
        return ctx

    if coast_distance_km <= 3:
        ctx.zone = "coastal_waters"
    elif coast_distance_km <= 15:
        ctx.zone = "near_shore_onshore"
    else:
        ctx.zone = "inland"

    return ctx

