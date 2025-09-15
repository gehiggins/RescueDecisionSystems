from .types import Context

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

