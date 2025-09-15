from .types import Context, DataPlan

def make_wx_data_plan(
    ctx: Context,
    mode: str = "auto",                 # "auto" | "force" | "off"
    coastal_buffer_km: float = 10.0,    # safety margin for jagged shorelines
    default_window_h: int = 6,
    default_radius_km: float = 75.0,
    default_station_limit: int = 5,
) -> DataPlan:
    """
    Turn Context into a Weather DataPlan.
    Decision is centralized here (fetchers should NOT decide).

    Rules:
      - force  => include_marine=True
      - off    => include_marine=False
      - auto   =>
          * if error_radius_km >= 10 → include_marine
          * else if both coast_distance_km & error_radius_km known:
                include_marine if coast_distance_km <= (error_radius_km + coastal_buffer_km)
          * else conservative default → include_marine=False
    """
    rationale: list[str] = []

    # 1) Explicit modes
    if mode == "force":
        rationale.append("mode=force → include_marine=True")
        return DataPlan(
            include_marine=True,
            time_window_hours=default_window_h,
            spatial_radius_km=default_radius_km,
            station_limit=default_station_limit,
            rationale=rationale,
        )

    if mode == "off":
        rationale.append("mode=off → include_marine=False")
        return DataPlan(
            include_marine=False,
            time_window_hours=default_window_h,
            spatial_radius_km=default_radius_km,
            station_limit=default_station_limit,
            rationale=rationale,
        )

    # 2) AUTO mode
    include = False

    # Big uncertainty → include marine
    if (ctx.error_radius_km or 0) >= 10:
        rationale.append(f"error_radius_km={ctx.error_radius_km} ≥ 10 → include marine")
        include = True
    # Known distances → include if uncertainty could touch water
    elif ctx.coast_distance_km is not None and ctx.error_radius_km is not None:
        threshold = (ctx.error_radius_km or 0) + coastal_buffer_km
        if ctx.coast_distance_km <= threshold:
            rationale.append(
                f"coast_distance_km={ctx.coast_distance_km} within error+buffer ({ctx.error_radius_km}+{coastal_buffer_km})"
            )
            include = True
        else:
            rationale.append("coast_distance outside error+buffer → include_marine=False")
            include = False
    else:
        # Unknown proximity & small error → conservative False
        rationale.append("coast_distance unknown and small error → include_marine=False")

    return DataPlan(
        include_marine=include,
        time_window_hours=default_window_h,
        spatial_radius_km=default_radius_km,
        station_limit=default_station_limit,
        rationale=rationale,
    )
