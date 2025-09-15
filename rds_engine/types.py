from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class Context:
    """
    Operational context derived from an alert/location.
    """
    lat: float
    lon: float
    error_radius_km: Optional[float] = None
    coast_distance_km: Optional[float] = None   # fill later when you add coastline distance
    zone: Optional[str] = None                  # e.g., inland / near_shore_onshore / coastal_waters / offshore
    notes: List[str] = field(default_factory=list)

@dataclass
class DataPlan:
    """
    What data we plan to fetch, with simple parameters and rationale.
    """
    include_marine: bool
    time_window_hours: int = 6
    spatial_radius_km: float = 75.0
    station_limit: int = 5
    providers_required: List[str] = field(default_factory=lambda: ["open_meteo"])
    providers_optional: List[str] = field(default_factory=lambda: ["meteostat"])
    rationale: List[str] = field(default_factory=list)
