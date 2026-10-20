"""Coastline-distance calculation for validated alert coordinates."""

from pathlib import Path
import logging
import math

import geopandas as gpd
from shapely.geometry import Point, box


LOG = logging.getLogger(__name__)
DEFAULT_COASTLINE_PATH = (
    Path(__file__).resolve().parents[2]
    / "data"
    / "shapefiles"
    / "coastline"
    / "ne_10m_coastline.shp"
)


def _validated_coordinates(latitude, longitude):
    try:
        latitude = float(latitude)
        longitude = float(longitude)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("Latitude and longitude must be finite numbers") from exc
    if (
        not math.isfinite(latitude)
        or not math.isfinite(longitude)
        or not -90 <= latitude <= 90
        or not -180 <= longitude <= 180
    ):
        raise ValueError("Latitude or longitude is outside valid Earth bounds")
    return latitude, longitude


def _nearby_coastline(coastline, latitude, longitude):
    """Select a bounded geographic neighborhood, including dateline wrap."""
    coastline = coastline.to_crs("EPSG:4326")
    for radius in (5, 15, 30, 60, 90):
        south = max(-90, latitude - radius)
        north = min(90, latitude + radius)
        west = longitude - radius
        east = longitude + radius
        if west < -180:
            windows = (
                box(west + 360, south, 180, north),
                box(-180, south, east, north),
            )
        elif east > 180:
            windows = (
                box(west, south, 180, north),
                box(-180, south, east - 360, north),
            )
        else:
            windows = (box(west, south, east, north),)
        mask = coastline.geometry.apply(
            lambda geometry: (
                geometry is not None
                and not geometry.is_empty
                and any(geometry.intersects(window) for window in windows)
            )
        )
        if mask.any():
            return coastline.loc[mask]
    return coastline


def compute_distance_to_shore(latitude, longitude, coastline_path=None):
    """Return the nearest committed coastline distance in kilometres.

    The local azimuthal-equidistant projection is centred on the alert. This
    keeps the distance calculation in metres instead of treating geographic
    degrees as if they had one constant length everywhere on Earth.
    """
    latitude, longitude = _validated_coordinates(latitude, longitude)
    path = Path(coastline_path or DEFAULT_COASTLINE_PATH).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Coastline shapefile not found: {path}")

    coastline = gpd.read_file(path)
    if coastline.empty:
        raise ValueError(f"Coastline shapefile contains no geometry: {path}")
    if coastline.crs is None:
        raise ValueError(f"Coastline shapefile has no coordinate system: {path}")

    coastline = _nearby_coastline(coastline, latitude, longitude)
    local_crs = (
        f"+proj=aeqd +lat_0={latitude} +lon_0={longitude} "
        "+datum=WGS84 +units=m +no_defs"
    )
    coastline_local = coastline.to_crs(local_crs)
    # A world dataset can contain geometry at the projection's antipode, where
    # coordinates are infinite. Remove those remote artifacts before asking
    # Shapely for the nearest projected distance.
    finite_geometry = coastline_local.geometry.apply(
        lambda geometry: (
            geometry is not None
            and not geometry.is_empty
            and geometry.is_valid
            and all(math.isfinite(value) for value in geometry.bounds)
        )
    )
    coastline_local = coastline_local.loc[finite_geometry]
    if coastline_local.empty:
        raise ValueError("No coastline geometry could be projected for the alert")
    alert_point = gpd.GeoSeries(
        [Point(longitude, latitude)], crs="EPSG:4326"
    ).to_crs(local_crs).iloc[0]
    distance_metres = coastline_local.geometry.distance(alert_point).min()
    if not math.isfinite(distance_metres):
        raise ValueError("No valid coastline geometry was available")

    distance_km = float(distance_metres) / 1000.0
    LOG.debug("Nearest coastline is %.2f km from the alert", distance_km)
    return distance_km
