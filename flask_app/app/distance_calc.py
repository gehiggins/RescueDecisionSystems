import geopandas as gpd
from geopy.distance import geodesic
import os

def compute_distance_to_shore(latitude, longitude):
    """Computes the distance from a given point to the nearest shoreline."""
    print("🔍 Computing distance to shore...")
    
    # ✅ Load coastline shapefile
    coastline_path = os.path.join(os.getcwd(), "data", "coastline", "ne_10m_coastline.shp")
    coastline = gpd.read_file(coastline_path)
    
    # ✅ Convert alert location into a GeoSeries point
    alert_point = gpd.GeoSeries([gpd.points_from_xy([longitude], [latitude])[0]])
    
    # ✅ Compute distances to all coastlines and get the minimum
    coastline["distance_km"] = coastline.geometry.distance(alert_point.iloc[0]) * 111  # Approx conversion to km
    min_distance_km = coastline["distance_km"].min()
    
    print(f"✅ Distance to shore: {min_distance_km:.2f} km")
    return min_distance_km
