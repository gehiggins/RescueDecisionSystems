import pandas as pd

def build_sat_overlay_geojson(sat_overlay_df: pd.DataFrame, alert_df: pd.DataFrame) -> dict:
    """
    Convert satellite overlay DataFrame (+ alert_df for temporary center) into a GeoJSON FeatureCollection.
    Each feature is a Point at the alert A-side center, with satellite footprint and metadata in properties.

    Properties included:
      name, type, owner, constellation, altitude_km, footprint_radius_km, snapshot_utc

    Returns:
      dict: GeoJSON FeatureCollection
    """
    # Temporary center: use alert_df A-side (position_lat_dd_a, position_lon_dd_a)
    if alert_df is not None and not alert_df.empty:
        center_lat = alert_df.iloc[0].get("position_lat_dd_a")
        center_lon = alert_df.iloc[0].get("position_lon_dd_a")
    else:
        center_lat, center_lon = None, None

    features = []
    for _, row in sat_overlay_df.iterrows():
        # When TLEs are available, use row["lat_dd"], row["lon_dd"] for subpoint
        lat = row.get("lat_dd", center_lat)
        lon = row.get("lon_dd", center_lon)
        if pd.isna(lat) or pd.isna(lon):
            lat, lon = center_lat, center_lon

        properties = {
            "name": row.get("name"),
            "type": row.get("type"),
            "owner": row.get("owner"),
            "constellation": row.get("constellation"),
            "altitude_km": row.get("altitude_km"),
            "footprint_radius_km": row.get("footprint_radius_km"),
            "snapshot_utc": row.get("snapshot_utc"),
        }

        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [lon, lat]
            },
            "properties": properties
        }
        features.append(feature)

    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    return geojson