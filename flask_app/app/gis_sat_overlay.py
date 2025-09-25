import pandas as pd

def build_sat_overlay_geojson(sat_overlay_df: pd.DataFrame) -> dict:
    feats = []
    for _, r in sat_overlay_df.iterrows():
        # Core point (subpoint)
        feats.append({
            "type": "Feature",
            "geometry": {"type": "Point",
                         "coordinates": [float(r["lon_dd"]), float(r["lat_dd"])]},
            "properties": {
                "sat_name": r.get("sat_name"),
                "norad_id": r.get("norad_id"),
                "at_time_utc": str(r.get("at_time_utc")),
                "alt_km": r.get("alt_km"),
                "footprint_radius_km": r.get("footprint_radius_km"),
                "radius_m": float(r["footprint_radius_km"]) * 1000.0 if pd.notna(r.get("footprint_radius_km")) else None,
                "tle_epoch_utc": str(r.get("tle_epoch_utc")),
                "tle_age_hours": r.get("tle_age_hours"),
                "source": r.get("source"),
                "track_window_forward_min": r.get("track_window_forward_min"),
                "track_start_utc": str(r.get("track_start_utc")),
                "track_end_utc": str(r.get("track_end_utc")),
                "variant": r.get("_variant"),
                "distance_km": r.get("distance_km"),
                "_feature": "sat_subpoint"
            }
        })
        # Optional forward track
        if isinstance(r.get("track_coords"), (list, tuple)) and len(r["track_coords"]) > 1:
            feats.append({
                "type": "Feature",
                "geometry": {"type": "LineString",
                             "coordinates": r["track_coords"]},
                "properties": {
                    "sat_name": r.get("sat_name"),
                    "_feature": "sat_track"
                }
            })
        # Optional next pass marker
        npm = r.get("next_pass_marker")
        if isinstance(npm, dict) and pd.notna(npm.get("lat_dd")) and pd.notna(npm.get("lon_dd")):
            feats.append({
                "type": "Feature",
                "geometry": {"type": "Point",
                             "coordinates": [float(npm["lon_dd"]), float(npm["lat_dd"])]},
                "properties": {
                    "label": f'Next pass {str(npm.get("time_utc"))}',
                    "elevation_max_deg": npm.get("elevation_max_deg"),
                    "_feature": "next_pass"
                }
            })
    return {"type": "FeatureCollection", "features": feats}