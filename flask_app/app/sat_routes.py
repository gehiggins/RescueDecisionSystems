from flask import Blueprint, request, jsonify
from flask_app.app.sat_pipeline import build_sat_overlay_df
from flask_app.app.gis_sat_overlay import build_sat_overlay_geojson
from flask_app.app.pipeline_controller import get_alert_df_for_site  # Assumes you have this helper

routes_sat = Blueprint("routes_sat", __name__)

@routes_sat.route("/sat/cover", methods=["GET"])
def sat_cover():
    site_id = request.args.get("site_id")
    if not site_id:
        return jsonify({"error": "Missing site_id"}), 400

    # Obtain current alert_df for the site (assumes helper exists)
    alert_df = get_alert_df_for_site(site_id)
    if alert_df is None or alert_df.empty:
        return jsonify({"error": "No alert data found for site"}), 404

    # Build satellite overlay DataFrame (MVP: reporting scope, LEO only, no TLE)
    sat_overlay_df = build_sat_overlay_df(
        alert_df,
        scope="reporting",
        types=("LEO",),
        use_tle=False
    )

    # Convert to GeoJSON for the map overlay
    geojson = build_sat_overlay_geojson(sat_overlay_df, alert_df)
    return jsonify(geojson)