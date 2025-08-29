import os
from flask import Flask, send_from_directory
from .parser_sarsat_msg import *
from .utils import *
# (Import other necessary files if needed)


def create_app():
    """Initialize Flask app"""
    app = Flask(
        __name__, 
        template_folder=os.path.join(os.getcwd(), "templates"),
        static_folder=os.path.join(os.getcwd(), "static")  # ✅ Ensures Flask serves static files
    )

    from flask_app.app.routes import main_bp
    app.register_blueprint(main_bp)

    # ✅ Ensure Flask knows about the maps directory
    @app.route('/maps/<path:filename>')
    def serve_maps(filename):
        maps_dir = os.path.join(os.getcwd(), "maps")
        return send_from_directory(maps_dir, filename)

    return app
