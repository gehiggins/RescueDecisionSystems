"""Flask application factory for the RDS web interface."""

from pathlib import Path
import sys

from flask import Flask


# Older RDS modules import ``app.*`` because the application was originally run
# from inside ``flask_app``. Keep that name available while the modules are
# migrated to package-relative imports.
sys.modules.setdefault("app", sys.modules[__name__])


def create_app(config=None):
    """Create the RDS Flask application without starting a server."""
    flask_root = Path(__file__).resolve().parent.parent
    repository_root = flask_root.parent

    application = Flask(
        __name__,
        static_folder=str(flask_root / "static"),
        template_folder=str(flask_root / "templates"),
    )
    application.config.from_mapping(
        # Keep accidental file uploads or runaway browser requests from being
        # loaded into memory as alert text. Real SARSAT messages are far
        # smaller, and both limits remain configurable for controlled testing.
        MAX_CONTENT_LENGTH=256 * 1024,
        RDS_MAX_ALERT_CHARS=100_000,
        RDS_MAPS_DIR=str(repository_root / "data" / "maps"),
    )
    if config:
        application.config.update(config)

    from .routes import main_bp

    application.register_blueprint(main_bp)

    @application.errorhandler(413)
    def request_too_large(_error):
        return {
            "error": "Request is too large to process as a SARSAT alert."
        }, 413

    return application
