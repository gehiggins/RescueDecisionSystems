"""Flask application factory for the RDS web interface."""

from pathlib import Path
import math
import sys

from flask import Flask, request


# Older RDS modules import ``app.*`` because the application was originally run
# from inside ``flask_app``. Keep that name available while the modules are
# migrated to package-relative imports.
sys.modules.setdefault("app", sys.modules[__name__])

_DEFAULT_MAX_CONTENT_LENGTH = 256 * 1024


def _positive_integer_setting(value, default):
    """Return a positive whole-number setting and whether fallback was used."""
    if not isinstance(value, bool):
        try:
            numeric = float(value)
        except (TypeError, ValueError, OverflowError):
            numeric = float("nan")
        if math.isfinite(numeric) and numeric >= 1 and numeric.is_integer():
            return int(numeric), False
    return default, True


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
        MAX_CONTENT_LENGTH=_DEFAULT_MAX_CONTENT_LENGTH,
        RDS_MAX_ALERT_CHARS=100_000,
        RDS_MAPS_DIR=str(repository_root / "data" / "maps"),
    )
    if config:
        application.config.update(config)
    configured_content_limit = application.config.get("MAX_CONTENT_LENGTH")
    (
        normalized_content_limit,
        used_content_limit_fallback,
    ) = _positive_integer_setting(
        configured_content_limit,
        _DEFAULT_MAX_CONTENT_LENGTH,
    )
    if used_content_limit_fallback:
        application.logger.warning(
            "Invalid MAX_CONTENT_LENGTH=%r; using %s",
            configured_content_limit,
            _DEFAULT_MAX_CONTENT_LENGTH,
        )
    application.config["MAX_CONTENT_LENGTH"] = normalized_content_limit

    from .routes import main_bp

    application.register_blueprint(main_bp)

    @application.after_request
    def set_response_safety_headers(response):
        # Generated maps are embedded by the same RDS origin, so SAMEORIGIN
        # prevents third-party framing without breaking the analysis page.
        response.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Referrer-Policy", "same-origin")
        if request.endpoint in {"main.process_alert", "main.serve_map"}:
            response.headers["Cache-Control"] = "no-store"
        return response

    @application.errorhandler(413)
    def request_too_large(_error):
        return {
            "error": "Request is too large to process as a SARSAT alert."
        }, 413

    return application
