"""Development and console entry point for the RDS Flask application."""

import argparse
import logging

from flask import request

from flask_app.app import create_app

app = create_app()

# ✅ Log every request Flask processes
@app.before_request
def log_request():
    app.logger.debug("Flask request: %s %s", request.method, request.path)

def _port_number(value):
    port = int(value)
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("port must be between 1 and 65535")
    return port


def main(argv=None):
    """Run the local development server with debug behavior explicitly opted in."""
    parser = argparse.ArgumentParser(description="Run the local RDS web application")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=_port_number, default=5000)
    parser.add_argument(
        "--debug",
        action="store_true",
        help="enable Flask debug mode and the development reloader",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO)
    app.logger.info(
        "Flask development server is starting on %s:%s (debug=%s).",
        args.host,
        args.port,
        args.debug,
    )
    app.run(
        host=args.host,
        port=args.port,
        debug=args.debug,
        use_reloader=args.debug,
    )


if __name__ == '__main__':
    main()
