"""Development and console entry point for the RDS Flask application."""

import logging

from flask import request

from flask_app.app import create_app

app = create_app()

# ✅ Log every request Flask processes
@app.before_request
def log_request():
    print(f"🔹 Flask received request: {request.method} {request.path}")

def main():
    """Run the local development server."""
    logging.basicConfig(level=logging.DEBUG)
    print("✅ Flask is running in DEBUG mode. Logging all requests.")
    app.run(debug=True)


if __name__ == '__main__':
    main()
