import logging
from app import create_app
from flask import request

app = create_app()

# ✅ Log every request Flask processes
@app.before_request
def log_request():
    print(f"🔹 Flask received request: {request.method} {request.path}")

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    print("✅ Flask is running in DEBUG mode. Logging all requests.")
    app.run(debug=True)
