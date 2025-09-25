import os, sys
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
FLASK_APP_DIR = os.path.join(REPO_ROOT, "flask_app")
if FLASK_APP_DIR not in sys.path:
    sys.path.insert(0, FLASK_APP_DIR)
