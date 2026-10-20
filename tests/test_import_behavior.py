import importlib
import sys


def test_shared_import_module_does_not_print_during_import(capsys):
    from flask_app.app import setup_imports

    capsys.readouterr()
    importlib.reload(setup_imports)

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_noaa_diagnostic_import_does_not_reach_network(monkeypatch):
    calls = []

    def unexpected_request(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("NOAA diagnostic made a request during import")

    monkeypatch.setattr("requests.get", unexpected_request)
    sys.modules.pop("flask_app.test_noaa_api", None)

    importlib.import_module("flask_app.test_noaa_api")

    assert calls == []


def test_weather_health_import_does_not_configure_global_logging(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "logging.basicConfig", lambda *args, **kwargs: calls.append((args, kwargs))
    )

    module = importlib.import_module("flask_app.app.health_wx")
    importlib.reload(module)

    assert calls == []
