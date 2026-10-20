import importlib

import pytest


def test_database_smoke_module_import_has_no_side_effects(monkeypatch, capsys):
    monkeypatch.delenv("DATABASE_URL", raising=False)

    module = importlib.import_module("flask_app.app.schema_verification_script")

    assert callable(module.main)
    assert capsys.readouterr().out == ""


def test_database_smoke_check_requires_explicit_url(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    module = importlib.import_module("flask_app.app.schema_verification_script")

    with pytest.raises(SystemExit, match="DATABASE_URL must be set explicitly"):
        module.main()
