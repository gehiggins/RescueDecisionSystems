import importlib

import pytest


def test_sql_models_import_without_database_url(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    sql_models = importlib.import_module("flask_app.app.sql_models")
    sql_models = importlib.reload(sql_models)

    assert sql_models.engine is None
    assert sql_models.SessionLocal is None


def test_database_operations_require_explicit_configuration(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    database = importlib.import_module("flask_app.app.database")
    database.sql_models.engine = None
    database.sql_models.SessionLocal = None

    with pytest.raises(RuntimeError, match="DATABASE_URL is required"):
        database._new_session()


def test_database_can_be_configured_explicitly(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    sql_models = importlib.import_module("flask_app.app.sql_models")

    engine = sql_models.configure_database("sqlite+pysqlite:///:memory:")
    session = sql_models.SessionLocal()
    try:
        assert str(engine.url) == "sqlite+pysqlite:///:memory:"
    finally:
        session.close()
        engine.dispose()
        sql_models.engine = None
        sql_models.SessionLocal = None
