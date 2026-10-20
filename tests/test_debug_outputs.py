from pathlib import Path

import pandas as pd

from flask_app.app.debug_outputs import dump_dataframe


def test_dataframe_debug_dump_is_disabled_by_default(monkeypatch, tmp_path):
    monkeypatch.delenv("RDS_DEBUG_DUMP", raising=False)
    monkeypatch.setenv("RDS_DEBUG_DIR", str(tmp_path))

    result = dump_dataframe(pd.DataFrame([{"site_id": "98372"}]), "alert.csv")

    assert result is None
    assert not (tmp_path / "alert.csv").exists()


def test_dataframe_debug_dump_uses_configured_directory(monkeypatch, tmp_path):
    monkeypatch.setenv("RDS_DEBUG_DUMP", "1")
    monkeypatch.setenv("RDS_DEBUG_DIR", str(tmp_path))

    result = dump_dataframe(pd.DataFrame([{"site_id": "98372"}]), "alert.csv")

    assert result == Path(tmp_path) / "alert.csv"
    assert result.read_text(encoding="utf-8") == "site_id\n98372\n"
