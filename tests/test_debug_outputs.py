from pathlib import Path

import pandas as pd
import pytest

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


@pytest.mark.parametrize("filename", ["../alert.csv", "nested/alert.csv"])
def test_dataframe_debug_dump_rejects_paths_outside_configured_directory(
    monkeypatch, tmp_path, filename
):
    monkeypatch.setenv("RDS_DEBUG_DUMP", "1")
    monkeypatch.setenv("RDS_DEBUG_DIR", str(tmp_path / "debug"))

    with pytest.raises(ValueError, match="plain filename"):
        dump_dataframe(pd.DataFrame([{"site_id": "98372"}]), filename)

    assert not (tmp_path / "alert.csv").exists()


def test_dataframe_debug_dump_rejects_symbolic_link_target(monkeypatch, tmp_path):
    output_dir = tmp_path / "debug"
    output_dir.mkdir()
    outside = tmp_path / "outside.csv"
    outside.write_text("preserve me", encoding="utf-8")
    (output_dir / "alert.csv").symlink_to(outside)
    monkeypatch.setenv("RDS_DEBUG_DUMP", "1")
    monkeypatch.setenv("RDS_DEBUG_DIR", str(output_dir))

    with pytest.raises(ValueError, match="symbolic link"):
        dump_dataframe(pd.DataFrame([{"site_id": "98372"}]), "alert.csv")

    assert outside.read_text(encoding="utf-8") == "preserve me"
