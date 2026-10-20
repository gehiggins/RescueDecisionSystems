"""Safety coverage for the deferred import namespace migration helper."""

from fix_app_imports import fix_imports
from preview_fix_app_imports import preview_import_fixes


def _fixture_tree(tmp_path):
    root = tmp_path / "flask_app"
    root.mkdir()
    source = root / "module.py"
    source.write_text(
        "from app.utils import helper\n"
        "# from app.comment import ignored\n"
        "TEXT = 'from app.string import ignored'\n",
        encoding="utf-8",
    )
    archive = root / "local archive"
    archive.mkdir()
    archived_source = archive / "old.py"
    archived_source.write_text("from app.old import value\n", encoding="utf-8")
    return root, source, archived_source


def test_import_helper_previews_without_writing(tmp_path):
    root, source, archived_source = _fixture_tree(tmp_path)
    original = source.read_text(encoding="utf-8")
    messages = []

    count = fix_imports(root, output=messages.append)

    assert count == 1
    assert source.read_text(encoding="utf-8") == original
    assert archived_source.read_text(encoding="utf-8") == "from app.old import value\n"
    assert any("would update" in message for message in messages)


def test_import_helper_requires_explicit_apply_and_only_changes_import_lines(tmp_path):
    root, source, _archived_source = _fixture_tree(tmp_path)

    count = fix_imports(root, apply=True, output=lambda _message: None)

    assert count == 1
    assert source.read_text(encoding="utf-8") == (
        "from flask_app.app.utils import helper\n"
        "# from app.comment import ignored\n"
        "TEXT = 'from app.string import ignored'\n"
    )


def test_compatibility_preview_delegates_without_writing(tmp_path):
    root, source, _archived_source = _fixture_tree(tmp_path)
    original = source.read_text(encoding="utf-8")

    assert preview_import_fixes(root, output=lambda _message: None) == 1
    assert source.read_text(encoding="utf-8") == original
