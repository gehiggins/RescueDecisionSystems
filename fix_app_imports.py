"""Preview or explicitly apply the deferred ``app`` import migration."""

import argparse
import os
from pathlib import Path
import re
import tempfile


REPOSITORY_ROOT = Path(__file__).resolve().parent
DEFAULT_PROJECT_ROOT = REPOSITORY_ROOT / "flask_app"
EXCLUDED_PARTS = {"local archive", ".ipynb_checkpoints", "0_archive"}
IMPORT_PATTERN = re.compile(r"^(?P<indent>[ \t]*)from app\.", re.MULTILINE)


def _python_files(project_root):
    root = Path(project_root).resolve()
    if not root.is_dir():
        raise ValueError(f"project root is not a directory: {root}")
    for path in sorted(root.rglob("*.py")):
        if any(part in EXCLUDED_PARTS for part in path.relative_to(root).parts):
            continue
        yield path


def scan_import_fixes(project_root=DEFAULT_PROJECT_ROOT):
    """Return proposed line-level changes without modifying any file."""
    proposed = {}
    for path in _python_files(project_root):
        lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
        changes = []
        for line_number, line in enumerate(lines, start=1):
            replacement = IMPORT_PATTERN.sub(
                r"\g<indent>from flask_app.app.", line, count=1
            )
            if replacement != line:
                changes.append((line_number, line.rstrip(), replacement.rstrip()))
        if changes:
            proposed[path] = changes
    return proposed


def _atomic_write(path, content):
    """Replace one source file while retaining its permission bits."""
    original_mode = path.stat().st_mode
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary.write(content)
            temporary_path = Path(temporary.name)
        os.chmod(temporary_path, original_mode)
        os.replace(temporary_path, path)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()


def fix_imports(project_root=DEFAULT_PROJECT_ROOT, apply=False, output=print):
    """Preview changes by default; write only when ``apply`` is explicitly true."""
    proposed = scan_import_fixes(project_root)
    for path, changes in proposed.items():
        output(f"{path}")
        for line_number, old, new in changes:
            output(f"  line {line_number}: {old}")
            output(f"              -> {new}")
        if apply:
            content = path.read_text(encoding="utf-8")
            _atomic_write(
                path,
                IMPORT_PATTERN.sub(r"\g<indent>from flask_app.app.", content),
            )

    action = "updated" if apply else "would update"
    output(f"{len(proposed)} file(s) {action}")
    return len(proposed)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Preview the deferred app-import namespace migration"
    )
    parser.add_argument("--root", type=Path, default=DEFAULT_PROJECT_ROOT)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write the displayed changes; preview is the default",
    )
    args = parser.parse_args(argv)
    fix_imports(args.root, apply=args.apply)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
