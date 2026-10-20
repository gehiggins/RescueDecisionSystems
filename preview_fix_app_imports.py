"""Compatibility entry point for previewing the deferred import migration."""

import argparse
from pathlib import Path

from fix_app_imports import DEFAULT_PROJECT_ROOT, fix_imports


def preview_import_fixes(project_root=DEFAULT_PROJECT_ROOT, output=print):
    return fix_imports(project_root, apply=False, output=output)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Preview app import namespace changes")
    parser.add_argument("--root", type=Path, default=DEFAULT_PROJECT_ROOT)
    args = parser.parse_args(argv)
    preview_import_fixes(args.root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
