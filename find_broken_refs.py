"""
Static import checker.
Walks a directory tree, parses every .py file with AST, and reports any
top-level import that cannot be resolved in the current Python environment
or as a local module under src/.

Usage:
    python check_imports.py                        # uses _DEFAULT_PROJECT_DIR
    python check_imports.py C:/Economy/Invest/TrendMaster
"""

import ast
import importlib.util
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_PROJECT_DIR = Path("C:/Economy/Invest/TrendMaster")
_CHECKS = ["src", "tests"]

# Platform-specific stdlib modules that will not resolve on Windows but are
# valid imports on Unix — suppress these to avoid false positives.
_PLATFORM_SPECIFIC = frozenset({
    "resource",
    "termios",
    "fcntl",
    "grp",
    "pwd",
    "pty",
    "tty",
    "readline",
})

# ---------------------------------------------------------------------------
# Module resolution
# ---------------------------------------------------------------------------

def _find_module(name: str, src_path: Path) -> bool:
    """
    Return True if *name* is resolvable as:
      - a built-in module
      - a known platform-specific stdlib module
      - an installed package
      - a local module (.py file or package) anywhere under *src_path*
    """
    if name in sys.builtin_module_names:
        return True
    if name in _PLATFORM_SPECIFIC:
        return True
    try:
        if importlib.util.find_spec(name) is not None:
            return True
    except (ModuleNotFoundError, ValueError):
        pass
    # Search recursively under src — handles modules in sub-packages
    return (
        any(src_path.rglob(f"{name}.py"))
        or any(src_path.rglob(f"{name}/__init__.py"))
    )


def _prepare_path(project_dir: Path) -> None:
    """Insert project root and src onto sys.path."""
    for candidate in (project_dir, project_dir / "src"):
        entry = str(candidate)
        if entry not in sys.path:
            sys.path.insert(0, entry)


# ---------------------------------------------------------------------------
# AST scanning
# ---------------------------------------------------------------------------

BrokenRef = tuple[str, int, str]  # (file_path, line_number, module_name)


def _imports_from_file(path: Path, src_path: Path) -> list[BrokenRef]:
    """
    Parse *path* and return broken top-level imports.
    Relative imports are skipped — they require package context to resolve.
    """
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError as exc:
        print(f"  [parse error] {path}: {exc}")
        return []

    broken: list[BrokenRef] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root_name = alias.name.split(".")[0]
                if not _find_module(root_name, src_path):
                    broken.append((str(path), node.lineno, alias.name))

        elif isinstance(node, ast.ImportFrom):
            if node.level > 0:
                continue  # skip relative imports
            if node.module:
                root_name = node.module.split(".")[0]
                if not _find_module(root_name, src_path):
                    broken.append((str(path), node.lineno, node.module))

    return broken


def check_imports(directory: Path, src_path: Path) -> list[BrokenRef]:
    """Recursively scan all .py files under *directory* and return broken imports."""
    broken: list[BrokenRef] = []
    for path in sorted(directory.rglob("*.py")):
        broken.extend(_imports_from_file(path, src_path))
    return broken


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def _report(broken: list[BrokenRef]) -> None:
    if not broken:
        print("  ✅ No broken imports found.")
        return
    for file_path, lineno, module in broken:
        print(f"  ❌ {file_path}:{lineno} — cannot find module '{module}'")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(project_dir: Path = _DEFAULT_PROJECT_DIR) -> None:
    if not project_dir.is_dir():
        print(f"❌ Project directory not found: {project_dir}")
        sys.exit(1)

    print(f"Project root: {project_dir}")
    _prepare_path(project_dir)

    src_path = project_dir / "src"
    all_broken: list[BrokenRef] = []

    for subdir_name in _CHECKS:
        subdir = project_dir / subdir_name
        if not subdir.is_dir():
            print(f"\n[{subdir_name}] directory not found at {subdir}, skipping.")
            continue

        print(f"\nChecking {subdir} ...")
        broken = check_imports(subdir, src_path)
        _report(broken)
        all_broken.extend(broken)

    print()
    if all_broken:
        print(f"Found {len(all_broken)} broken import(s) across the project.")
        sys.exit(1)
    else:
        print("All imports resolved successfully.")
        sys.exit(0)


if __name__ == "__main__":
    project_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else _DEFAULT_PROJECT_DIR
    main(project_dir)
