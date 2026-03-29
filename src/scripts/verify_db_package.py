"""
Smoke-test: verifies the database package exports the expected public API.
Run from anywhere:
    python scripts/verify_db_package.py
"""

import sys
import importlib
from pathlib import Path
import path_config 
# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_REQUIRED_EXPORTS = [
    "DatabaseManager",
    "SchemaManager",
    "SCHEMA_DEFINITIONS",
    "TABLE_COLUMNS",
    "APIDataFetcher",
]

_REMOVED_EXPORTS = [
    "DataProcessor",
]

# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------

def _find_project_root() -> Path:
    """
    Walk upward from this file until we find a directory containing 'src'.
    This makes the script runnable from ANY location.
    """
    p = Path(__file__).resolve()
    for parent in p.parents:
        if (parent / "src").exists():
            return parent
    raise RuntimeError("Could not locate project root containing 'src' directory")


def _ensure_src_on_path() -> Path:
    project_root = _find_project_root()
    src_path = project_root / "src"

    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

    return src_path

# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

def _check_required(module: object) -> list[str]:
    """Return names that are missing from *module*."""
    return [name for name in _REQUIRED_EXPORTS if not hasattr(module, name)]


def _check_removed(module: object) -> list[str]:
    """Return names that should have been removed but are still present."""
    return [name for name in _REMOVED_EXPORTS if hasattr(module, name)]

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    src_path = _ensure_src_on_path()
    print(f"Using src path: {src_path}")

    try:
        database = importlib.import_module("database")
        print("✅ Successfully imported database package")
    except Exception as exc:
        print(f"❌ Import failed: {exc}")
        sys.exit(1)

    failed = False

    missing = _check_required(database)
    if missing:
        print(f"❌ Missing expected exports: {missing}")
        failed = True
    else:
        print("✅ All expected exports are present")

    still_present = _check_removed(database)
    if still_present:
        print(f"❌ Exports that should have been removed are still present: {still_present}")
        failed = True
    else:
        print("✅ All removed exports are correctly absent")

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
