"""
Smoke-test: verifies the database package exports the expected public API.
Run from the project root:
    python scripts/verify_database_package.py
"""

import sys
import importlib
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SRC_PATH = Path(__file__).parent.parent / "src"

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
# Path setup
# ---------------------------------------------------------------------------

def _ensure_src_on_path() -> None:
    src = str(_SRC_PATH)
    if src not in sys.path:
        sys.path.insert(0, src)

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
    _ensure_src_on_path()
    print(f"Using src path: {_SRC_PATH}")

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