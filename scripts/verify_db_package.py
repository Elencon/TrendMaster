r"""
Smoke-test: verifies the database package exports the expected public API.

Run from anywhere:
    python scripts/verify_db_package.py
"""

import sys
import importlib
from pathlib import Path


# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------

def _ensure_src_on_path() -> Path:
    script_dir = Path(__file__).resolve().parent

    for parent in [script_dir, *script_dir.parents]:
        src_path = parent / "src"
        if src_path.is_dir():
            if str(src_path) not in sys.path:
                sys.path.insert(0, str(src_path))
            return src_path

    raise RuntimeError("Could not locate project root containing 'src' directory")


# ---------------------------------------------------------------------------
# Expected API
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
# Checks
# ---------------------------------------------------------------------------

def _check_required(module: object) -> list[str]:
    return [name for name in _REQUIRED_EXPORTS if not hasattr(module, name)]


def _check_removed(module: object) -> list[str]:
    return [name for name in _REMOVED_EXPORTS if hasattr(module, name)]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    try:
        src_path = _ensure_src_on_path()
        print(f"Using src path: {src_path}")
    except Exception as exc:
        print(f"❌ Failed to prepare import path: {exc}")
        sys.exit(1)

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