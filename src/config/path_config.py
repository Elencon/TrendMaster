"""
Centralized path configuration for the entire TrendMaster project.

Features:
- Robust project root detection
- Clean path definitions
- Safe sys.path handling (dev-only)
- No duplication or side effects in production
"""

import sys
from pathlib import Path
from functools import lru_cache

# ---------------------------------------------------------
# Locate project root (directory containing "src")
# ---------------------------------------------------------

@lru_cache(maxsize=1)
def _find_project_root() -> Path:
    """
    Locate the project root by searching for a directory containing 'src'.

    Uses caching to avoid repeated filesystem traversal.
    """
    current = Path(__file__).resolve()

    for parent in current.parents:
        src_dir = parent / "src"

        # Optional: strengthen detection with pyproject.toml
        if src_dir.is_dir():
            return parent

    raise RuntimeError(
        "Could not locate project root containing 'src' directory"
    )


PROJECT_ROOT: Path = _find_project_root()

# ---------------------------------------------------------
# Define key paths
# ---------------------------------------------------------

SRC_PATH: Path = PROJECT_ROOT / "src"
GUI_PATH: Path = SRC_PATH / "gui"

DATA_PATH: Path = PROJECT_ROOT / "data"
CSV_PATH: Path = DATA_PATH / "CSV"
API_PATH: Path = DATA_PATH / "API"

ENV_PATH: Path = PROJECT_ROOT / ".env"

# Optional: future extensions
LOGS_PATH: Path = PROJECT_ROOT / "logs"
CONFIG_PATH: Path = PROJECT_ROOT / "config"

# ---------------------------------------------------------
# sys.path management (DEV / TEST ONLY)
# ---------------------------------------------------------

def _add_to_syspath(path: Path) -> None:
    """
    Safely add a path to sys.path (idempotent).
    """
    resolved = str(path.resolve())

    if resolved not in sys.path:
        sys.path.insert(0, resolved)


def _configure_syspath() -> None:
    """
    Configure sys.path for development and testing environments only.

    This is intentionally disabled in frozen/production builds.
    """
    is_frozen = getattr(sys, "frozen", False)
    is_pytest = "pytest" in sys.modules

    # Only enable in dev/test
    if not is_frozen:
        _add_to_syspath(PROJECT_ROOT)
        _add_to_syspath(SRC_PATH)

        # Optional — enable only if needed
        # _add_to_syspath(GUI_PATH)


# Apply configuration once at import
_configure_syspath()

# ---------------------------------------------------------
# Public API (optional but clean)
# ---------------------------------------------------------

__all__ = [
    "PROJECT_ROOT",
    "SRC_PATH",
    "GUI_PATH",
    "DATA_PATH",
    "CSV_PATH",
    "API_PATH",
    "ENV_PATH",
    "LOGS_PATH",
    "CONFIG_PATH",
]

# ---------------------------------------------------------
# Debug / CLI usage
# ---------------------------------------------------------

def _debug_print() -> None:
    print("PROJECT_ROOT:", PROJECT_ROOT)
    print("SRC_PATH:", SRC_PATH)
    print("GUI_PATH:", GUI_PATH)
    print("DATA_PATH:", DATA_PATH)
    print("CSV_PATH:", CSV_PATH)
    print("API_PATH:", API_PATH)
    print("ENV_PATH:", ENV_PATH)
    print("LOGS_PATH:", LOGS_PATH)
    print("CONFIG_PATH:", CONFIG_PATH)
    print("sys.path (head):", sys.path[:5], "...")


if __name__ == "__main__":
    _debug_print()