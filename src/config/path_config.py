r"""
C:\Economy\Invest\TrendMaster\src\config\path_config.py
Centralized path configuration for the TrendMaster project.

This module:
- Detects the project root reliably (directory containing 'src')
- Defines all important project paths
- Optionally adjusts sys.path in development environments
"""

from pathlib import Path
from functools import lru_cache
import sys
import logging

_logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# Project root detection
# ---------------------------------------------------------

@lru_cache(maxsize=1)
def _find_project_root(start: Path | None = None) -> Path:
    """
    Walk upward until we find a directory containing 'src'.

    This approach is:
    - Simple and predictable
    - Fast (no directory scanning)
    - Robust across dev, CI, and most runtime environments
    """
    start = (start or Path(__file__)).resolve()

    for parent in [start] + list(start.parents):
        try:
            if (parent / "src").is_dir():
                return parent
        except (PermissionError, OSError):
            continue

    raise RuntimeError("Could not locate project root containing 'src' directory")


PROJECT_ROOT: Path = _find_project_root()


# ---------------------------------------------------------
# Path constants
# ---------------------------------------------------------

SRC_PATH: Path = PROJECT_ROOT / "src"
GUI_PATH: Path = SRC_PATH / "gui"

DATA_PATH: Path = PROJECT_ROOT / "data"
CSV_PATH: Path = DATA_PATH / "CSV"
API_PATH: Path = DATA_PATH / "API"
CACHE_PATH: Path = DATA_PATH / "cache"

ENV_PATH: Path = PROJECT_ROOT / ".env"
LOGS_PATH: Path = PROJECT_ROOT / "logs"
CONFIG_PATH: Path = PROJECT_ROOT / "config"


# ---------------------------------------------------------
# Optional sys.path adjustments (dev/test only)
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
    Enable sys.path injection only in development environments.

    Disabled automatically in frozen/packaged apps.
    """
    is_frozen = getattr(sys, "frozen", False)

    if not is_frozen:
        _add_to_syspath(PROJECT_ROOT)
        _add_to_syspath(SRC_PATH)


# Apply configuration once at import
_configure_syspath()


# ---------------------------------------------------------
# Public API
# ---------------------------------------------------------

__all__ = [
    "PROJECT_ROOT",
    "SRC_PATH",
    "GUI_PATH",
    "DATA_PATH",
    "CSV_PATH",
    "API_PATH",
    "CACHE_PATH",
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
    print("CACHE_PATH:", CACHE_PATH)
    print("ENV_PATH:", ENV_PATH)
    print("LOGS_PATH:", LOGS_PATH)
    print("CONFIG_PATH:", CONFIG_PATH)
    print("sys.path (head):", sys.path[:5])


if __name__ == "__main__":
    _debug_print()