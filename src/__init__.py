r"""
C:\Economy\Invest\TrendMaster\src\__init__.py
Centralized path configuration for the TrendMaster project.

This module provides reliable project paths with minimal side effects.
It supports clean development workflows while staying friendly to proper packaging.
"""

from pathlib import Path
import sys
import warnings

# ---------------------------------------------------------
# Locate project root
# ---------------------------------------------------------
def _find_project_root() -> Path:
    """Find project root by walking upward until a 'src' directory is found."""
    current = Path(__file__).resolve().parent
    for parent in [current, *current.parents]:
        if (parent / "src").is_dir():
            return parent

    warnings.warn(
        "Could not reliably detect project root containing 'src' directory. "
        "Falling back to parent of this file.",
        stacklevel=2,
    )
    return current.parent


PROJECT_ROOT: Path = _find_project_root()

# ---------------------------------------------------------
# Define key paths
# ---------------------------------------------------------
SRC_PATH: Path = PROJECT_ROOT / "src"
GUI_PATH: Path = SRC_PATH / "gui"
DATA_PATH: Path = PROJECT_ROOT / "data"
CSV_PATH: Path = DATA_PATH / "CSV"
API_PATH: Path = DATA_PATH / "API"

# ---------------------------------------------------------
# Ensure data directories exist (safe in development)
# ---------------------------------------------------------
def _ensure_dirs(*paths: Path) -> None:
    """Create directories if they don't exist."""
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)


_ensure_dirs(DATA_PATH, CSV_PATH, API_PATH)

# ---------------------------------------------------------
# Minimal sys.path adjustment — only for development convenience
# ---------------------------------------------------------
def _add_to_syspath(path: Path) -> None:
    """Add path to sys.path if not already present."""
    resolved = str(path.resolve())
    if resolved not in sys.path:
        sys.path.insert(0, resolved)


# Only adjust sys.path in common development scenarios
if __name__ == "__main__" or "pytest" in sys.modules or not getattr(sys, "frozen", False):
    _add_to_syspath(PROJECT_ROOT)
    _add_to_syspath(SRC_PATH)
    # Note: Do not add GUI_PATH unless you have a strong reason

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
]

# ---------------------------------------------------------
# Debug output when run directly
# ---------------------------------------------------------
if __name__ == "__main__":
    print("PROJECT_ROOT :", PROJECT_ROOT)
    print("SRC_PATH     :", SRC_PATH)
    print("DATA_PATH    :", DATA_PATH)
    print("CSV_PATH     :", CSV_PATH)
    print("API_PATH     :", API_PATH)
    print("sys.path[:3] :", sys.path[:3])