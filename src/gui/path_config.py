"""
Centralized path configuration for the entire TrendMaster project.

This module:
- Detects the project root dynamically
- Exposes all important project paths
- Ensures project root and src/ are added to sys.path
- Allows imports like:
      from src.database import ...
  and:
      from database import ...
"""

import sys
from pathlib import Path

# ---------------------------------------------------------
# Locate project root (directory containing "src")
# ---------------------------------------------------------

def _find_project_root() -> Path:
    p = Path(__file__).resolve()
    for parent in p.parents:
        if (parent / "src").exists():
            return parent
    raise RuntimeError("Could not locate project root containing 'src' directory")

PROJECT_ROOT = _find_project_root()

# ---------------------------------------------------------
# Define key paths
# ---------------------------------------------------------

SRC_PATH = PROJECT_ROOT / "src"
GUI_PATH = SRC_PATH / "gui"
DATA_PATH = PROJECT_ROOT / "data"
CSV_PATH = DATA_PATH / "CSV"
API_PATH = DATA_PATH / "API"

# ---------------------------------------------------------
# Add paths to sys.path
# ---------------------------------------------------------

def _add_to_syspath(path: Path) -> None:
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

# Add both project root and src/
# This allows BOTH import styles:
#   from src.database import ...
#   from database import ...
_add_to_syspath(PROJECT_ROOT)
_add_to_syspath(SRC_PATH)

# Optional: add GUI folder if you want direct imports
_add_to_syspath(GUI_PATH)

# ---------------------------------------------------------
# Debug print (optional)
# ---------------------------------------------------------
if __name__ == "__main__":
    print("PROJECT_ROOT:", PROJECT_ROOT)
    print("SRC_PATH:", SRC_PATH)
    print("GUI_PATH:", GUI_PATH)
    print("DATA_PATH:", DATA_PATH)
    print("CSV_PATH:", CSV_PATH)
    print("API_PATH:", API_PATH)
    print("sys.path:", sys.path[:5], "...")
