"""Centralized path configuration for GUI modules"""

import sys
from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
SRC_PATH = PROJECT_ROOT / "src"
GUI_PATH = PROJECT_ROOT / "gui"
DATA_PATH = PROJECT_ROOT / "data"
CSV_PATH = DATA_PATH / "CSV"
API_PATH = DATA_PATH / "API"

# Add to sys.path if not already there
for path in [SRC_PATH, GUI_PATH]:
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)
