"""
ETL Pipeline Dashboard Interface using PySide6
Compatibility wrapper - imports from modularized dashboard_window package
"""

import sys
from pathlib import Path

# Set up Python path to find src directory
current_dir = Path(__file__).parent.parent
if str(current_dir) not in sys.path:
    sys.path.insert(0, str(current_dir))

from src.guidashboard_window import main


if __name__ == "__main__":
    sys.exit(main())
