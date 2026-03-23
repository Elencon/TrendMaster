"""
ETL Pipeline GUI Interface using PySide6
Compatibility wrapper - imports from modularized main_window package
"""

import sys
from pathlib import Path
from main_window import main


if __name__ == "__main__":
    sys.exit(main())