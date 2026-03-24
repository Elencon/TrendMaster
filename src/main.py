r"""
C:\Economy\Invest\TrendMaster\src\main.py
Main module for database operations.
"""

from pathlib import Path
from connect import config, logger
from database.db_manager import DatabaseManager

DATA_DIR = Path(__file__).parent.parent / "data"

if __name__ == "__main__":
    db = DatabaseManager(config, DATA_DIR, logger)

    if db.test_connection():
        if db.create_all_tables_from_csv():
            db.import_csv_data()
            db.verify_data()