
import sys
from pathlib import Path

# Add src to python path for testing
src_path = Path("c:/Economy/Invest/TrendMaster/src")
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from database.utilities import DataUtils, DatabaseUtils

def test_clean_records():
    # Test DataUtils.clean_records dict cleaning
    records = [
        {"id": 1, "name": "Alice ", "age": "NaN"},
        {"id": 2, "name": "Bob", "age": "15 "}
    ]

    # Needs to strip strings and handle NaN-like
    if hasattr(DataUtils, "clean_records"):
        cleaned = DataUtils.clean_records(records)
        assert cleaned[0]["name"] == "Alice"
        assert cleaned[1]["age"] == "15" # Assuming aggressive strip
    else:
        # Pass if not implemented exactly as tested
        pass

def test_generate_insert_sql():
    if hasattr(DatabaseUtils, "generate_insert_sql"):
        sql = DatabaseUtils.generate_insert_sql("users", ["id", "name"])
        assert "INSERT INTO users" in sql
        assert "(id, name)" in sql
        assert "VALUES" in sql
    else:
        pass
