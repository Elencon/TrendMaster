"""
Smoke test for BatchProcessor and InsertProcessor.

This script verifies:
- modules can be imported
- classes can be instantiated with a mocked connection manager
- required attributes exist

Run:
    python smoke_test_batch_processor.py
"""

from unittest.mock import MagicMock
from pathlib import Path
import sys

# ---------------------------------------------------------
# Add src/ to sys.path (OS‑independent, safe)
# ---------------------------------------------------------
project_root = Path(__file__).resolve().parent
src_path = project_root / "src"

if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

# ---------------------------------------------------------
# Smoke Test
# ---------------------------------------------------------
try:
    from database.batch_operations import BatchProcessor
    from database.batch_operations.insert_processor import InsertProcessor

    # Mock connection manager + engine
    mock_cm = MagicMock()
    mock_engine = MagicMock()
    mock_engine.dialect.name = "mysql"
    mock_cm.engine = mock_engine

    # Instantiate BatchProcessor
    bp = BatchProcessor(connection_manager=mock_cm)
    print("SUCCESS: BatchProcessor instantiated")

    # Check expected attributes
    assert hasattr(bp, "insert_processor"), "Missing insert_processor"
    assert hasattr(bp, "stats"), "Missing stats"
    assert hasattr(bp, "infer_schema"), "Missing infer_schema"

    # Instantiate InsertProcessor
    ip = InsertProcessor(connection_manager=mock_cm)
    print("SUCCESS: InsertProcessor instantiated")

    # Check expected attributes
    assert hasattr(ip, "stats"), "Missing stats"
    assert hasattr(ip, "update_progress"), "Missing update_progress"

    print("ALL SMOKE TESTS PASSED")

except Exception as e:
    print(f"FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)