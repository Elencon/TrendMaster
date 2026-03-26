from unittest.mock import MagicMock
from pathlib import Path
import sys

# Add src to path using pathlib (OS‑independent)
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root / "src"))

try:
    from database.batch_operations import BatchProcessor, BaseBatchProcessor
    from database.batch_operations.insert_processor import InsertProcessor
    from database.batch_operations.update_processor import UpdateProcessor
    from database.batch_operations.upsert_processor import UpsertProcessor
    from database.batch_operations.delete_processor import DeleteProcessor

    mock_cm = MagicMock()
    mock_engine = MagicMock()
    mock_engine.dialect.name = "mysql"
    mock_cm.engine = mock_engine

    bp = BatchProcessor(connection_manager=mock_cm)
    print("SUCCESS: BatchProcessor instantiated")

    # Check attributes
    assert hasattr(bp, 'insert_processor')
    assert hasattr(bp, 'stats')
    assert hasattr(bp, 'infer_schema')

    # Check sub-processor
    ip = InsertProcessor(connection_manager=mock_cm)
    print("SUCCESS: InsertProcessor instantiated")
    assert hasattr(ip, 'stats')
    assert hasattr(ip, 'update_progress')

    print("ALL SMOKE TESTS PASSED")

except Exception as e:
    print(f"FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)