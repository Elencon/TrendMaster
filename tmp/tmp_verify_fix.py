import sys
from pathlib import Path

# Add src to sys.path
src_path = str(Path(r'c:\Economy\Invest\TrendMaster\src').resolve())
sys.path.insert(0, src_path)

print("Attempting to import database.batch_operations...")
try:
    print("SUCCESS: BatchProcessor imported successfully.")
except Exception as e:
    print(f"FAILED: {e}")
    sys.exit(1)
