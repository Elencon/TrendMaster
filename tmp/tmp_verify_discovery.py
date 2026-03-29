import sys
from pathlib import Path

# Add src to sys.path
src_path = str(Path(r'c:\Economy\Invest\TrendMaster\src').resolve())
sys.path.insert(0, src_path)

print("Attempting to import test_batch_processors...")
try:
    print("SUCCESS: test_batch_processors imported successfully.")
except Exception as e:
    print(f"FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
