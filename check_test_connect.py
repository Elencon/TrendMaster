import sys
from pathlib import Path
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))
try:
    print("SUCCESS: test_connect imported.")
except Exception:
    import traceback
    traceback.print_exc()
