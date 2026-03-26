import sys
from pathlib import Path

# Add src to sys.path
src_path = str(Path(r'c:\Economy\Invest\TrendMaster\src').resolve())
sys.path.insert(0, src_path)

print(f"DEBUG: sys.path[0] = {sys.path[0]}")

try:
    print("Attempting to import common.exceptions...")
    import common.exceptions
    print("SUCCESS: common.exceptions imported.")
    
    print("Attempting to import api.api_client...")
    import api.api_client
    print("SUCCESS: api.api_client imported.")
    
except Exception as e:
    print(f"FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
