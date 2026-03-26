import sys
from pathlib import Path

# Add src to sys.path
src_path = str(Path(r'c:\Economy\Invest\TrendMaster\src').resolve())
sys.path.insert(0, src_path)

print(f"Python path: {sys.path}")

modules_to_test = [
    "dotenv",
    "logging",
    "dataclasses",
    "pathlib",
    "config",
    "connect",
    "database.batch_operations",
    "database.batch_operations.batch_processor",
]

for module_name in modules_to_test:
    print(f"Importing {module_name}...", end=" ", flush=True)
    try:
        __import__(module_name)
        print("SUCCESS")
    except Exception as e:
        print(f"FAILED: {e}")

print("Diagnostic complete.")
