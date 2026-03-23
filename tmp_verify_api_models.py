import sys
from pathlib import Path

# Add src to sys.path
src_path = str(Path(r'c:\Economy\Invest\TrendMaster\src').resolve())
sys.path.insert(0, src_path)

print("Direct test verification starting...")

try:
    from tests.api.test_api_models import (
        test_request_method_idempotency,
        test_api_request_creation,
        test_api_response_properties
    )
    
    print("Running test_request_method_idempotency...", end=" ")
    test_request_method_idempotency()
    print("SUCCESS")
    
    print("Running test_api_request_creation...", end=" ")
    test_api_request_creation()
    print("SUCCESS")
    
    print("Running test_api_response_properties...", end=" ")
    test_api_response_properties()
    print("SUCCESS")
    
    print("Verification complete.")
except Exception as e:
    print(f"FAILED with error: {e}")
    sys.exit(1)
