"""Test runner for core ETL pipeline functionality tests."""

import importlib.util
from pathlib import Path

def run_all_tests():
    """Run all core functionality test files."""
    print("ETL PIPELINE CORE FUNCTIONALITY TESTS")
    print("="*60)
    
    tests_dir = Path(__file__).resolve().parent
    # Recursively find all test files
    test_files = sorted(list(tests_dir.rglob('test_*.py')))
    
    print(f"Tests directory: {tests_dir}")
    print(f"Found {len(test_files)} test files\n")
    
    results = {}
    
    for test_file in test_files:
        # Calculate relative path to tests_dir to determine the module name
        relative_path = test_file.relative_to(tests_dir)
        # Convert path to dotted module name (e.g., api/test_api_client.py -> api.test_api_client)
        test_name = ".".join(relative_path.with_suffix('').parts)
        
        print(f"Running {test_name}...")
        print("-" * 40)
        
        try:
            # Load and run the test module
            spec = importlib.util.spec_from_file_location(test_name, str(test_file))
            test_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(test_module)
            
            results[test_name] = "PASSED"
            
        except Exception as e:
            results[test_name] = f"FAILED: {e}"
        
        print("\n")
    
    # Summary
    print("="*60)
    print("TEST RESULTS SUMMARY")
    print("="*60)
    
    for test_name, result in results.items():
        print(f"{test_name:<25}: {result}")
    
    # Overall result
    passed = sum(1 for r in results.values() if "PASSED" in r)
    total = len(results)
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("All tests passed successfully!")
        return True
    else:
        print("WARNING: Some tests failed. Please check the output above.")
        return False

if __name__ == "__main__":
    # Run core functional tests only
    success = run_all_tests()
    
    print("\nCore functionality test suite completed!")