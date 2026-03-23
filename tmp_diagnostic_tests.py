from pathlib import Path

def find_tests():
    tests_dir = Path("tests").resolve()
    print(f"Tests directory: {tests_dir}")
    test_files = sorted(list(tests_dir.rglob('test_*.py')))
    print(f"Found {len(test_files)} test files:")
    for f in test_files:
        relative_path = f.relative_to(tests_dir)
        test_name = ".".join(relative_path.with_suffix('').parts)
        print(f" - {test_name} ({f})")

if __name__ == "__main__":
    find_tests()
