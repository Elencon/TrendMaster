import sys
import subprocess

try:
    import pytest
    print(f"pytest version: {pytest.__version__}")
except ImportError:
    print("pytest not installed in this environment")

print(f"Python executable: {sys.executable}")
print(f"Python path: {sys.path}")
