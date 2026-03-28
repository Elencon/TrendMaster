import pytest
import sys

with open('pytest_log.txt', 'w') as f:
    sys.stdout = f
    sys.stderr = f
    pytest.main(['tests/gui/user_management/test_user_management.py', '-v', '--tb=short'])
