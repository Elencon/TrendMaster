import pytest
import sys
from PySide6.QtWidgets import QApplication

@pytest.fixture(scope="session")
def qapp_instance():
    """
    Ensure we have a single QApplication instance for the test session.
    pytest-qt provides `qapp` which we could use, but sometimes initializing custom
    style overrides breaks things.
    """
    app = QApplication.instance()
    if app is None:
        app = QApplication(sys.argv)
    yield app

# In many cases, pytest-qt's built-in `qtbot` and `qapp` fixtures are sufficient.
