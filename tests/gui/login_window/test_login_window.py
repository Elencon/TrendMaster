from unittest.mock import MagicMock, patch

# Need to mock CacheCleaner before importing LoginWindow
import sys
sys.modules['cache_cleaner'] = MagicMock()

from src.gui.login_window.window import LoginWindow

@patch('src.gui.login_window.window.AuthManager')
def test_login_window_init(mock_auth_manager, qapp_instance):
    """Test LoginWindow initializes securely."""
    mock_auth_manager.get_instance.return_value = MagicMock()
    window = LoginWindow()

    assert window.windowTitle() == "TrendMaster - Secure Login"
    # Basic structural check
    assert hasattr(window, "login_form")
    assert window.login_form is not None

@patch('src.gui.login_window.window.LoginWorker')
def test_login_window_attempt_login(mock_worker_cls, qapp_instance):
    """Test login attempt submission."""
    with patch('src.gui.login_window.window.AuthManager') as mock_auth:
         window = LoginWindow()
         window.login_form.username_input.setText("test_user")
         window.login_form.password_input.setText("Password123!")

         window._on_login_clicked()

         # Worker should be properly initialized and started
         mock_worker_cls.assert_called_once()
         mock_worker_cls.return_value.start.assert_called_once()

         # Note: testing exact args depends on whether the worker expects dict or separated args,
         # but we just assert creation and start.
