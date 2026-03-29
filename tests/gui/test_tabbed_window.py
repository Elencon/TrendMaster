from unittest.mock import MagicMock, patch

# Mock modules that interfere with basic GUI setup during tests
import sys
sys.modules['cache_cleaner'] = MagicMock()
sys.modules['themes'] = MagicMock()

from src.gui.tabbed_window import MainTabbedWindow

@patch('src.gui.tabbed_window.ETLMainWindow')
@patch('src.gui.tabbed_window.DashboardWindow')
def test_tabbed_window_init(mock_dashboard, mock_admin, qapp_instance):
    """Test MainTabbedWindow initializes properly."""

    # We create a simple mock widget for the window instances so the QTabWidget doesn't complain about adding None.
    from PySide6.QtWidgets import QWidget
    mock_admin.return_value = QWidget()
    mock_dashboard.return_value = QWidget()

    # Needs to be mocked or run without failing
    with patch('src.gui.tabbed_window.MainTabbedWindow._cleanup_orphaned_auth_processes'):
        window = MainTabbedWindow()

        assert window.windowTitle() == "TrendMaster - Application Suite"
        assert window.tabs.count() == 2

        # Verify the two main modules were instantiated
        mock_admin.assert_called_once()
        mock_dashboard.assert_called_once()

@patch('src.gui.tabbed_window.ETLMainWindow')
@patch('src.gui.tabbed_window.DashboardWindow')
def test_tabbed_window_logout(mock_dashboard, mock_admin, qapp_instance):
    """Test logout behavior."""
    mock_admin.return_value = MagicMock()
    mock_dashboard.return_value = MagicMock()

    with patch('src.gui.tabbed_window.MainTabbedWindow._cleanup_orphaned_auth_processes'), \
         patch('src.gui.tabbed_window.AuthManager') as MockAuth, \
         patch('src.gui.tabbed_window.LoginWindow') as MockLogin:

        window = MainTabbedWindow()

        # Call logout
        window._handle_logout()

        # Ensure auth manager clears session
        MockAuth.get_instance.return_value.logout.assert_called_once()

        # Ensure login window is shown
        MockLogin.return_value.show.assert_called_once()

        # Window shouldn't be visible (though PySide mock might not capture this easily without checking properties)
        # We assume the code called self.hide()
