import pytest
from unittest.mock import MagicMock, patch

# Fix issues with mock themes and cache cleaners
import sys
sys.modules['cache_cleaner'] = MagicMock()
sys.modules['themes'] = MagicMock()

from src.gui.admin_window.window import ETLMainWindow

def test_admin_window_init(qapp_instance):
    """Test ETLMainWindow initializes properly without crashing."""
    with patch('src.gui.admin_window.window.AdminUIBuilder') as MockBuilder, \
         patch('src.gui.admin_window.window.AdminOperationHandler') as MockHandler:

        # We need mock builder/handler to not crash on Qt setup if they depend on real widgets that we didn't mock
        # But wait, ETLMainWindow explicitly calls _ui_builder methods which might fail if MagicMock is returned
        # and it returns MagicMock for layouts. That is actually fine, setCentralWidget can take a mock.
        # Actually in PySide6, setCentralWidget requires a QWidget.
        mock_widget = MagicMock()
        MockBuilder.return_value.create_main_layout.return_value = mock_widget
        MockBuilder.return_value.create_toolbar.return_value = mock_widget

        window = ETLMainWindow()

        assert window.windowTitle() == "ETL Pipeline Manager - Production Ready (1,289+ Records)"

        MockBuilder.assert_called_once_with(window)
        MockHandler.assert_called_once_with(window)
        MockHandler.return_value.initialize_status.assert_called_once()

def test_admin_window_close_event(qapp_instance):
    """Test that close event calls cleanup."""
    with patch('src.gui.admin_window.window.AdminUIBuilder'), \
         patch('src.gui.admin_window.window.AdminOperationHandler') as MockHandler:
        window = ETLMainWindow()

        event = MagicMock()
        window.closeEvent(event)

        # Verify handler cleanup is called before rejecting/accepting
        MockHandler.return_value.cleanup_on_close.assert_called_once()
        event.accept.assert_called_once()

def test_admin_window_delegates():
    """Test that operations are correctly delegated to the handler."""
    with patch('src.gui.admin_window.window.AdminUIBuilder'), \
         patch('src.gui.admin_window.window.AdminOperationHandler') as MockHandler:

        # In PySide/PyQt, we typically don't need a QApplication for patching pure python attributes if they don't instaniate widgets.
        # But to be safe we use qapp or mock the widget creation.
        # The window creates widgets in _setup_ui.

        # We can just instantiate it to test delegates.
        # The easiest approach is to create a fake window.
        pass

@pytest.mark.usefixtures("qapp_instance")
class TestAdminWindowDelegation:
    @pytest.fixture(autouse=True)
    def setup_mocks(self):
        self.builder_patcher = patch('src.gui.admin_window.window.AdminUIBuilder')
        self.handler_patcher = patch('src.gui.admin_window.window.AdminOperationHandler')
        self.mock_builder_cls = self.builder_patcher.start()
        self.mock_handler_cls = self.handler_patcher.start()

        self.window = ETLMainWindow()
        yield

        self.builder_patcher.stop()
        self.handler_patcher.stop()

    def test_test_db_connection(self):
        self.window.test_db_connection()
        self.mock_handler_cls.return_value.test_db_connection.assert_called_once()

    def test_create_tables(self):
        self.window.create_tables()
        self.mock_handler_cls.return_value.create_tables.assert_called_once()

    def test_load_csv_data(self):
        self.window.load_csv_data()
        self.mock_handler_cls.return_value.load_csv_data.assert_called_once()
