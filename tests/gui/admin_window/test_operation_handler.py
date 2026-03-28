import pytest
from unittest.mock import MagicMock, call, patch
from PySide6.QtWidgets import QWidget
from src.gui.admin_window.operation_handler import AdminOperationHandler

@pytest.fixture
def mock_window():
    window = MagicMock(spec=QWidget)
    window.statusBar = MagicMock()
    window.progress_bar = MagicMock()
    window.output_text = MagicMock()
    window.api_url_input = MagicMock()
    window.api_url_input.text.return_value = "http://test.com/api"
    window.operation_buttons = {}
    return window

def test_handler_initialization(mock_window):
    """Test handler initializes properly with window reference."""
    handler = AdminOperationHandler(mock_window)
    assert handler.window == mock_window
    assert handler.current_worker is None
    assert handler.selected_csv_files == []

def test_initialize_status(mock_window):
    """Test initialize status appends text."""
    handler = AdminOperationHandler(mock_window)
    
    # We shouldn't actually need module availability to test the text logic.
    handler.append_output = MagicMock()
    handler.initialize_status()
    
    # We should see multiple calls to append_output
    assert handler.append_output.call_count > 0

def test_set_buttons_enabled(mock_window):
    """Test button toggling logic."""
    mock_btn = MagicMock()
    mock_window.operation_buttons = {"test_btn": mock_btn}
    
    handler = AdminOperationHandler(mock_window)
    handler.set_buttons_enabled(False)
    
    # It might only disable if modules are available or based on logic, let's verify call
    assert mock_btn.setEnabled.called

@patch('src.gui.admin_window.operation_handler.ETLWorker')
def test_start_operation(mock_worker_cls, mock_window):
    """Test starting an operation."""
    handler = AdminOperationHandler(mock_window)
    mock_worker = MagicMock()
    mock_worker_cls.return_value = mock_worker
    
    handler.start_operation("test_connection")
    
    # Ensure worker was created and started
    mock_worker_cls.assert_called_once_with("test_connection")
    mock_worker.start.assert_called_once()
    assert handler.current_worker == mock_worker
    
def test_cleanup_operation(mock_window):
    """Test cleanup resets worker and UI state."""
    handler = AdminOperationHandler(mock_window)
    mock_worker = MagicMock()
    mock_worker.isRunning.return_value = False
    handler.current_worker = mock_worker
    
    handler.cleanup_operation()
    
    assert handler.current_worker is None
    mock_worker.deleteLater.assert_called_once()
    mock_window.progress_bar.setVisible.assert_called_with(False)
