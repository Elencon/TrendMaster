from unittest.mock import patch

from src.gui.dashboard_window.window import DashboardWindow

@patch('src.gui.dashboard_window.window.DashboardUIBuilder')
@patch('src.gui.dashboard_window.window.DashboardDataHandler')
def test_dashboard_window_init(mock_handler, mock_builder, qapp_instance):
    """Test DashboardWindow initializes properly."""
    # Build a fake UI so it doesn't crash
    from PySide6.QtWidgets import QWidget
    mock_builder.return_value.create_main_layout.return_value = QWidget()
    mock_builder.return_value.create_toolbar.return_value = QWidget()

    window = DashboardWindow()

    assert window.windowTitle() == "TrendMaster Analytics Dashboard"
    mock_builder.assert_called_once()
    mock_handler.assert_called_once()

@patch('src.gui.dashboard_window.window.DashboardDataHandler')
def test_dashboard_window_load_data(mock_handler_cls, qapp_instance):
    """Test data loading delegation."""
    with patch('src.gui.dashboard_window.window.DashboardUIBuilder'):
        window = DashboardWindow()
        window.load_data()

        mock_handler_cls.return_value.load_dashboard_data.assert_called_once()
