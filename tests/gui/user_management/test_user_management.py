import pytest
from unittest.mock import MagicMock, patch
import sys
from PySide6.QtWidgets import QApplication

# Initialize QApplication
app = QApplication.instance()
if app is None:
    app = QApplication(sys.argv)

from src.gui.user_management.user_management_dialog import UserManagementDialog
from src.gui.user_management.manage_users_widget import ManageUsersWidget
from src.gui.user_management.create_user_widget import CreateUserWidget

@pytest.fixture(autouse=True)
def mock_db_connections():
    with patch('src.gui.user_management.user_management_dialog.connect_to_mysql') as mock_conn, \
         patch('src.gui.user_management.user_management_dialog.DatabaseConfig'), \
         patch('src.gui.user_management.user_management_dialog.UserManager') as mock_user_manager:
        yield mock_user_manager

def test_user_management_dialog_init(mock_db_connections):
    """Test dialog initialization."""
    # Assume mock_db_connections is the mock for UserManager class
    manager_instance = mock_db_connections.return_value
    manager_instance.get_all_users.return_value = []
    
    dialog = UserManagementDialog()
    assert dialog.windowTitle() == "User Management"
    assert hasattr(dialog, "tabs")

def test_manage_users_widget_refresh():
    """Test manage users widget structure and logic."""
    widget = ManageUsersWidget()
    
    mock_users = [
        {
            "user_id": 1, 
            "username": "admin", 
            "role": "Administrator", 
            "name": "Admin", 
            "last_name": "User", 
            "active": True, 
            "last_login": None
        }
    ]
    
    widget.load_users(mock_users)
    assert widget._users_table.rowCount() == 1
    
def test_create_user_widget():
    """Test user creation UI submission."""
    widget = CreateUserWidget()
    widget._username_input.setText("new_user")
    widget._password_input.setText("Password123!")
    widget._confirm_password_input.setText("Password123!")
    
    # Just check if the widget renders and has inputs setup correctly
    assert widget._username_input.text() == "new_user"
    assert widget._password_input.text() == "Password123!"
    
    # Check that data fetching works
    data = widget.get_form_data()
    assert data["username"] == "new_user"
    assert data["password"] == "Password123!"
    
    # Test logic
    results = []
    widget.user_created.connect(lambda d: results.append(d))
    widget._on_create_clicked()
    
    assert len(results) == 1
    assert results[0]["username"] == "new_user"
