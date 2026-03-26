"""
User Management Dialog
Allows administrators to create and manage user accounts.
"""
import sys
from typing import Optional
from pathlib import Path

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QMessageBox,
    QTabWidget, QInputDialog
)
from PySide6.QtCore import Signal

from database import connect_to_mysql
from auth.user_manager import UserManager
from config import DatabaseConfig

from .create_user_widget import CreateUserWidget
from .manage_users_widget import ManageUsersWidget

# Centralized path configuration

gui_path = Path(__file__).parent.parent
sys.path.insert(0, str(gui_path))

class UserManagementDialog(QWidget):
    """Dialog for managing user accounts (Administrator only)"""

    user_created = Signal(str)  # Emits username when user is created

    def __init__(self, parent=None):
        super().__init__(parent)
        self.user_manager: Optional[UserManager] = None
        self.db_connection = None

        self.setWindowTitle("User Management")
        self.resize(900, 650)

        self._setup_ui()
        self._connect_database()
        self._load_users()

    def _setup_ui(self):
        """Set up the user interface"""
        main_layout = QVBoxLayout(self)

        # Create tab widget
        self.tabs = QTabWidget()
        self.tabs.setObjectName("user_mgmt_tabs")

        # Create user widget
        self.create_widget = CreateUserWidget()
        self.create_widget.user_created.connect(self._handle_user_creation)
        self.tabs.addTab(self.create_widget, "Create User")

        # Manage users widget
        self.manage_widget = ManageUsersWidget()
        self.manage_widget.refresh_requested.connect(self._load_users)
        self.manage_widget.role_change_requested.connect(self._change_user_role)
        self.manage_widget.deactivate_requested.connect(self._deactivate_user)
        self.manage_widget.activate_requested.connect(self._activate_user)
        self.manage_widget.delete_requested.connect(self._delete_user)
        self.tabs.addTab(self.manage_widget, "Manage Users")

        main_layout.addWidget(self.tabs)

    def _connect_database(self):
        """Connect to database and initialize UserManager"""
        try:
            db_config = DatabaseConfig().to_dict()
            self.db_connection = connect_to_mysql(db_config)

            if self.db_connection:
                self.user_manager = UserManager(self.db_connection)
            else:
                QMessageBox.critical(self, "Database Error", "Failed to connect to database")
        except Exception as e:
            QMessageBox.critical(self, "Database Error", f"Error connecting to database:\n{e}")

    def _handle_user_creation(self, username, password, confirm):
        """Handle user creation request from widget"""
        if not self.user_manager:
            QMessageBox.warning(self, "Error", "User manager not initialized")
            return

        # Get form data
        data = self.create_widget.get_form_data()

        # Validate username
        if not data['username']:
            QMessageBox.warning(self, "Validation Error", "Username cannot be empty")
            return

        # Validate password strength using widget's validator
        is_valid, error_message = self.create_widget.validate_password()
        if not is_valid:
            QMessageBox.warning(self, "Weak Password", error_message)
            return

        # Validate password confirmation
        if data['password'] != data['confirm_password']:
            QMessageBox.warning(self, "Validation Error", "Passwords do not match")
            return

        # Parse staff_id
        staff_id = None
        if data['staff_id']:
            try:
                staff_id = int(data['staff_id'])
            except ValueError:
                QMessageBox.warning(self, "Validation Error", "Staff ID must be a number")
                return

        # Create user
        success = self.user_manager.create_user(
            data['username'],
            data['password'],
            data['role'],
            staff_id
        )

        if success:
            QMessageBox.information(
                self,
                "Success",
                f"User '{data['username']}' created successfully with role '{data['role']}'"
            )
            self.create_widget.clear_form()
            self.user_created.emit(data['username'])
            self._load_users()
        else:
            QMessageBox.critical(
                self,
                "Error",
                f"Failed to create user '{data['username']}'\n\nUsername may already exist."
            )

    def _load_users(self):
        """Load and display all users"""
        if not self.user_manager:
            return

        users = self.user_manager.get_all_users()
        self.manage_widget.load_users(users)

    def _change_user_role(self, user_id: int, username: str):
        """Change a user's role"""
        if not self.user_manager:
            return

        current_role = self.manage_widget.get_selected_user_role()
        if not current_role:
            QMessageBox.warning(self, "Selection Required", "Please select a user from the table")
            return

        # Show role selection dialog
        roles = ["Employee", "Manager", "Administrator"]
        new_role, ok = QInputDialog.getItem(
            self,
            "Change Role",
            f"Select new role for '{username}':",
            roles,
            roles.index(current_role),
            False
        )

        if ok and new_role != current_role:
            success = self.user_manager.update_user_role(user_id, new_role)

            if success:
                QMessageBox.information(self, "Success", f"Role changed to '{new_role}' for user '{username}'")
                self._load_users()
            else:
                QMessageBox.critical(self, "Error", "Failed to update user role")

    def _deactivate_user(self, user_id: int, username: str):
        """Deactivate a user account"""
        if not self.user_manager:
            return

        # Confirm
        reply = QMessageBox.question(
            self,
            "Confirm Deactivation",
            f"Are you sure you want to deactivate user '{username}'?\n\n"
            "They will no longer be able to log in.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )

        if reply == QMessageBox.StandardButton.Yes:
            success = self.user_manager.deactivate_user(user_id)

            if success:
                QMessageBox.information(self, "Success", f"User '{username}' has been deactivated")
                self._load_users()
            else:
                QMessageBox.critical(self, "Error", "Failed to deactivate user")

    def _activate_user(self, user_id: int, username: str):
        """Activate a user account"""
        if not self.user_manager:
            return

        success = self.user_manager.activate_user(user_id)

        if success:
            QMessageBox.information(self, "Success", f"User '{username}' has been activated")
            self._load_users()
        else:
            QMessageBox.critical(self, "Error", "Failed to activate user")

    def _delete_user(self, user_id: int, username: str):
        """Permanently delete a user account"""
        if not self.user_manager:
            return

        # Strong confirmation with warning
        reply = QMessageBox.warning(
            self,
            "⚠️ Confirm Permanent Deletion",
            f"Are you sure you want to PERMANENTLY DELETE user '{username}'?\n\n"
            "⚠️ WARNING: This action CANNOT be undone!\n"
            "⚠️ All user data will be permanently removed.\n\n"
            "Consider using 'Deactivate User' instead to preserve data.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )

        if reply == QMessageBox.StandardButton.Yes:
            # Second confirmation
            confirm_text, ok = QInputDialog.getText(
                self,
                "Final Confirmation",
                f"Type '{username}' to confirm deletion:"
            )

            if ok and confirm_text == username:
                success = self.user_manager.delete_user(user_id)

                if success:
                    QMessageBox.information(
                        self,
                        "Deleted",
                        f"User '{username}' has been permanently deleted"
                    )
                    self._load_users()
                else:
                    QMessageBox.critical(self, "Error", "Failed to delete user")
            elif ok:
                QMessageBox.information(
                    self,
                    "Cancelled",
                    "Deletion cancelled - username did not match"
                )

    def closeEvent(self, event):
        """Clean up database connection when closing"""
        if self.db_connection:
            self.db_connection.close()

        event.accept()
