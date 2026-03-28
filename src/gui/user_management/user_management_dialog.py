"""
User Management Dialog
Allows administrators to create and manage user accounts.
"""

from typing import Optional

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


class UserManagementDialog(QWidget):
    """Dialog for managing user accounts (Administrator only)."""

    user_created = Signal(str)  # Emits username when user is created

    def __init__(self, parent=None):
        super().__init__(parent)

        # Protected internal state
        self._user_manager: Optional[UserManager] = None
        self._db_connection = None

        self.setWindowTitle("User Management")
        self.resize(900, 650)

        self._setup_ui()
        self._connect_database()
        self._load_users()

    # ---------------------------------------------------------
    # Public read-only accessors
    # ---------------------------------------------------------
    @property
    def user_manager(self):
        return self._user_manager

    @property
    def db_connection(self):
        return self._db_connection

    @property
    def create_widget(self):
        return self._create_widget

    @property
    def manage_widget(self):
        return self._manage_widget

    @property
    def tabs(self):
        return self._tabs

    # ---------------------------------------------------------
    # UI Setup
    # ---------------------------------------------------------
    def _setup_ui(self):
        layout = QVBoxLayout(self)

        self._tabs = QTabWidget()
        self._tabs.setObjectName("user_mgmt_tabs")

        # Create User tab
        self._create_widget = CreateUserWidget()
        self._create_widget.user_created.connect(self._handle_user_creation)
        self._tabs.addTab(self._create_widget, "Create User")

        # Manage Users tab
        self._manage_widget = ManageUsersWidget()
        self._manage_widget.refresh_requested.connect(self._load_users)
        self._manage_widget.role_change_requested.connect(self._change_user_role)
        self._manage_widget.deactivate_requested.connect(self._deactivate_user)
        self._manage_widget.activate_requested.connect(self._activate_user)
        self._manage_widget.delete_requested.connect(self._delete_user)
        self._tabs.addTab(self._manage_widget, "Manage Users")

        layout.addWidget(self._tabs)

    # ---------------------------------------------------------
    # Database Connection
    # ---------------------------------------------------------
    def _connect_database(self):
        try:
            db_config = DatabaseConfig().to_dict()
            self._db_connection = connect_to_mysql(db_config)

            if self._db_connection:
                self._user_manager = UserManager(self._db_connection)
            else:
                QMessageBox.critical(self, "Database Error", "Failed to connect to database")

        except Exception as e:
            QMessageBox.critical(self, "Database Error", f"Error connecting to database:\n{e}")

    # ---------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------
    def _ensure_manager(self) -> bool:
        """Return True if user manager is available, else show warning."""
        if self._user_manager:
            return True
        QMessageBox.warning(self, "Error", "User manager not initialized")
        return False

    # ---------------------------------------------------------
    # User Creation
    # ---------------------------------------------------------
    def _handle_user_creation(self, *args):
        """
        Handle user creation request from widget.
        Accepts any signal signature for compatibility.
        """
        if not self._ensure_manager():
            return

        data = self._create_widget.get_form_data()

        # Validate username
        if not data["username"]:
            QMessageBox.warning(self, "Validation Error", "Username cannot be empty")
            return

        # Validate password strength
        is_valid, error_message = self._create_widget.validate_password()
        if not is_valid:
            QMessageBox.warning(self, "Weak Password", error_message)
            return

        # Validate confirmation
        if data["password"] != data["confirm_password"]:
            QMessageBox.warning(self, "Validation Error", "Passwords do not match")
            return

        # Parse staff ID
        staff_id = None
        if data["staff_id"]:
            try:
                staff_id = int(data["staff_id"])
            except ValueError:
                QMessageBox.warning(self, "Validation Error", "Staff ID must be a number")
                return

        # Create user
        success = self._user_manager.create_user(
            data["username"],
            data["password"],
            data["role"],
            staff_id,
        )

        if success:
            QMessageBox.information(
                self,
                "Success",
                f"User '{data['username']}' created successfully with role '{data['role']}'",
            )
            self._create_widget.clear_form()
            self.user_created.emit(data["username"])
            self._load_users()
        else:
            QMessageBox.critical(
                self,
                "Error",
                f"Failed to create user '{data['username']}'\n\nUsername may already exist.",
            )

    # ---------------------------------------------------------
    # Load Users
    # ---------------------------------------------------------
    def _load_users(self):
        if not self._ensure_manager():
            return

        users = self._user_manager.get_all_users()
        self._manage_widget.load_users(users)

    # ---------------------------------------------------------
    # Role Change
    # ---------------------------------------------------------
    def _change_user_role(self, user_id: int, username: str):
        if not self._ensure_manager():
            return

        current_role = self._manage_widget.get_selected_user_role()
        if not current_role:
            QMessageBox.warning(self, "Selection Required", "Please select a user")
            return

        roles = ["Employee", "Manager", "Administrator"]

        new_role, ok = QInputDialog.getItem(
            self,
            "Change Role",
            f"Select new role for '{username}':",
            roles,
            roles.index(current_role),
            False,
        )

        if ok and new_role != current_role:
            if self._user_manager.update_user_role(user_id, new_role):
                QMessageBox.information(self, "Success", f"Role changed to '{new_role}'")
                self._load_users()
            else:
                QMessageBox.critical(self, "Error", "Failed to update user role")

    # ---------------------------------------------------------
    # Activate / Deactivate / Delete
    # ---------------------------------------------------------
    def _deactivate_user(self, user_id: int, username: str):
        if not self._ensure_manager():
            return

        reply = QMessageBox.question(
            self,
            "Confirm Deactivation",
            f"Deactivate user '{username}'?\nThey will no longer be able to log in.",
            QMessageBox.Yes | QMessageBox.No,
        )

        if reply == QMessageBox.Yes:
            if self._user_manager.deactivate_user(user_id):
                QMessageBox.information(self, "Success", f"User '{username}' deactivated")
                self._load_users()
            else:
                QMessageBox.critical(self, "Error", "Failed to deactivate user")

    def _activate_user(self, user_id: int, username: str):
        if not self._ensure_manager():
            return

        if self._user_manager.activate_user(user_id):
            QMessageBox.information(self, "Success", f"User '{username}' activated")
            self._load_users()
        else:
            QMessageBox.critical(self, "Error", "Failed to activate user")

    def _delete_user(self, user_id: int, username: str):
        if not self._ensure_manager():
            return

        reply = QMessageBox.warning(
            self,
            "⚠️ Confirm Permanent Deletion",
            f"PERMANENTLY DELETE user '{username}'?\n\n"
            "⚠️ This action CANNOT be undone.\n"
            "Consider deactivation instead.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )

        if reply != QMessageBox.Yes:
            return

        confirm_text, ok = QInputDialog.getText(
            self,
            "Final Confirmation",
            f"Type '{username}' to confirm deletion:",
        )

        if not ok:
            return

        if confirm_text != username:
            QMessageBox.information(self, "Cancelled", "Username did not match")
            return

        if self._user_manager.delete_user(user_id):
            QMessageBox.information(self, "Deleted", f"User '{username}' deleted")
            self._load_users()
        else:
            QMessageBox.critical(self, "Error", "Failed to delete user")

    # ---------------------------------------------------------
    # Cleanup
    # ---------------------------------------------------------
    def closeEvent(self, event):
        if self._db_connection:
            self._db_connection.close()
        event.accept()
