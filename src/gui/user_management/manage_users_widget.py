"""
Manage Users Widget - Table and actions for managing existing users
"""

from typing import List, Dict, Any, Optional
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QTableWidget, QTableWidgetItem, QHeaderView
)
from PySide6.QtCore import Qt, Signal


class ManageUsersWidget(QWidget):
    """Widget for viewing and managing existing users"""

    refresh_requested = Signal()
    role_change_requested = Signal(int, str)  # user_id, username
    deactivate_requested = Signal(int, str)   # user_id, username
    activate_requested = Signal(int, str)     # user_id, username
    delete_requested = Signal(int, str)       # user_id, username

    def __init__(self, parent=None):
        super().__init__(parent)
        self._setup_ui()

    def _setup_ui(self):
        """Set up the user interface"""
        layout = QVBoxLayout(self)

        # Title
        title = QLabel("User Accounts")
        title.setObjectName("section_title")
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)

        # Users table
        self.users_table = QTableWidget()
        self.users_table.setObjectName("users_table")
        self.users_table.setColumnCount(6)
        self.users_table.setHorizontalHeaderLabels([
            "User ID", "Username", "Role", "Staff Name", "Active", "Last Login"
        ])
        self.users_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.users_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.users_table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        self.users_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)

        # Set row height - add 4 pixels to default
        self.users_table.verticalHeader().setDefaultSectionSize(self.users_table.verticalHeader().defaultSectionSize() + 4)

        layout.addWidget(self.users_table)

        # Action buttons
        self._create_action_buttons(layout)

    def _create_action_buttons(self, layout):
        """Create action buttons"""
        btn_layout = QHBoxLayout()

        self.refresh_btn = QPushButton("Refresh")
        self.refresh_btn.setObjectName("refresh_btn")
        self.refresh_btn.clicked.connect(self.refresh_requested.emit)

        self.change_role_btn = QPushButton("Change Role")
        self.change_role_btn.setObjectName("change_role_btn")
        self.change_role_btn.clicked.connect(self._on_change_role)

        self.deactivate_btn = QPushButton("Deactivate User")
        self.deactivate_btn.setObjectName("deactivate_btn")
        self.deactivate_btn.clicked.connect(self._on_deactivate)

        self.activate_btn = QPushButton("Activate User")
        self.activate_btn.setObjectName("activate_btn")
        self.activate_btn.clicked.connect(self._on_activate)

        self.delete_btn = QPushButton("Delete User")
        self.delete_btn.setObjectName("delete_btn")
        self.delete_btn.clicked.connect(self._on_delete)

        btn_layout.addWidget(self.refresh_btn)
        btn_layout.addStretch()
        btn_layout.addWidget(self.change_role_btn)
        btn_layout.addWidget(self.deactivate_btn)
        btn_layout.addWidget(self.activate_btn)
        btn_layout.addWidget(self.delete_btn)

        layout.addLayout(btn_layout)

    def _on_change_role(self):
        """Emit signal to change user role"""
        user_id, username = self._get_selected_user()
        if user_id:
            self.role_change_requested.emit(user_id, username)

    def _on_deactivate(self):
        """Emit signal to deactivate user"""
        user_id, username = self._get_selected_user()
        if user_id:
            self.deactivate_requested.emit(user_id, username)

    def _on_activate(self):
        """Emit signal to activate user"""
        user_id, username = self._get_selected_user()
        if user_id:
            self.activate_requested.emit(user_id, username)

    def _on_delete(self):
        """Emit signal to delete user"""
        user_id, username = self._get_selected_user()
        if user_id:
            self.delete_requested.emit(user_id, username)

    def _get_selected_user(self) -> tuple[Optional[int], Optional[str]]:
        """Get selected user ID and username"""
        if not self.users_table.selectedItems():
            return None, None

        row = self.users_table.currentRow()
        user_id = int(self.users_table.item(row, 0).text())
        username = self.users_table.item(row, 1).text()
        return user_id, username

    def get_selected_user_role(self) -> Optional[str]:
        """Get the role of selected user"""
        if not self.users_table.selectedItems():
            return None

        row = self.users_table.currentRow()
        return self.users_table.item(row, 2).text()

    def load_users(self, users: List[Dict[str, Any]]):
        """Load users into the table"""
        self.users_table.setRowCount(len(users))

        for row, user in enumerate(users):
            self.users_table.setItem(row, 0, QTableWidgetItem(str(user['user_id'])))
            self.users_table.setItem(row, 1, QTableWidgetItem(user['username']))
            self.users_table.setItem(row, 2, QTableWidgetItem(user['role']))

            # Staff name
            staff_name = ""
            if user.get('name') and user.get('last_name'):
                staff_name = f"{user['name']} {user['last_name']}"
            self.users_table.setItem(row, 3, QTableWidgetItem(staff_name))

            # Active status
            active_text = "Yes" if user['active'] else "No"
            self.users_table.setItem(row, 4, QTableWidgetItem(active_text))

            # Last login
            last_login = user.get('last_login')
            last_login_text = last_login.strftime("%Y-%m-%d %H:%M") if last_login else "Never"
            self.users_table.setItem(row, 5, QTableWidgetItem(last_login_text))
