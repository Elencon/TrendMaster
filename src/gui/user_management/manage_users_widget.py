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
    """Widget for viewing and managing existing users."""

    refresh_requested = Signal()
    role_change_requested = Signal(int, str)      # user_id, username
    deactivate_requested = Signal(int, str)       # user_id, username
    activate_requested = Signal(int, str)         # user_id, username
    delete_requested = Signal(int, str)           # user_id, username

    def __init__(self, parent=None):
        super().__init__(parent)
        self._setup_ui()

    # ---------------------------------------------------------
    # UI Setup
    # ---------------------------------------------------------
    def _setup_ui(self):
        layout = QVBoxLayout(self)

        # Title
        title = QLabel("User Accounts")
        title.setObjectName("section_title")
        title.setAlignment(Qt.AlignCenter)
        layout.addWidget(title)

        # Users table
        self._users_table = QTableWidget()
        self._users_table.setObjectName("users_table")
        self._users_table.setColumnCount(6)
        self._users_table.setHorizontalHeaderLabels([
            "User ID", "Username", "Role", "Staff Name", "Active", "Last Login"
        ])
        self._users_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self._users_table.setSelectionBehavior(QTableWidget.SelectRows)
        self._users_table.setSelectionMode(QTableWidget.SingleSelection)
        self._users_table.setEditTriggers(QTableWidget.NoEditTriggers)

        # Slightly larger row height
        vh = self._users_table.verticalHeader()
        vh.setDefaultSectionSize(vh.defaultSectionSize() + 4)

        layout.addWidget(self._users_table)

        # Action buttons
        self._create_action_buttons(layout)

    def _create_action_buttons(self, layout):
        btn_layout = QHBoxLayout()

        self._refresh_btn = QPushButton("Refresh")
        self._refresh_btn.setObjectName("refresh_btn")
        self._refresh_btn.clicked.connect(self.refresh_requested.emit)

        self._change_role_btn = QPushButton("Change Role")
        self._change_role_btn.setObjectName("change_role_btn")
        self._change_role_btn.clicked.connect(self._on_change_role)

        self._deactivate_btn = QPushButton("Deactivate User")
        self._deactivate_btn.setObjectName("deactivate_btn")
        self._deactivate_btn.clicked.connect(self._on_deactivate)

        self._activate_btn = QPushButton("Activate User")
        self._activate_btn.setObjectName("activate_btn")
        self._activate_btn.clicked.connect(self._on_activate)

        self._delete_btn = QPushButton("Delete User")
        self._delete_btn.setObjectName("delete_btn")
        self._delete_btn.clicked.connect(self._on_delete)

        btn_layout.addWidget(self._refresh_btn)
        btn_layout.addStretch()
        btn_layout.addWidget(self._change_role_btn)
        btn_layout.addWidget(self._deactivate_btn)
        btn_layout.addWidget(self._activate_btn)
        btn_layout.addWidget(self._delete_btn)

        layout.addLayout(btn_layout)

    # ---------------------------------------------------------
    # Button Handlers
    # ---------------------------------------------------------
    def _on_change_role(self):
        user_id, username = self._get_selected_user()
        if user_id is not None:
            self.role_change_requested.emit(user_id, username)

    def _on_deactivate(self):
        user_id, username = self._get_selected_user()
        if user_id is not None:
            self.deactivate_requested.emit(user_id, username)

    def _on_activate(self):
        user_id, username = self._get_selected_user()
        if user_id is not None:
            self.activate_requested.emit(user_id, username)

    def _on_delete(self):
        user_id, username = self._get_selected_user()
        if user_id is not None:
            self.delete_requested.emit(user_id, username)

    # ---------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------
    def _get_selected_user(self) -> tuple[Optional[int], Optional[str]]:
        """Return (user_id, username) of selected row."""
        if not self._users_table.selectedItems():
            return None, None

        row = self._users_table.currentRow()
        user_id = int(self._users_table.item(row, 0).text())
        username = self._users_table.item(row, 1).text()
        return user_id, username

    def get_selected_user_role(self) -> Optional[str]:
        """Return role of selected user."""
        if not self._users_table.selectedItems():
            return None

        row = self._users_table.currentRow()
        return self._users_table.item(row, 2).text()

    # ---------------------------------------------------------
    # Load Users
    # ---------------------------------------------------------
    def load_users(self, users: List[Dict[str, Any]]):
        """Load users into the table."""
        self._users_table.setRowCount(len(users))

        for row, user in enumerate(users):
            self._users_table.setItem(row, 0, QTableWidgetItem(str(user["user_id"])))
            self._users_table.setItem(row, 1, QTableWidgetItem(user["username"]))
            self._users_table.setItem(row, 2, QTableWidgetItem(user["role"]))

            # Staff name
            name = user.get("name")
            last = user.get("last_name")
            staff_name = f"{name} {last}" if name and last else ""
            self._users_table.setItem(row, 3, QTableWidgetItem(staff_name))

            # Active
            active_text = "Yes" if user.get("active") else "No"
            self._users_table.setItem(row, 4, QTableWidgetItem(active_text))

            # Last login
            last_login = user.get("last_login")
            last_login_text = (
                last_login.strftime("%Y-%m-%d %H:%M") if last_login else "Never"
            )
            self._users_table.setItem(row, 5, QTableWidgetItem(last_login_text))
