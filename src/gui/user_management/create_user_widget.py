"""
Create User Widget - Form for creating new user accounts
"""

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QFormLayout, QLabel,
    QLineEdit, QPushButton, QComboBox, QGroupBox, QTextEdit,
)
from PySide6.QtCore import Qt, Signal

from auth.password_policy import PasswordPolicyValidator


class CreateUserWidget(QWidget):
    """Widget for creating new user accounts."""

    user_created = Signal(dict)

    def __init__(self, parent=None):
        super().__init__(parent)

        self._password_validator = PasswordPolicyValidator()
        self._setup_ui()

        self._password_input.textChanged.connect(self._update_password_strength)

    # ================= UI =================

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        title = QLabel("Create New User Account")
        title.setObjectName("section_title")
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)

        self._create_form(layout)
        self._create_password_requirements(layout)
        self._create_role_descriptions(layout)

        layout.addStretch()
        self._create_button(layout)

    def _create_form(self, layout):
        form_group = QGroupBox("User Information")
        form_layout = QFormLayout()

        input_width = 300

        self._username_input = QLineEdit()
        self._username_input.setPlaceholderText("Enter username")
        self._username_input.setMaximumWidth(input_width)

        self._password_input = QLineEdit()
        self._password_input.setEchoMode(QLineEdit.EchoMode.Password)
        self._password_input.setPlaceholderText("Enter strong password")
        self._password_input.setMaximumWidth(input_width)

        self._confirm_password_input = QLineEdit()
        self._confirm_password_input.setEchoMode(QLineEdit.EchoMode.Password)
        self._confirm_password_input.setPlaceholderText("Confirm password")
        self._confirm_password_input.setMaximumWidth(input_width)

        self._strength_label = QLabel("Password Strength: Not entered")

        self._role_combo = QComboBox()
        self._role_combo.addItems(["Employee", "Manager", "Administrator"])
        self._role_combo.setMaximumWidth(input_width)

        self._staff_id_input = QLineEdit()
        self._staff_id_input.setPlaceholderText("Optional: link to staff member")
        self._staff_id_input.setMaximumWidth(input_width)

        form_layout.addRow("Username:", self._username_input)
        form_layout.addRow("Password:", self._password_input)
        form_layout.addRow("", self._strength_label)
        form_layout.addRow("Confirm Password:", self._confirm_password_input)
        form_layout.addRow("Role:", self._role_combo)
        form_layout.addRow("Staff ID:", self._staff_id_input)

        form_group.setLayout(form_layout)
        layout.addWidget(form_group)

    def _create_password_requirements(self, layout):
        group = QGroupBox("Password Requirements")
        box = QVBoxLayout()

        text = QTextEdit()
        text.setReadOnly(True)
        text.setMaximumHeight(120)
        text.setPlainText(self._password_validator.get_requirements_text())

        box.addWidget(text)
        group.setLayout(box)
        layout.addWidget(group)

    def _create_role_descriptions(self, layout):
        group = QGroupBox("Role Permissions")
        box = QVBoxLayout()

        roles = [
            "• Employee: View and export data",
            "• Manager: View, export, import, and modify data",
            "• Administrator: Full system access including user management",
        ]

        for r in roles:
            box.addWidget(QLabel(r))

        group.setLayout(box)
        layout.addWidget(group)

    def _create_button(self, layout):
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()

        self._create_btn = QPushButton("Create User")
        self._create_btn.clicked.connect(self._on_create_clicked)

        btn_layout.addWidget(self._create_btn)
        layout.addLayout(btn_layout)

    # ================= Logic =================

    def _on_create_clicked(self):
        data = self.get_form_data()

        if not data["username"]:
            self._show_error("Username cannot be empty")
            return

        if not data["password"]:
            self._show_error("Password cannot be empty")
            return

        if data["password"] != data["confirm_password"]:
            self._show_error("Passwords do not match")
            return

        is_valid, errors = self._password_validator.validate(data["password"])
        if not is_valid:
            self._show_error("\n".join(errors))
            return

        self.user_created.emit(data)

    def get_form_data(self) -> dict:
        return {
            "username": self._username_input.text().strip(),
            "password": self._password_input.text(),
            "confirm_password": self._confirm_password_input.text(),
            "role": self._role_combo.currentText(),
            "staff_id": self._staff_id_input.text().strip(),
        }

    def clear_form(self):
        self._username_input.clear()
        self._password_input.clear()
        self._confirm_password_input.clear()
        self._staff_id_input.clear()
        self._role_combo.setCurrentIndex(0)

        self._strength_label.setText("Password Strength: Not entered")
        self._strength_label.setStyleSheet("")

    def _update_password_strength(self, password):
        if not password:
            self._strength_label.setText("Password Strength: Not entered")
            self._strength_label.setStyleSheet("")
            return

        strength_label, strength_score = self._password_validator.calculate_strength(password)
        is_valid, _ = self._password_validator.validate(password)

        if strength_score >= 80:
            color = "#27ae60"
        elif strength_score >= 60:
            color = "#2ecc71"
        elif strength_score >= 40:
            color = "#f39c12"
        elif strength_score >= 20:
            color = "#e67e22"
        else:
            color = "#e74c3c"

        status = "✓ Valid" if is_valid else "✗ Does not meet requirements"

        self._strength_label.setText(
            f"Password Strength: {strength_label} ({strength_score}%) - {status}"
        )
        self._strength_label.setStyleSheet(f"color: {color}; font-weight: bold;")

    def _show_error(self, message: str):
        self._strength_label.setText(f"Error: {message}")
        self._strength_label.setStyleSheet("color: red; font-weight: bold;")
