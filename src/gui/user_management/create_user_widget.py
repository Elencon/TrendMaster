"""
Create User Widget - Form for creating new user accounts
"""

import sys
from pathlib import Path

# Add src to path for password policy
gui_path = Path(__file__).parent.parent.parent
src_path = gui_path / "src"
sys.path.insert(0, str(src_path))

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QFormLayout, QLabel,
    QLineEdit, QPushButton, QComboBox, QGroupBox, QTextEdit
)
from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QColor, QAction

from auth.password_policy import PasswordPolicyValidator


class CreateUserWidget(QWidget):
    """Widget for creating new user accounts"""
    
    user_created = Signal(str, str, str)  # username, password, role
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.password_validator = PasswordPolicyValidator()
        self._setup_ui()
        
        # Connect password field to strength checker
        self.password_input.textChanged.connect(self._update_password_strength)
    
    def _setup_ui(self):
        """Set up the user interface"""
        layout = QVBoxLayout(self)
        
        # Title
        title = QLabel("Create New User Account")
        title.setObjectName("section_title")
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)
        
        # Form
        self._create_form(layout)
        
        # Password requirements
        self._create_password_requirements(layout)
        
        # Role descriptions
        self._create_role_descriptions(layout)
        
        layout.addStretch()
        
        # Create button
        self._create_button(layout)
    
    def _create_form(self, layout):
        """Create the user input form"""
        form_group = QGroupBox("User Information")
        form_group.setObjectName("form_group")
        form_layout = QFormLayout()
        
        # Set consistent width for all inputs
        input_width = 300
        
        self.username_input = QLineEdit()
        self.username_input.setObjectName("username_input")
        self.username_input.setPlaceholderText("Enter username")
        self.username_input.setMaximumWidth(input_width)
        
        # Password field with eye button in horizontal layout
        password_layout = QHBoxLayout()
        self.password_input = QLineEdit()
        self.password_input.setObjectName("password_input")
        self.password_input.setEchoMode(QLineEdit.EchoMode.Password)
        self.password_input.setPlaceholderText("Enter strong password")
        self.password_input.setMaximumWidth(input_width - 30)
        
        self.password_toggle_btn = QPushButton("👁")
        self.password_toggle_btn.setObjectName("password_toggle_btn")
        self.password_toggle_btn.setFixedSize(30, 25)
        self.password_toggle_btn.setToolTip("Hold to show password")
        self.password_toggle_btn.pressed.connect(lambda: self._show_password(True))
        self.password_toggle_btn.released.connect(lambda: self._show_password(False))
        
        password_layout.addWidget(self.password_input)
        password_layout.addWidget(self.password_toggle_btn)
        password_layout.addStretch()
        
        # Confirm password field with eye button in horizontal layout
        confirm_layout = QHBoxLayout()
        self.confirm_password_input = QLineEdit()
        self.confirm_password_input.setObjectName("confirm_password_input")
        self.confirm_password_input.setEchoMode(QLineEdit.EchoMode.Password)
        self.confirm_password_input.setPlaceholderText("Confirm password")
        self.confirm_password_input.setMaximumWidth(input_width - 30)
        
        self.confirm_toggle_btn = QPushButton("👁")
        self.confirm_toggle_btn.setObjectName("confirm_toggle_btn")
        self.confirm_toggle_btn.setFixedSize(30, 25)
        self.confirm_toggle_btn.setToolTip("Hold to show password")
        self.confirm_toggle_btn.pressed.connect(lambda: self._show_confirm(True))
        self.confirm_toggle_btn.released.connect(lambda: self._show_confirm(False))
        
        confirm_layout.addWidget(self.confirm_password_input)
        confirm_layout.addWidget(self.confirm_toggle_btn)
        confirm_layout.addStretch()
        
        # Password strength indicator
        self.strength_label = QLabel("Password Strength: Not entered")
        self.strength_label.setObjectName("strength_label")
        
        self.role_combo = QComboBox()
        self.role_combo.setObjectName("role_combo")
        self.role_combo.addItems(["Employee", "Manager", "Administrator"])
        self.role_combo.setMaximumWidth(input_width)
        
        self.staff_id_input = QLineEdit()
        self.staff_id_input.setObjectName("staff_id_input")
        self.staff_id_input.setPlaceholderText("Optional: link to staff member")
        self.staff_id_input.setMaximumWidth(input_width)
        
        # Add rows to form
        form_layout.addRow("Username:", self.username_input)
        form_layout.addRow("Password:", password_layout)
        form_layout.addRow("", self.strength_label)
        form_layout.addRow("Confirm Password:", confirm_layout)
        form_layout.addRow("Role:", self.role_combo)
        form_layout.addRow("Staff ID:", self.staff_id_input)
        
        form_group.setLayout(form_layout)
        layout.addWidget(form_group)
    
    def _create_password_requirements(self, layout):
        """Create password requirements display"""
        req_group = QGroupBox("Password Requirements")
        req_group.setObjectName("password_req_group")
        req_layout = QVBoxLayout()
        
        req_text = QTextEdit()
        req_text.setObjectName("password_requirements")
        req_text.setReadOnly(True)
        req_text.setMaximumHeight(120)
        req_text.setPlainText(self.password_validator.get_requirements_text())
        
        req_layout.addWidget(req_text)
        req_group.setLayout(req_layout)
        layout.addWidget(req_group)
    
    def _create_role_descriptions(self, layout):
        """Create role permissions description"""
        desc_group = QGroupBox("Role Permissions")
        desc_group.setObjectName("role_desc_group")
        desc_layout = QVBoxLayout()
        
        roles = [
            "• Employee: View and export data",
            "• Manager: View, export, import, and modify data",
            "• Administrator: Full system access including user management"
        ]
        
        for role_text in roles:
            label = QLabel(role_text)
            label.setObjectName("role_description")
            desc_layout.addWidget(label)
        
        desc_group.setLayout(desc_layout)
        layout.addWidget(desc_group)
    
    def _create_button(self, layout):
        """Create the submit button"""
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        
        self.create_btn = QPushButton("Create User")
        self.create_btn.setObjectName("create_user_btn")
        self.create_btn.clicked.connect(self._on_create_clicked)
        btn_layout.addWidget(self.create_btn)
        
        layout.addLayout(btn_layout)
    
    def _on_create_clicked(self):
        """Emit signal with form data"""
        username = self.username_input.text().strip()
        password = self.password_input.text()
        confirm = self.confirm_password_input.text()
        role = self.role_combo.currentText()
        staff_id = self.staff_id_input.text().strip()
        
        # Validation is done in parent dialog
        self.user_created.emit(username, password, confirm)
    
    def get_form_data(self):
        """Get all form data"""
        return {
            'username': self.username_input.text().strip(),
            'password': self.password_input.text(),
            'confirm_password': self.confirm_password_input.text(),
            'role': self.role_combo.currentText(),
            'staff_id': self.staff_id_input.text().strip()
        }
    
    def clear_form(self):
        """Clear all form inputs"""
        self.username_input.clear()
        self.password_input.clear()
        self.confirm_password_input.clear()
        self.staff_id_input.clear()
        self.role_combo.setCurrentIndex(0)
        self.strength_label.setText("Password Strength: Not entered")
        self.strength_label.setStyleSheet("")
    
    def _update_password_strength(self, password):
        """Update password strength indicator"""
        if not password:
            self.strength_label.setText("Password Strength: Not entered")
            self.strength_label.setStyleSheet("")
            return
        
        # Calculate strength
        strength_label, strength_score = self.password_validator.calculate_strength(password)
        
        # Validate against policy
        is_valid, errors = self.password_validator.validate(password)
        
        # Set color based on strength
        if strength_score >= 80:
            color = "#27ae60"  # Green
        elif strength_score >= 60:
            color = "#2ecc71"  # Light green
        elif strength_score >= 40:
            color = "#f39c12"  # Orange
        elif strength_score >= 20:
            color = "#e67e22"  # Dark orange
        else:
            color = "#e74c3c"  # Red
        
        # Show validation status
        if is_valid:
            status = "✓ Valid"
        else:
            status = "✗ Does not meet requirements"
        
        self.strength_label.setText(f"Password Strength: {strength_label} ({strength_score}%) - {status}")
        self.strength_label.setStyleSheet(f"color: {color}; font-weight: bold;")
    
    def validate_password(self):
        """
        Validate password against policy
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        password = self.password_input.text()
        
        if not password:
            return False, "Password cannot be empty"
        
        is_valid, errors = self.password_validator.validate(password)
        
        if not is_valid:
            error_msg = "Password does not meet requirements:\n\n" + "\n".join(f"• {err}" for err in errors)
            return False, error_msg
        
        return True, ""
    
    def _show_password(self, show: bool):
        """Show/hide password while button is held"""
        if show:
            self.password_input.setEchoMode(QLineEdit.EchoMode.Normal)
        else:
            self.password_input.setEchoMode(QLineEdit.EchoMode.Password)
    
    def _show_confirm(self, show: bool):
        """Show/hide confirm password while button is held"""
        if show:
            self.confirm_password_input.setEchoMode(QLineEdit.EchoMode.Normal)
        else:
            self.confirm_password_input.setEchoMode(QLineEdit.EchoMode.Password)
