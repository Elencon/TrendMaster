"""
UI components for the login window.
Contains input fields, buttons, and styling for login interface.
"""

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit, 
    QPushButton, QFrame, QMessageBox
)
from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QFont, QPixmap


class LoginForm(QWidget):
    """Login form with username and password fields."""
    
    login_requested = Signal(str, str)  # username, password
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setup_ui()
    
    def setup_ui(self):
        """Setup the login form UI."""
        layout = QVBoxLayout(self)
        layout.setSpacing(20)
        layout.setContentsMargins(40, 40, 40, 40)
        
        # Title
        title_label = QLabel("Store Database Login")
        title_label.setObjectName("login_title")
        title_font = QFont()
        title_font.setPointSize(18)
        title_font.setBold(True)
        title_label.setFont(title_font)
        title_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(title_label)
        
        # Subtitle
        subtitle_label = QLabel("Enter your credentials to access the system")
        subtitle_label.setObjectName("login_subtitle")
        subtitle_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(subtitle_label)
        
        # Spacer
        layout.addSpacing(20)
        
        # Username field
        username_label = QLabel("Username:")
        username_label.setObjectName("field_label")
        layout.addWidget(username_label)
        
        # Username field with matching width to password
        username_layout = QHBoxLayout()
        username_layout.setSpacing(5)
        username_layout.setContentsMargins(0, 0, 0, 0)
        
        self.username_input = QLineEdit()
        self.username_input.setObjectName("username_input")
        self.username_input.setPlaceholderText("Enter your username")
        self.username_input.setMinimumHeight(35)
        
        username_layout.addWidget(self.username_input)
        username_layout.addSpacing(35)  # Match eye button exact width
        
        layout.addLayout(username_layout)
        
        layout.addSpacing(15)
        
        # Password field
        password_label = QLabel("Password:")
        password_label.setObjectName("field_label")
        layout.addWidget(password_label)
        
        # Password field with eye button
        password_layout = QHBoxLayout()
        password_layout.setSpacing(5)
        password_layout.setContentsMargins(0, 0, 0, 0)
        
        self.password_input = QLineEdit()
        self.password_input.setObjectName("password_input")
        self.password_input.setPlaceholderText("Enter your password")
        self.password_input.setEchoMode(QLineEdit.Password)
        self.password_input.setMinimumHeight(35)
        
        self.password_toggle_btn = QPushButton("👁")
        self.password_toggle_btn.setObjectName("password_toggle_btn")
        self.password_toggle_btn.setMaximumWidth(35)
        self.password_toggle_btn.setMinimumHeight(35)
        self.password_toggle_btn.setToolTip("Hold to show password")
        self.password_toggle_btn.setStyleSheet("color: #AAAAAA;")  # Light grey color
        self.password_toggle_btn.pressed.connect(lambda: self._show_password(True))
        self.password_toggle_btn.released.connect(lambda: self._show_password(False))
        
        password_layout.addWidget(self.password_input)
        password_layout.addWidget(self.password_toggle_btn)
        
        layout.addLayout(password_layout)
        
        # Connect Enter key to login
        self.username_input.returnPressed.connect(self.on_login_clicked)
        self.password_input.returnPressed.connect(self.on_login_clicked)
        
        # Spacer
        layout.addSpacing(10)
        
        # Login button
        self.login_btn = QPushButton("Login")
        self.login_btn.setObjectName("login_btn")
        self.login_btn.setMinimumHeight(40)
        self.login_btn.clicked.connect(self.on_login_clicked)
        layout.addWidget(self.login_btn)
        
        # Status label
        self.status_label = QLabel("")
        self.status_label.setObjectName("status_label")
        self.status_label.setAlignment(Qt.AlignCenter)
        self.status_label.setWordWrap(True)
        layout.addWidget(self.status_label)
        
        # Stretch to push everything up
        layout.addStretch()
    
    def on_login_clicked(self):
        """Handle login button click."""
        username = self.username_input.text().strip()
        password = self.password_input.text()
        
        if not username:
            self.show_error("Please enter your username")
            return
        
        if not password:
            self.show_error("Please enter your password")
            return
        
        # Clear status and disable button
        self.status_label.clear()
        self.login_btn.setEnabled(False)
        self.login_btn.setText("Logging in...")
        
        # Emit signal
        self.login_requested.emit(username, password)
    
    def show_error(self, message: str):
        """Show error message."""
        self.status_label.setText(f"❌ {message}")
        self.status_label.setStyleSheet("color: #f44336;")
    
    def show_success(self, message: str):
        """Show success message."""
        self.status_label.setText(f"✅ {message}")
        self.status_label.setStyleSheet("color: #4caf50;")
    
    def reset_login_button(self):
        """Reset login button to original state."""
        self.login_btn.setEnabled(True)
        self.login_btn.setText("Login")
    
    def clear_fields(self):
        """Clear all input fields."""
        self.username_input.clear()
        self.password_input.clear()
        self.status_label.clear()
    
    def _show_password(self, show: bool):
        """Show/hide password while button is held"""
        if show:
            self.password_input.setEchoMode(QLineEdit.EchoMode.Normal)
        else:
            self.password_input.setEchoMode(QLineEdit.EchoMode.Password)
