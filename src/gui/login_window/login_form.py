import sys
from PySide6.QtWidgets import (
    QApplication, QWidget, QLabel, QLineEdit,
    QPushButton, QVBoxLayout, QHBoxLayout
)
from PySide6.QtCore import Qt, Signal


# ------------------------------------------------------------
# LOGIN FORM (VIEW)
# ------------------------------------------------------------
class LoginForm(QWidget):
    login_requested = Signal(str, str)
    login_successful = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(20)
        layout.setContentsMargins(40, 40, 40, 40)

        # Title
        title = QLabel("Store Database Login")
        title.setAlignment(Qt.AlignCenter)
        title.setStyleSheet("font-size: 18px; font-weight: bold;")
        layout.addWidget(title)

        # Username
        layout.addWidget(QLabel("Username:"))
        self._username_input = QLineEdit()
        self._username_input.setPlaceholderText("Enter your username")
        layout.addWidget(self._username_input)

        # Password
        layout.addWidget(QLabel("Password:"))
        pw_layout = QHBoxLayout()

        self._password_input = QLineEdit()
        self._password_input.setEchoMode(QLineEdit.Password)

        self._toggle_btn = QPushButton("👁")
        self._toggle_btn.setFixedWidth(35)
        self._toggle_btn.pressed.connect(self._reveal_password)
        self._toggle_btn.released.connect(self._hide_password)

        pw_layout.addWidget(self._password_input)
        pw_layout.addWidget(self._toggle_btn)
        layout.addLayout(pw_layout)

        # Login button
        self._login_btn = QPushButton("Login")
        self._login_btn.clicked.connect(self._emit_login)
        layout.addWidget(self._login_btn)

        # Status label
        self._status = QLabel("")
        self._status.setAlignment(Qt.AlignCenter)
        layout.addWidget(self._status)

        # Keyboard flow
        self._username_input.returnPressed.connect(self._focus_password)
        self._password_input.returnPressed.connect(self._emit_login)

    # -------------------------
    # Read‑only properties
    # -------------------------
    @property
    def username(self):
        return self._username_input.text().strip()

    @property
    def password(self):
        return self._password_input.text().strip()

    # -------------------------
    # UI State
    # -------------------------
    def set_loading(self, loading: bool):
        self._login_btn.setEnabled(not loading)
        self._login_btn.setText("Logging in..." if loading else "Login")

    def show_error(self, msg):
        self._status.setText(f"❌ {msg}")
        self._status.setStyleSheet("color: red;")

    def show_success(self, msg):
        self._status.setText(f"✅ {msg}")
        self._status.setStyleSheet("color: green;")

    def clear_status(self):
        self._status.clear()
        self._status.setStyleSheet("")

    # -------------------------
    # Internal
    # -------------------------
    def _emit_login(self):
        self.clear_status()
        self.set_loading(True)
        self.login_requested.emit(self.username, self.password)

    def _focus_password(self):
        self._password_input.setFocus()

    def _reveal_password(self):
        self._password_input.setEchoMode(QLineEdit.Normal)

    def _hide_password(self):
        self._password_input.setEchoMode(QLineEdit.Password)


# ------------------------------------------------------------
# LOCAL CONTROLLER
# ------------------------------------------------------------
class _LocalController:
    """Controller used only for local testing."""

    def __init__(self, view: LoginForm):
        self._view = view
        self._auth = _LocalAuth()

        view.login_requested.connect(self._handle_login)

    def _handle_login(self, username, password):
        ok, msg = self._auth.authenticate(username, password)

        if ok:
            self._view.show_success("Login successful")
            self._view.login_successful.emit()
        else:
            self._view.show_error(msg)

        self._view.set_loading(False)


# ------------------------------------------------------------
# LOCAL TEST RUNNER
# ------------------------------------------------------------

# ------------------------------------------------------------
# LOCAL AUTH SERVICE (MOCK)
# ------------------------------------------------------------
class _LocalAuth:
    """Simple local authentication mock for testing."""

    def authenticate(self, username, password):
        print(f"[LOCAL TEST] username={username}, password={password}")

        if username == "admin" and password == "1234":
            return True, "OK"

        return False, "Invalid username or password"



if __name__ == "__main__":
    app = QApplication(sys.argv)

    view = LoginForm()
    controller = _LocalController(view)

    view.show()
    sys.exit(app.exec())
