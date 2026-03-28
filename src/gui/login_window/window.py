"""
Login window for user authentication.
Provides login interface and handles authentication flow.
"""

import logging
from typing import Optional, Dict, Any

from PySide6.QtWidgets import QMainWindow, QWidget, QVBoxLayout, QMessageBox
from PySide6.QtCore import Signal

from .login_form import LoginForm
from .worker import LoginWorker
from two_factor_verify_dialog import TwoFactorVerifyDialog  # type: ignore
from auth.two_factor_auth import TwoFactorAuth  # type: ignore

logger = logging.getLogger(__name__)


class LoginWindow(QMainWindow):
    """Main login window for user authentication."""

    login_successful = Signal(object)  # user_data

    def __init__(self, user_manager, session_manager, db_connection=None):
        super().__init__()

        # Protected internal state
        self._user_manager = user_manager
        self._session_manager = session_manager
        self._worker: Optional[LoginWorker] = None
        self._pending_user_data: Optional[Dict[str, Any]] = None

        # Determine DB connection for 2FA
        self._db_connection = db_connection or getattr(user_manager, "db_connection", None)
        self._two_factor_auth = (
            TwoFactorAuth(self._db_connection)
            if self._db_connection
            else None
        )

        if not self._two_factor_auth:
            logger.warning("No database connection available for 2FA")

        self._setup_ui()

    # ---------------------------------------------------------
    # Read-only properties
    # ---------------------------------------------------------
    @property
    def user_manager(self):
        return self._user_manager

    @property
    def session_manager(self):
        return self._session_manager

    @property
    def db_connection(self):
        return self._db_connection

    @property
    def two_factor_auth(self):
        return self._two_factor_auth

    @property
    def login_form(self):
        return self._login_form

    # ---------------------------------------------------------
    # UI Setup
    # ---------------------------------------------------------
    def _setup_ui(self):
        self.setWindowTitle("Login - Store Database Management")
        self.setObjectName("login_window")
        self.setFixedSize(450, 500)
        self._center_on_screen()

        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        layout = QVBoxLayout(central_widget)
        layout.setContentsMargins(0, 0, 0, 0)

        self._login_form = LoginForm()
        self._login_form.login_requested.connect(self._on_login_requested)
        layout.addWidget(self._login_form)

    def _center_on_screen(self):
        from PySide6.QtGui import QGuiApplication

        screen = QGuiApplication.primaryScreen().geometry()
        geo = self.frameGeometry()
        geo.moveCenter(screen.center())
        self.move(geo.topLeft())

    # ---------------------------------------------------------
    # Authentication Flow
    # ---------------------------------------------------------
    def _on_login_requested(self, username: str, password: str):
        logger.info(f"Login requested for username: {username}")

        if self._worker and self._worker.isRunning():
            logger.warning("Login already in progress")
            return

        self._login_form.set_loading(True)

        self._worker = LoginWorker(self._user_manager, username, password)
        self._worker.authentication_complete.connect(self._on_authentication_complete)
        self._worker.finished.connect(self._cleanup_worker)
        self._worker.start()

    def _on_authentication_complete(self, success: bool, user_data: Optional[Dict[str, Any]]):
        self._login_form.set_loading(False)

        if not success:
            logger.warning("Login failed: Invalid credentials")
            self._login_form.show_error("Invalid username or password")
            return

        user_data = user_data or {}
        username = user_data.get("username")
        user_id = user_data.get("user_id")
        role = user_data.get("role")

        logger.info(f"Login successful for user: {username} (role={role})")

        # No 2FA available → skip
        if not self._two_factor_auth:
            self._complete_login(user_data)
            return

        # Admin must have 2FA
        if role == "Administrator":
            if not self._two_factor_auth.is_2fa_enabled(user_id):
                self._enforce_2fa_setup(user_data)
                return
            self._verify_2fa(user_data)
            return

        # Non-admin: optional 2FA
        if self._two_factor_auth.is_2fa_enabled(user_id):
            self._verify_2fa(user_data)
            return

        self._complete_login(user_data)

    # ---------------------------------------------------------
    # 2FA Setup & Verification
    # ---------------------------------------------------------
    def _enforce_2fa_setup(self, user_data):
        username = user_data.get("username")

        msg = QMessageBox(self)
        msg.setIcon(QMessageBox.Warning)
        msg.setWindowTitle("Two-Factor Authentication Required")
        msg.setText("As an Administrator, you must enable Two-Factor Authentication.")
        msg.setInformativeText(
            "Two-Factor Authentication (2FA) adds an extra layer of security.\n\n"
            "You will need:\n"
            "• A smartphone with an authenticator app\n"
            "• A few minutes to scan a QR code and verify your setup\n\n"
            "Click 'Setup 2FA' to continue."
        )
        msg.setStandardButtons(QMessageBox.Ok | QMessageBox.Cancel)
        msg.button(QMessageBox.Ok).setText("Setup 2FA")
        msg.button(QMessageBox.Cancel).setText("Cancel Login")

        if msg.exec() != QMessageBox.Ok:
            self._login_form.show_error("2FA setup is required for administrators")
            logger.warning(f"Administrator {username} cancelled 2FA setup")
            return

        from two_factor_setup_dialog import TwoFactorSetupDialog

        dialog = TwoFactorSetupDialog(
            user_data["user_id"],
            username,
            self._db_connection,
            self,
        )
        dialog.setup_completed.connect(
            lambda enabled: self._on_forced_2fa_setup(enabled, user_data)
        )
        dialog.exec()

    def _on_forced_2fa_setup(self, enabled: bool, user_data):
        if not enabled:
            self._login_form.show_error("2FA setup is required for administrators")
            return

        self._verify_2fa(user_data)

    def _verify_2fa(self, user_data):
        self._pending_user_data = user_data

        dialog = TwoFactorVerifyDialog(
            user_data["user_id"],
            user_data["username"],
            self._db_connection,
            self,
        )
        dialog.verification_successful.connect(self._on_2fa_verified)
        dialog.exec()

    def _on_2fa_verified(self):
        if self._pending_user_data:
            self._complete_login(self._pending_user_data)
            self._pending_user_data = None

    # ---------------------------------------------------------
    # Final Login Completion
    # ---------------------------------------------------------
    def _complete_login(self, user_data):
        self._session_manager.login(user_data)
        self._login_form.show_success("Login successful!")
        self.login_successful.emit(user_data)
        self.close()

    # ---------------------------------------------------------
    # Worker Cleanup
    # ---------------------------------------------------------
    def _cleanup_worker(self):
        if not self._worker:
            return

        try:
            self._worker.authentication_complete.disconnect(self._on_authentication_complete)
        except Exception:
            pass

        self._worker.deleteLater()
        self._worker = None

    # ---------------------------------------------------------
    # Cleanup on close
    # ---------------------------------------------------------
    def closeEvent(self, event):
        if self._worker and self._worker.isRunning():
            self._worker.quit()
            if not self._worker.wait(2000):
                self._worker.terminate()
                self._worker.wait()

        event.accept()
