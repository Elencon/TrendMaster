"""
Login window for user authentication.
Provides login interface and handles authentication flow.
"""

import sys
import logging
from PySide6.QtWidgets import QMainWindow, QWidget, QVBoxLayout, QMessageBox
from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QIcon

from .ui_components import LoginForm
from .worker import LoginWorker
from ..two_factor_verify_dialog import TwoFactorVerifyDialog  # type: ignore
from auth.two_factor_auth import TwoFactorAuth  # type: ignore

logger = logging.getLogger(__name__)


class LoginWindow(QMainWindow):
    """Main login window for user authentication."""
    
    # Signal emitted when login is successful
    login_successful = Signal(object)  # user_data
    
    def __init__(self, user_manager, session_manager, db_connection=None):
        """
        Initialize LoginWindow.
        
        Args:
            user_manager: UserManager instance for authentication
            session_manager: SessionManager instance for session handling
            db_connection: Database connection for 2FA (optional, extracted from user_manager if not provided)
        """
        super().__init__()
        self.user_manager = user_manager
        self.session_manager = session_manager
        self.worker = None
        
        # Get database connection for 2FA
        # If not provided, extract from user_manager
        self.db_connection = db_connection or getattr(user_manager, 'db_connection', None)
        
        if self.db_connection:
            self.two_factor_auth = TwoFactorAuth(self.db_connection)
        else:
            logger.warning("No database connection available for 2FA")
            self.two_factor_auth = None
            
        self.pending_user_data = None  # Store user data until 2FA is verified
        
        self.setup_ui()
        # Don't apply custom styles - use theme system
    
    def setup_ui(self):
        """Setup the login window UI."""
        self.setWindowTitle("Login - Store Database Management")
        self.setObjectName("login_window")
        self.setFixedSize(450, 500)
        
        # Center window on screen
        self.center_on_screen()
        
        # Create central widget
        central_widget = QWidget()
        central_widget.setObjectName("login_form")
        self.setCentralWidget(central_widget)
        
        # Main layout
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)
        
        # Add login form
        self.login_form = LoginForm()
        self.login_form.login_requested.connect(self.on_login_requested)
        main_layout.addWidget(self.login_form)
    
    def center_on_screen(self):
        """Center the window on the screen."""
        from PySide6.QtGui import QGuiApplication
        screen = QGuiApplication.primaryScreen().geometry()
        window_geometry = self.frameGeometry()
        center_point = screen.center()
        window_geometry.moveCenter(center_point)
        self.move(window_geometry.topLeft())
    
    def on_login_requested(self, username: str, password: str):
        """
        Handle login request.
        
        Args:
            username: Username entered
            password: Password entered
        """
        logger.info(f"Login requested for username: {username}")
        
        # Create and start worker thread
        self.worker = LoginWorker(self.user_manager, username, password)
        self.worker.authentication_complete.connect(self.on_authentication_complete)
        self.worker.start()
    
    def on_authentication_complete(self, success: bool, user_data: object):
        """
        Handle authentication completion.
        
        Args:
            success: Whether authentication was successful
            user_data: User data if successful, None otherwise
        """
        # Reset login button
        self.login_form.reset_login_button()
        
        if success:
            logger.info(f"Login successful for user: {user_data.get('username')}")
            
            # Check if user is administrator
            user_role = user_data.get('role')
            user_id = user_data.get('user_id')
            
            # For administrators, check 2FA status
            if user_role == 'Administrator':
                if not self.two_factor_auth:
                    logger.error("2FA not available - missing database connection")
                    self._complete_login(user_data)
                    return
                    
                is_2fa_enabled = self.two_factor_auth.is_2fa_enabled(user_id)
                
                if not is_2fa_enabled:
                    # Administrator must enable 2FA before proceeding
                    self._enforce_2fa_setup(user_data)
                    return
                else:
                    # 2FA is enabled, verify it
                    self._verify_2fa(user_data)
                    return
            
            # For non-administrators, check if 2FA is enabled (optional)
            if self.two_factor_auth.is_2fa_enabled(user_id):
                self._verify_2fa(user_data)
                return
            
            # No 2FA required/enabled, proceed with login
            self._complete_login(user_data)
        else:
            logger.warning("Login failed: Invalid credentials")
            self.login_form.show_error("Invalid username or password")
    
    def _enforce_2fa_setup(self, user_data):
        """Enforce 2FA setup for administrators"""
        username = user_data.get('username')
        
        # Show mandatory setup message
        msg = QMessageBox(self)
        msg.setIcon(QMessageBox.Warning)
        msg.setWindowTitle("Two-Factor Authentication Required")
        msg.setText(f"As an Administrator, you must enable Two-Factor Authentication.")
        msg.setInformativeText(
            "Two-Factor Authentication (2FA) adds an extra layer of security to your account.\n\n"
            "You will need:\n"
            "• A smartphone with an authenticator app (Google Authenticator, Authy, etc.)\n"
            "• A few minutes to scan a QR code and verify your setup\n\n"
            "Click 'Setup 2FA' to continue."
        )
        msg.setStandardButtons(QMessageBox.Ok | QMessageBox.Cancel)
        msg.button(QMessageBox.Ok).setText("Setup 2FA")
        msg.button(QMessageBox.Cancel).setText("Cancel Login")
        
        result = msg.exec()
        
        if result == QMessageBox.Ok:
            # Open 2FA setup dialog
            from two_factor_setup_dialog import TwoFactorSetupDialog
            user_id = user_data.get('user_id')
            
            dialog = TwoFactorSetupDialog(user_id, username, self.db_connection, self)
            dialog.setup_completed.connect(lambda enabled: self._on_forced_2fa_setup(enabled, user_data))
            dialog.exec()
        else:
            # User cancelled, show error and don't log in
            self.login_form.show_error("2FA setup is required for administrators")
            logger.warning(f"Administrator {username} cancelled mandatory 2FA setup")
    
    def _on_forced_2fa_setup(self, enabled: bool, user_data):
        """Handle forced 2FA setup completion"""
        if enabled:
            # Setup complete, now verify the code
            self._verify_2fa(user_data)
        else:
            # Setup was cancelled or failed
            self.login_form.show_error("2FA setup is required for administrators")
            logger.warning(f"Administrator {user_data.get('username')} failed to complete 2FA setup")
    
    def _verify_2fa(self, user_data):
        """Verify 2FA code"""
        self.pending_user_data = user_data
        user_id = user_data.get('user_id')
        username = user_data.get('username')
        
        # Show verification dialog
        dialog = TwoFactorVerifyDialog(user_id, username, self.db_connection, self)
        dialog.verification_successful.connect(self._on_2fa_verified)
        dialog.exec()
    
    def _on_2fa_verified(self):
        """Handle successful 2FA verification"""
        if self.pending_user_data:
            self._complete_login(self.pending_user_data)
            self.pending_user_data = None
    
    def _complete_login(self, user_data):
        """Complete the login process"""
        # Start session
        self.session_manager.login(user_data)
        
        # Show success message
        self.login_form.show_success("Login successful!")
        
        # Emit success signal
        self.login_successful.emit(user_data)
        
        # Close login window
        self.close()
    
    def closeEvent(self, event):
        """Handle window close event."""
        # If worker is still running, wait for it
        if self.worker and self.worker.isRunning():
            self.worker.wait()
        event.accept()
