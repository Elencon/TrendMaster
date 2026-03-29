"""
Main entry point for the Store Database Management application.
Starts with login window, then opens dashboard based on user role.
"""

import sys
import os
from pathlib import Path

sys.dont_write_bytecode = True
os.environ['PYTHONDONTWRITEBYTECODE'] = '1'

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_PATH = PROJECT_ROOT / "src"
GUI_PATH = PROJECT_ROOT / "gui"

sys.path.insert(0, str(SRC_PATH))
sys.path.insert(0, str(GUI_PATH))

from PySide6.QtWidgets import QApplication, QMessageBox
from PySide6.QtGui import QIcon

import logging
from src.connect import connect_to_mysql
from src.auth.user_manager import UserManager
from src.auth.session import SessionManager
from src.auth.permissions import PermissionManager
from src.config import DatabaseConfig
from src.gui.login_window import LoginWindow  # type: ignore
from src.gui.dashboard_window import DashboardMainWindow  # type: ignore
from src.gui.themes import ThemeManager  # type: ignore
from src.gui.tabbed_window import TabbedMainWindow  # type: ignore
from src.gui.admin_window import ETLMainWindow  # type: ignore
from src.gui.user_management import UserManagementDialog  # type: ignore

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/application.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class Application:
    """Main application controller."""

    def __init__(self):
        self.app = QApplication.instance() or QApplication(sys.argv)
        self.app.setStyle("Fusion")

        # Set application icon
        icon_path = GUI_PATH / "themes" / "img" / "logo.png"
        if icon_path.exists():
            self.app.setWindowIcon(QIcon(str(icon_path)))

        # Initialize theme manager once for entire application
        self.theme_manager = ThemeManager()

        self.db_connection = None
        self.user_manager = None
        self.session_manager = SessionManager()

        # Use tabbed window as main container
        self.main_window = None
        self.login_widget = None
        self.dashboard_widget = None

    def initialize_database(self) -> bool:
        """Initialize database connection."""
        try:
            db_config = DatabaseConfig().to_dict()
            self.db_connection = connect_to_mysql(db_config)

            if not self.db_connection:
                logger.error("Failed to establish database connection")
                self.show_error("Database Error",
                              "Could not connect to database.\nPlease check your configuration.")
                return False

            self.user_manager = UserManager(self.db_connection)
            logger.info("Database connection established")
            return True

        except Exception as e:
            logger.error(f"Database initialization error: {e}")
            self.show_error("Database Error", f"Failed to initialize database:\n{str(e)}")
            return False

    def show_login(self):
        """Show standalone login window."""
        # Create login window with db_connection for 2FA
        self.login_widget = LoginWindow(self.user_manager, self.session_manager, self.db_connection)
        self.login_widget.login_successful.connect(self.on_login_successful)

        # Apply theme
        self.theme_manager.apply_current_theme(self.app)

        # Show login window
        self.login_widget.show()

    def on_login_successful(self, user_data):
        """Handle successful login."""
        username = user_data.get('username')
        role = user_data.get('role')

        logger.info(f"Login successful: {username} ({role})")

        # Ensure old main window is gone
        if self.main_window:
            logger.warning("Old main window still exists, cleaning up...")
            try:
                self.main_window.hide()
                self.main_window.deleteLater()
            except:
                pass
            self.main_window = None

        # Create fresh main tabbed window
        logger.info("Creating new main window...")
        self.main_window = TabbedMainWindow()

        # Open dashboard
        self.show_dashboard()

        # Apply theme
        self.theme_manager.apply_current_theme(self.app)

        # Show main window
        self.main_window.show()
        self.main_window.raise_()
        self.main_window.activateWindow()

        logger.info("Main window displayed with dashboard")

    def show_dashboard(self):
        """Show dashboard in tabbed window based on user role."""
        # Pass theme manager to dashboard to avoid double theme application
        self.dashboard_widget = DashboardMainWindow(theme_manager=self.theme_manager)

        # Connect signals
        self.dashboard_widget.logout_requested.connect(self.on_logout)
        self.dashboard_widget.admin_window_requested.connect(self.open_admin_tab)
        self.dashboard_widget.user_management_requested.connect(self.open_user_management_tab)

        # Update dashboard based on role
        user_role = self.session_manager.get_role()
        username = self.session_manager.get_username()

        if not PermissionManager.can_manage_database(user_role):
            # Hide/disable Manage Database button for non-administrators (if present)
            if hasattr(self.dashboard_widget, 'manage_db_btn') and self.dashboard_widget.manage_db_btn is not None:
                self.dashboard_widget.manage_db_btn.setEnabled(False)
                self.dashboard_widget.manage_db_btn.setToolTip("Administrator access required")
                logger.info(f"Manage Database button disabled for role: {user_role}")

        # Add dashboard as tab (non-closable main tab)
        tab_title = f"Dashboard - {username} ({user_role})"
        self.main_window.add_tab(self.dashboard_widget, tab_title, closable=False)

        # Update main window title
        self.main_window.setWindowTitle(f"ETL Pipeline Manager - {username}")

    def open_admin_tab(self):
        """Open Admin window as a new tab."""
        # Check if tab already exists
        if self.main_window.get_tab_by_title("Database Management"):
            logger.info("Admin tab already open")
            return

        try:
            # Create admin window widget
            admin_widget = ETLMainWindow()
            self.main_window.add_tab(admin_widget, "Database Management", closable=True)
            logger.info("Opened Database Management tab")
        except Exception as e:
            logger.error(f"Failed to open admin tab: {e}")
            self.show_error("Error", f"Failed to open Database Management:\n{str(e)}")

    def open_user_management_tab(self):
        """Open User Management as a new tab."""
        # Check if tab already exists
        if self.main_window.get_tab_by_title("User Management"):
            logger.info("User Management tab already open")
            return

        try:
            # Create user management widget
            user_mgmt_widget = UserManagementDialog()
            self.main_window.add_tab(user_mgmt_widget, "User Management", closable=True)
            logger.info("Opened User Management tab")
        except Exception as e:
            logger.error(f"Failed to open user management tab: {e}")
            self.show_error("Error", f"Failed to open User Management:\n{str(e)}")

    def on_logout(self):
        """Handle logout - close main window and show login."""
        from PySide6.QtCore import QTimer

        logger.info("Logout - returning to login")

        # Clear session using correct method
        self.session_manager.logout()

        # Store reference to main window
        main_win = self.main_window

        # Set references to None FIRST (so Qt doesn't exit when window closes)
        self.main_window = None
        self.dashboard_widget = None

        # Create new login window
        new_login = LoginWindow(self.user_manager, self.session_manager, self.db_connection)
        new_login.login_successful.connect(self.on_login_successful)

        # Apply theme to new login
        self.theme_manager.apply_current_theme(self.app)

        # Show new login window
        self.login_widget = new_login
        self.login_widget.show()
        self.login_widget.raise_()
        self.login_widget.activateWindow()

        # Use QTimer to close main window slightly after login appears
        def close_main_window():
            if main_win:
                main_win.hide()
                main_win.deleteLater()

        QTimer.singleShot(50, close_main_window)

        logger.info("Logout completed")

    def show_error(self, title: str, message: str):
        """Show error message box."""
        QMessageBox.critical(None, title, message)

    def run(self) -> int:
        """Run the application."""
        # Initialize database
        if not self.initialize_database():
            return 1

        # Show login window
        self.show_login()

        # Run application
        return self.app.exec()


def main():
    """Application entry point."""
    try:
        app = Application()
        sys.exit(app.run())
    except Exception as e:
        logger.critical(f"Application failed to start: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
