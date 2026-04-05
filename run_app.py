"""
Main entry point for the Store Database Management application.
Starts with login window, then opens dashboard based on user role.
"""

import sys
import os
from pathlib import Path

# Prevent Python cache files from being created
sys.dont_write_bytecode = True
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"

# ---------------------------------------------------------
# Locate project root (TrendMaster folder)
# ---------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent

# Ensure project root is importable
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ---------------------------------------------------------
# Imports
# ---------------------------------------------------------
from PySide6.QtWidgets import QApplication, QMessageBox
from PySide6.QtGui import QIcon

import logging

from src.bootstrap import initialize
from src.database import connect_to_mysql
from src.auth.user_manager import UserManager
from src.auth.session import SessionManager
from src.auth.permissions import PermissionManager
from src.config import DatabaseConfig

from src.gui.login_window import LoginWindow
from src.gui.dashboard_window import DashboardMainWindow
from src.gui.themes import ThemeManager
from src.gui.tabbed_window import TabbedMainWindow
from src.gui.admin_window import ETLMainWindow
from src.gui.user_management import UserManagementDialog

from src.config.path_config import GUI_PATH


logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# Application Controller
# ---------------------------------------------------------
class Application:
    """Main application controller."""

    def __init__(self):
        # Initialize logging + directories FIRST
        initialize()

        # Create QApplication
        self.app = QApplication.instance() or QApplication(sys.argv)
        self.app.setStyle("Fusion")

        # Set application icon
        icon_path = GUI_PATH / "themes" / "img" / "logo.png"
        if icon_path.exists():
            self.app.setWindowIcon(QIcon(str(icon_path)))

        # Core components
        self.theme_manager = ThemeManager()
        self.db_connection = None
        self.user_manager = None
        self.session_manager = SessionManager()

        # GUI components
        self.main_window = None
        self.login_widget = None
        self.dashboard_widget = None

    # -----------------------------------------------------
    # Database Initialization
    # -----------------------------------------------------
    def initialize_database(self) -> bool:
        try:
            db_config = DatabaseConfig().to_dict()
            self.db_connection = connect_to_mysql(db_config)

            if not self.db_connection:
                logger.error("Failed to establish database connection")
                self.show_error(
                    "Database Error",
                    "Could not connect to database.\nPlease check your configuration.",
                )
                return False

            self.user_manager = UserManager(self.db_connection)
            logger.info("Database connection established")
            return True

        except Exception as e:
            logger.error(f"Database initialization error: {e}")
            self.show_error("Database Error", f"Failed to initialize database:\n{str(e)}")
            return False

    # -----------------------------------------------------
    # Login Window
    # -----------------------------------------------------
    def show_login(self):
        self.login_widget = LoginWindow(
            self.user_manager, self.session_manager, self.db_connection
        )
        self.login_widget.login_successful.connect(self.on_login_successful)

        self.theme_manager.apply_current_theme(self.app)
        self.login_widget.show()

    # -----------------------------------------------------
    # Login Success Handler
    # -----------------------------------------------------
    def on_login_successful(self, user_data):
        username = user_data.get("username")
        role = user_data.get("role")

        logger.info(f"Login successful: {username} ({role})")

        # Clean up old main window if needed
        if self.main_window:
            try:
                self.main_window.hide()
                self.main_window.deleteLater()
            except:
                pass
            self.main_window = None

        # Create new main window
        self.main_window = TabbedMainWindow()

        # Show dashboard
        self.show_dashboard()

        self.theme_manager.apply_current_theme(self.app)
        self.main_window.show()
        self.main_window.raise_()
        self.main_window.activateWindow()

        logger.info("Main window displayed with dashboard")

    # -----------------------------------------------------
    # Dashboard
    # -----------------------------------------------------
    def show_dashboard(self):
        self.dashboard_widget = DashboardMainWindow(theme_manager=self.theme_manager)

        self.dashboard_widget.logout_requested.connect(self.on_logout)
        self.dashboard_widget.admin_window_requested.connect(self.open_admin_tab)
        self.dashboard_widget.user_management_requested.connect(
            self.open_user_management_tab
        )

        user_role = self.session_manager.get_role()
        username = self.session_manager.get_username()

        # Disable DB management for non-admins
        if not PermissionManager.can_manage_database(user_role):
            if hasattr(self.dashboard_widget, "manage_db_btn"):
                self.dashboard_widget.manage_db_btn.setEnabled(False)
                self.dashboard_widget.manage_db_btn.setToolTip(
                    "Administrator access required"
                )
                logger.info(f"Manage Database button disabled for role: {user_role}")

        tab_title = f"Dashboard - {username} ({user_role})"
        self.main_window.add_tab(self.dashboard_widget, tab_title, closable=False)
        self.main_window.setWindowTitle(f"ETL Pipeline Manager - {username}")

    # -----------------------------------------------------
    # Admin Tab
    # -----------------------------------------------------
    def open_admin_tab(self):
        if self.main_window.get_tab_by_title("Database Management"):
            logger.info("Admin tab already open")
            return

        try:
            admin_widget = ETLMainWindow()
            self.main_window.add_tab(admin_widget, "Database Management", closable=True)
            logger.info("Opened Database Management tab")
        except Exception as e:
            logger.error(f"Failed to open admin tab: {e}")
            self.show_error("Error", f"Failed to open Database Management:\n{str(e)}")

    # -----------------------------------------------------
    # User Management Tab
    # -----------------------------------------------------
    def open_user_management_tab(self):
        if self.main_window.get_tab_by_title("User Management"):
            logger.info("User Management tab already open")
            return

        try:
            user_mgmt_widget = UserManagementDialog()
            self.main_window.add_tab(user_mgmt_widget, "User Management", closable=True)
            logger.info("Opened User Management tab")
        except Exception as e:
            logger.error(f"Failed to open user management tab: {e}")
            self.show_error("Error", f"Failed to open User Management:\n{str(e)}")

    # -----------------------------------------------------
    # Logout
    # -----------------------------------------------------
    def on_logout(self):
        from PySide6.QtCore import QTimer

        logger.info("Logout - returning to login")

        self.session_manager.logout()

        main_win = self.main_window
        self.main_window = None
        self.dashboard_widget = None

        new_login = LoginWindow(
            self.user_manager, self.session_manager, self.db_connection
        )
        new_login.login_successful.connect(self.on_login_successful)

        self.theme_manager.apply_current_theme(self.app)

        self.login_widget = new_login
        self.login_widget.show()
        self.login_widget.raise_()
        self.login_widget.activateWindow()

        def close_main_window():
            if main_win:
                main_win.hide()
                main_win.deleteLater()

        QTimer.singleShot(50, close_main_window)

        logger.info("Logout completed")

    # -----------------------------------------------------
    # Error Dialog
    # -----------------------------------------------------
    def show_error(self, title: str, message: str):
        QMessageBox.critical(None, title, message)

    # -----------------------------------------------------
    # Run Application
    # -----------------------------------------------------
    def run(self) -> int:
        if not self.initialize_database():
            return 1

        self.show_login()
        return self.app.exec()


# ---------------------------------------------------------
# Entry Point
# ---------------------------------------------------------
def main():
    try:
        app = Application()
        sys.exit(app.run())
    except Exception as e:
        logger.critical(f"Application failed to start: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()