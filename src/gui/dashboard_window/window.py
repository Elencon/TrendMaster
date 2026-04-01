"""Dashboard window class for ETL Pipeline Manager - Refactored for maintainability"""

import sys
import os
import logging

from PySide6.QtWidgets import (QMainWindow, QVBoxLayout, QWidget, QMessageBox,
                               QApplication, QMenu)
from PySide6.QtCore import QSettings, Signal, QPoint
from PySide6.QtGui import QAction

from src.gui.themes import ThemeManager
from src.auth.session import SessionManager  # type: ignore
from src.gui.two_factor_setup_dialog import TwoFactorSetupDialog  # type: ignore
from .ui_builder import DashboardUIBuilder
from .data_handler import DashboardDataHandler

sys.dont_write_bytecode = True
os.environ['PYTHONDONTWRITEBYTECODE'] = '1'

logger = logging.getLogger(__name__)


class DashboardMainWindow(QMainWindow):
    """Dashboard window with clean architecture and theme support"""

    logout_requested = Signal()  # Signal emitted when user clicks logout
    admin_window_requested = Signal()  # Signal to open admin window in tab
    user_management_requested = Signal()  # Signal to open user management in tab

    def __init__(self, theme_manager=None):
        super().__init__()
        self.settings = QSettings("ETL Solutions", "ETL Pipeline Dashboard")

        # Use provided theme manager or create new one (for standalone use)
        self.theme_manager = theme_manager if theme_manager else ThemeManager()
        self._owns_theme_manager = theme_manager is None

        # Initialize UI builder and data handler
        self.ui_builder = DashboardUIBuilder(self)
        self.data_handler = DashboardDataHandler(self)

        self._setup_ui()
        self._load_settings()
        self.data_handler.initialize_dashboard()

    def _setup_ui(self):
        """Set up the user interface"""
        self.setWindowTitle("ETL Pipeline Dashboard")
        self.setGeometry(100, 100, 1050, 850)

        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        main_layout = QVBoxLayout(central_widget)

        # Create all UI sections using the builder
        self.ui_builder.create_all_sections(main_layout)
        self.toolbar = self.ui_builder.create_toolbar()

        main_layout.addStretch()

        self.statusBar().showMessage("Ready")

    def _load_settings(self):
        """Load user settings"""
        geometry = self.settings.value("geometry")
        if geometry:
            self.restoreGeometry(geometry)

        # Only load theme settings if we own the theme manager (standalone mode)
        if self._owns_theme_manager:
            saved_theme = self.settings.value("theme/current_theme", "dark", type=str)
            self.theme_manager.set_theme(saved_theme)
            self._apply_theme()

    def _save_settings(self):
        """Save user settings"""
        self.settings.setValue("geometry", self.saveGeometry())
        self.settings.setValue("theme/current_theme", self.theme_manager.get_current_theme_name())

    def _apply_theme(self):
        """Apply current theme using theme manager"""
        app = QApplication.instance()
        self.theme_manager.apply_current_theme(app)

    # ==================== Event Handlers ====================

    def generate_customer_pdf(self):
        """Generate PDF report for selected customer - delegates to data handler"""
        self.data_handler.generate_customer_pdf()

    def show_settings_menu(self):
        """Show settings menu when toolbar button is clicked"""
        menu = QMenu(self)

        # Theme toggle action
        theme_action = QAction("Toggle Theme", self)
        theme_action.triggered.connect(self.toggle_theme)
        menu.addAction(theme_action)

        # Security Settings action (2FA)
        security_action = QAction("Security Settings (2FA)", self)
        security_action.triggered.connect(self.show_security_settings)
        menu.addAction(security_action)

        # Get the settings action widget position
        for action in self.toolbar.actions():
            if action.text() == "⚙":
                widget = self.toolbar.widgetForAction(action)
                if widget:
                    # Show menu directly below the button
                    pos = widget.mapToGlobal(QPoint(0, widget.height()))
                    menu.exec(pos)
                    return

        # Fallback: show at toolbar position
        menu.exec(self.toolbar.mapToGlobal(self.toolbar.rect().topRight()))

    def toggle_theme(self):
        """Toggle between light and dark theme"""
        self.theme_manager.toggle_theme()
        self._apply_theme()
        self.update()
        self.repaint()
        theme_name = self.theme_manager.get_current_theme_name()
        self.statusBar().showMessage(f"Switched to {theme_name} theme")

    def show_security_settings(self):
        """Show 2FA security settings dialog"""
        session = SessionManager()
        user_id = session.get_user_id()
        username = session.get_username()

        if not user_id:
            QMessageBox.warning(self, "Error", "Unable to determine current user")
            return

        # Get database connection from connect module
        from database import connect_to_mysql
        from config import DatabaseConfig

        try:
            db_config = DatabaseConfig().to_dict()
            db_conn = connect_to_mysql(db_config)

            if not db_conn:
                QMessageBox.warning(self, "Error", "Could not connect to database")
                return

            # Open 2FA setup dialog
            dialog = TwoFactorSetupDialog(user_id, username, db_conn, self)
            dialog.setup_completed.connect(self._on_2fa_setup_completed)
            dialog.exec()
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Failed to open security settings: {str(e)}")

    def _on_2fa_setup_completed(self, enabled: bool):
        """Handle 2FA setup completion"""
        if enabled:
            self.statusBar().showMessage("Two-Factor Authentication enabled successfully")
        else:
            self.statusBar().showMessage("Two-Factor Authentication disabled")

    def open_admin_window(self):
        """Request to open admin window as tab"""
        self.admin_window_requested.emit()
        self.statusBar().showMessage("Opening admin window...")

    def open_user_management(self):
        """Request to open user management as tab (Administrator only)"""
        # Double-check permissions
        session = SessionManager()
        user_role = session.get_role()

        if user_role != "Administrator":
            QMessageBox.warning(
                self,
                "Access Denied",
                "Only Administrators can manage users."
            )
            return

        self.user_management_requested.emit()
        self.statusBar().showMessage("Opening user management...")

    def logout(self):
        """Handle logout - clear session and emit signal to return to login"""
        reply = QMessageBox.question(
            self,
            "Confirm Logout",
            "Are you sure you want to logout?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )

        if reply == QMessageBox.StandardButton.Yes:
            # Clear session
            session = SessionManager()
            session.logout()

            logger.info("User confirmed logout")

            # Emit signal to notify application (let run_app.py handle window closing)
            self.logout_requested.emit()

    def closeEvent(self, event):
        """Handle application close with cleanup"""
        self.data_handler.cleanup_on_close()
        self._save_settings()
        event.accept()
