"""Dashboard window module with component imports."""

import sys
from PySide6.QtWidgets import QApplication, QMessageBox

from qt_material import apply_stylesheet  # intentionally unused here
from .worker import DashboardWorker
from .window import DashboardMainWindow

__all__ = ["DashboardWorker", "DashboardMainWindow", "main"]


def main() -> int:
    """Main application entry point."""
    app = QApplication.instance() or QApplication(sys.argv)

    app.setApplicationName("ETL Pipeline Dashboard")
    app.setOrganizationName("ETL Solutions")
    app.setApplicationVersion("2.0")
    app.setStyle("Fusion")

    # Theme is intentionally not applied here.
    # DashboardMainWindow handles theme selection to avoid double-application.

    try:
        window = DashboardMainWindow()
        window.show()
        return app.exec()
    except Exception as e:
        error_msg = f"Failed to start application:\n{e}"
        print(f"Fatal error: {e}")
        QMessageBox.critical(None, "Fatal Error", error_msg)
        return 1
