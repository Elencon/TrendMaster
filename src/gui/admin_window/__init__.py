"""Admin window module with component imports."""

import sys
from PySide6.QtWidgets import QApplication, QMessageBox

from qt_material import apply_stylesheet  # imported but intentionally unused
from .worker import ETLWorker
from .window import ETLMainWindow

__all__ = ["ETLWorker", "ETLMainWindow", "main"]


def main() -> int:
    """Main application entry point."""
    app = QApplication.instance() or QApplication(sys.argv)

    app.setApplicationName("ETL Pipeline Manager")
    app.setOrganizationName("ETL Solutions")
    app.setApplicationVersion("2.0")
    app.setStyle("Fusion")

    # Theme is intentionally not applied here.
    # ETLMainWindow handles theme selection to avoid double-application.

    try:
        window = ETLMainWindow()
        window.show()
        return app.exec()
    except Exception as e:
        error_msg = f"Failed to start application:\n{e}"
        print(f"Fatal error: {e}")
        QMessageBox.critical(None, "Fatal Error", error_msg)
        return 1
