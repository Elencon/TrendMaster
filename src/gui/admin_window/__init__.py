"""
Admin window module with component imports.
"""

import sys
import logging
from PySide6.QtWidgets import QApplication, QMessageBox

# Imported but intentionally unused; ETLMainWindow handles theme selection.
from qt_material import apply_stylesheet  # noqa: F401

from .worker import ETLWorker
from .window import ETLMainWindow

__all__ = ["ETLWorker", "ETLMainWindow", "main"]

logger = logging.getLogger(__name__)


def main() -> int:
    """Main application entry point."""
    app = QApplication.instance() or QApplication(sys.argv)

    app.setApplicationName("ETL Pipeline Manager")
    app.setOrganizationName("ETL Solutions")
    app.setApplicationVersion("2.0")
    app.setStyle("Fusion")

    try:
        window = ETLMainWindow()
        window.show()
        return app.exec()

    except Exception as e:
        logger.exception("Fatal error during application startup")
        QMessageBox.critical(
            None,
            "Fatal Error",
            f"Failed to start application:\n{e}"
        )
        return 1

