r"""
C:\Economy\Invest\TrendMaster\IntegrityService\integrity_service\gui\app.py
run from TrendMaster
cd IntegrityService
python -m integrity_service.gui.app
"""
import sys
import logging
from PySide6.QtWidgets import QApplication, QMessageBox
from .main_window import ETLMainWindow

logger = logging.getLogger(__name__)

def main():
    """Main application entry point."""
    app = QApplication.instance() or QApplication(sys.argv)

    app.setApplicationName("Integrity Service")
    app.setOrganizationName("TrendMaster")
    app.setApplicationVersion("1.0")
    app.setStyle("Fusion")

    # Load QSS stylesheet through ThemeManager
    try:
        from integrity_service.gui.themes import ThemeManager
        theme_manager = ThemeManager()
        theme_manager.apply_current_theme(app)
    except Exception as e:
        logger.error(f"Failed to apply theme manager: {e}")
    try:
        window = ETLMainWindow()
        window.show()

        sys.exit(app.exec())

    except Exception as e:
        logger.exception("Fatal error during application startup")
        QMessageBox.critical(
            None,
            "Fatal Error",
            f"Failed to start application:\n{e}"
        )
        return 1

if __name__ == "__main__":
    main()