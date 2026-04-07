r"""
C:\Economy\Invest\TrendMaster\IntegrityService\integrity_service\gui\app.py
run from TrendMaster
cd IntegrityService
python -m integrity_service.gui.app
"""
import sys
import logging
from PySide6.QtWidgets import QApplication
from .main_window import MainWindow

logger = logging.getLogger(__name__)

def main():
    """Main application entry point."""
    app = QApplication.instance() or QApplication(sys.argv)

    app.setApplicationName("Integrity Service")
    app.setOrganizationName("TrendMaster")
    app.setApplicationVersion("1.0")
    app.setStyle("Fusion")

    # Load QSS stylesheet
    try:
        with open("integrity_service/gui/styles.qss", "r", encoding="utf-8") as f:
            app.setStyleSheet(f.read())
    except FileNotFoundError:
        # Optional: silently ignore if style file is missing
        pass
    
    try:
        window = MainWindow()
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