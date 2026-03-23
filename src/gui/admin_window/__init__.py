"""Admin window module with component imports"""
import sys
from PySide6.QtWidgets import QApplication, QMessageBox

try:
    from qt_material import apply_stylesheet
    QT_MATERIAL_AVAILABLE = True
except ImportError:
    QT_MATERIAL_AVAILABLE = False

from .worker import ETLWorker
from .window import ETLMainWindow

__all__ = ['ETLWorker', 'ETLMainWindow', 'main']


def main():
    """Main application entry point"""
    app = QApplication.instance() or QApplication(sys.argv)
    app.setApplicationName("ETL Pipeline Manager")
    app.setOrganizationName("ETL Solutions")
    app.setApplicationVersion("2.0")
    
    app.setStyle('Fusion')
    
    # Don't apply theme here - let ThemeManager handle it in the window
    # This avoids double theme application which slows startup
    
    try:
        window = ETLMainWindow()
        window.show()
        return app.exec()
    except Exception as e:
        print(f"Fatal error: {e}")
        QMessageBox.critical(None, "Fatal Error", f"Failed to start application:\n{e}")
        return 1
