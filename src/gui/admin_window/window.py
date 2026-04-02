r"""
C:\Economy\Invest\TrendMaster\src\gui\admin_window\window.py
Main window class for ETL Pipeline Manager - Refactored for maintainability
python -m src.gui.admin_window.window
"""

import sys
import os
from typing import Dict

from PySide6.QtWidgets import QMainWindow, QVBoxLayout, QWidget, QApplication
from PySide6.QtCore import QSettings

from src.cache_cleaner import CacheCleaner
from src.gui.themes import ThemeManager
from .ui_builder import AdminUIBuilder
from .operation_handler import AdminOperationHandler

sys.dont_write_bytecode = True
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"


class ETLMainWindow(QMainWindow):
    """Main window with clean architecture and theme support"""
    
    def __init__(self):
        super().__init__()
        self.settings = QSettings("ETL Solutions", "ETL Pipeline Manager")
        self.theme_manager = ThemeManager()
        
        # Store operation buttons for unified control
        self.operation_buttons: Dict[str, any] = {}
        
        # Initialize UI builder and operation handler
        self.ui_builder = AdminUIBuilder(self)
        self.operation_handler = AdminOperationHandler(self)
        
        self._clean_application_cache()
        
        self._setup_ui()
        self._load_settings()
        self.operation_handler.initialize_status()
    
    def _clean_application_cache(self):
        """Clean application cache on startup"""
        try:
            cache_cleaner = CacheCleaner()
            cache_cleaner.clean_all(verbose=False, clean_logs=False)
        except Exception as e:
            print(f"Warning: Cache cleanup failed: {e}")
    
    def _setup_ui(self):
        """Set up the user interface"""
        self.setWindowTitle("ETL Pipeline Manager - Production Ready (1,289+ Records)")
        self.setGeometry(100, 100, 950, 750)
        
        # Create toolbar
        toolbar = self.ui_builder.create_toolbar()
        self.addToolBar(toolbar)
        
        # Create main layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        central_widget_layout = QVBoxLayout(central_widget)
        splitter = self.ui_builder.create_main_layout()
        central_widget_layout.addWidget(splitter)
        
        self.statusBar().showMessage("Ready")
    
    def _load_settings(self):
        """Load user settings"""
        geometry = self.settings.value("geometry")
        if geometry:
            self.restoreGeometry(geometry)
        
        saved_theme = self.settings.value("theme/current_theme", "dark", type=str)
        self.theme_manager.set_theme(saved_theme)
        self._apply_theme()
    
    def _save_settings(self):
        """Save user settings"""
        self.settings.setValue("geometry", self.saveGeometry())
        self.settings.setValue("api_url", self.api_url_input.text())
        self.settings.setValue("theme/current_theme", self.theme_manager.get_current_theme_name())
    
    def _apply_theme(self):
        """Apply current theme using theme manager"""
        app = QApplication.instance()
        self.theme_manager.apply_current_theme(app)
    
    def toggle_theme(self):
        """Toggle between light and dark theme"""
        self.theme_manager.toggle_theme()
        self._apply_theme()
        self.update()
        self.repaint()
        theme_name = self.theme_manager.get_current_theme_name()
        self.operation_handler.append_output(f"Switched to {theme_name} theme")
    
    # ==================== Delegated ETL Operations ====================
    # All operations delegate to operation_handler
    
    def test_db_connection(self):
        """Test database connection"""
        self.operation_handler.test_db_connection()
    
    def test_api_connection(self):
        """Test API connection"""
        self.operation_handler.test_api_connection()
    
    def create_tables(self):
        """Create database tables"""
        self.operation_handler.create_tables()
    
    def load_csv_data(self):
        """Load CSV data"""
        self.operation_handler.load_csv_data()
    
    def load_api_data(self):
        """Load API data from API and save to CSV"""
        self.operation_handler.load_api_data()
    
    def select_csv_files(self):
        """Select CSV files"""
        self.operation_handler.select_csv_files()
    
    def load_selected_files(self):
        """Load selected CSV files"""
        self.operation_handler.load_selected_files()
    
    def test_csv_access(self):
        """Test CSV file access"""
        self.operation_handler.test_csv_access()
    
    def test_api_export(self):
        """Test API data export"""
        self.operation_handler.test_api_export()
    
    def closeEvent(self, event):
        """Handle application close with cleanup"""
        self.operation_handler.cleanup_on_close()
        self._save_settings()
        event.accept()

        
def main():
    app = QApplication(sys.argv)
    window = ETLMainWindow()
    window.show()
    return app.exec()