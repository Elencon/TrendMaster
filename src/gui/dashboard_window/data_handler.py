r"""
C:\Economy\Invest\TrendMaster\src\gui\dashboard_window\data_handler.py
Data Handler for Dashboard Window - Extracted data loading and event handling methods
"""

import sys
import os
import subprocess
import logging
from typing import Optional

from PySide6.QtWidgets import QMessageBox, QTableWidgetItem
from PySide6.QtCore import QPoint

from .worker import DashboardWorker, MODULES_AVAILABLE
from auth.session import SessionManager  # type: ignore

logger = logging.getLogger(__name__)

# Windows-specific flag to hide console window
if sys.platform == "win32":
    CREATE_NO_WINDOW = 0x08000000
else:
    CREATE_NO_WINDOW = 0


class DashboardDataHandler:
    """Handles all data loading and event handling for the dashboard window"""
    
    def __init__(self, window):
        """
        Initialize the data handler.
        
        Args:
            window: Reference to the parent DashboardMainWindow instance
        """
        self.window = window
        self.current_worker: Optional[DashboardWorker] = None
    
    def initialize_dashboard(self):
        """Initialize dashboard and load table list"""
        if MODULES_AVAILABLE:
            self.window.statusBar().showMessage("Loading database tables...")
            self._start_operation("fetch_tables")

            # Load customers for dropdown
            self.load_customers()

            # Ensure session and pool are ready before loading employees
            from PySide6.QtCore import QTimer
            session = SessionManager()
            user_role = session.get_role()
            def delayed_employee_load():
                logger.info("Attempting to load employees after session/pool ready.")
                if user_role in ["Manager", "Administrator"]:
                    self.load_employees()
            QTimer.singleShot(300, delayed_employee_load)

            # Delay sales data fetch slightly to ensure UI is ready
            QTimer.singleShot(500, self.fetch_sales_data)
        else:
            self.window.tables_list.addItem("⚠️ Database modules not available")
            self.window.statusBar().showMessage("Database modules not available")
    
    def load_customers(self):
        """Load customers into dropdown"""
        worker = DashboardWorker("fetch_customers")
        worker.customers_loaded.connect(self.on_customers_loaded)
        worker.error.connect(lambda msg: logger.error(f"Customer fetch error: {msg}"))
        worker.finished.connect(worker.deleteLater)
        worker.start()
    
    def on_customers_loaded(self, customers: list):
        """
        Handle customers data loaded.
        
        Args:
            customers: List of customer dictionaries
        """
        # Check if customer_combo widget exists
        if not hasattr(self.window, 'customer_combo'):
            logger.error("Customer combo widget not found!")
            return
        
        self.window.customer_combo.clear()
        self.window.customer_combo.addItem("-- Select a customer --", None)
        for customer in customers:
            customer_id = customer['customer_id']
            name = f"{customer['first_name']} {customer['last_name']}"
            display_text = f"{name} (ID: {customer_id})"
            self.window.customer_combo.addItem(display_text, customer_id)
    
    def load_employees(self, retry_count=0):
        """Load employees into table, with retry on blank/error."""
        worker = DashboardWorker("fetch_employees")
        def handle_employees_loaded(employees):
            if (not employees or len(employees) == 0) and retry_count < 2:
                logger.warning(f"Employee fetch returned blank, retrying ({retry_count+1})...")
                self.load_employees(retry_count=retry_count+1)
            else:
                self.on_employees_loaded(employees)
        worker.employees_loaded.connect(handle_employees_loaded)
        worker.error.connect(lambda msg: self._handle_employee_error(msg, retry_count))
        worker.finished.connect(worker.deleteLater)
        worker.start()

    def _handle_employee_error(self, msg, retry_count):
        logger.error(f"Employee fetch error: {msg}")
        if retry_count < 2:
            logger.info(f"Retrying employee fetch after error ({retry_count+1})...")
            self.load_employees(retry_count=retry_count+1)
    
    def on_employees_loaded(self, employees: list):
        """
        Handle employees data loaded.
        
        Args:
            employees: List of employee dictionaries
        """
        # Check if employee_table widget exists (only created for Manager/Admin)
        if not hasattr(self.window, 'employee_table'):
            logger.error("employee_table widget does not exist for current user role!")
            return

        logger.debug(f"on_employees_loaded called. Employees: {employees}")
        logger.debug(f"employee_table exists. Current row count: {self.window.employee_table.rowCount()}")

        try:
            self.window.employee_table.setRowCount(len(employees))

            if not employees:
                self.window.employee_table.setRowCount(0)
                self.window.employee_table.clearContents()
                self.window.employee_table.setHorizontalHeaderLabels(['Name', 'Email'])
                self.window.employee_table.setRowCount(1)
                self.window.employee_table.setItem(0, 0, QTableWidgetItem("No employees found or connection error"))
                self.window.employee_table.setItem(0, 1, QTableWidgetItem(""))
                logger.warning("No employees found or connection error. Table populated with warning message.")
                return

            for row, emp in enumerate(employees):
                name = f"{emp.get('name', '')} {emp.get('last_name', '')}"
                self.window.employee_table.setItem(row, 0, QTableWidgetItem(name))
                self.window.employee_table.setItem(row, 1, QTableWidgetItem(emp.get('email', 'N/A')))
            logger.debug(f"Employee table populated with {len(employees)} employees.")
        except Exception as e:
            logger.error(f"Error populating employee_table: {e}")
        # PDF generation should only be triggered by explicit user action, not on login/employee load.
    
    def on_pdf_generated(self, filepath: str):
        """
        Handle PDF generation complete.
        
        Args:
            filepath: Path to the generated PDF file
        """
        self.window.statusBar().showMessage(f"PDF report generated successfully!", 5000)
        
        # Ask if user wants to open the PDF
        reply = QMessageBox.question(
            self.window,
            "PDF Generated",
            f"Report saved to:\n{filepath}\n\nWould you like to open it?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.Yes
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            if sys.platform == "win32":
                os.startfile(filepath)
            elif sys.platform == "darwin":  # macOS
                subprocess.run(["open", filepath])
            else:  # linux
                subprocess.run(["xdg-open", filepath])
    
    def fetch_sales_data(self):
        """Fetch sales data for gauge chart"""
        worker = DashboardWorker("fetch_sales")
        worker.sales_data_loaded.connect(self.on_sales_loaded)
        worker.error.connect(lambda msg: logger.error(f"Sales fetch error: {msg}"))
        worker.finished.connect(worker.deleteLater)
        worker.start()
    
    def on_sales_loaded(self, total_sales: float):
        """
        Handle sales data loaded.
        
        Args:
            total_sales: Total sales amount
        """
        # Direct update - widget should be ready by now
        try:
            self.window.sales_gauge.set_sales_data(total_sales)
        except Exception as e:
            logger.error(f"Failed to update sales gauge: {e}")
    
    def _start_operation(self, operation: str):
        """
        Start background operation.
        
        Args:
            operation: Name of the operation to start
        """
        if self.current_worker and self.current_worker.isRunning():
            return
        
        self.current_worker = DashboardWorker(operation)
        self.current_worker.progress.connect(self._on_progress)
        self.current_worker.finished.connect(self._on_operation_finished)
        self.current_worker.error.connect(self._on_operation_error)
        
        if operation == "fetch_tables":
            self.current_worker.tables_loaded.connect(self.on_tables_loaded)
        
        self.current_worker.start()
    
    def _on_progress(self, message: str):
        """Handle progress updates"""
        self.window.statusBar().showMessage(message)
    
    def _on_operation_finished(self, message: str):
        """Handle operation completion"""
        self.window.statusBar().showMessage(message)
        self._cleanup_operation()
    
    def _on_operation_error(self, message: str):
        """Handle operation error"""
        self.window.statusBar().showMessage(f"Error: {message}")
        
        # If database connection failed, show expected tables from schema
        try:
            from src.database.schema_manager import SCHEMA_DEFINITIONS
            self.window.tables_list.clear()
            self.window.tables_list.addItem("⚠️ Database connection failed - Showing expected tables:")
            for table_name in SCHEMA_DEFINITIONS.keys():
                self.window.tables_list.addItem(f"📊 {table_name} (not connected)")
        except Exception:
            self.window.tables_list.clear()
            self.window.tables_list.addItem(f"⚠️ {message}")
        
        self._cleanup_operation()
    
    def on_tables_loaded(self, tables: list):
        """
        Handle tables list loaded.
        
        Args:
            tables: List of table names
        """
        self.window.tables_list.clear()
        if tables:
            for table in tables:
                self.window.tables_list.addItem(f"📁 {table}")
        else:
            self.window.tables_list.addItem("No tables found")
    
    def _cleanup_operation(self):
        """Clean up after operation completion"""
        if self.current_worker:
            # Disconnect all signals before deleting
            try:
                self.current_worker.progress.disconnect()
                self.current_worker.finished.disconnect()
                self.current_worker.error.disconnect()
                if hasattr(self.current_worker, 'tables_loaded'):
                    self.current_worker.tables_loaded.disconnect()
            except:
                pass
            self.current_worker.deleteLater()
            self.current_worker = None
    
    def cleanup_on_close(self):
            import logging as _logging
            logger = _logging.getLogger(__name__)
            if hasattr(self.window, 'employee_table'):
                logger.debug(f"employee_table widget exists, current row count: {self.window.employee_table.rowCount()}")
    def cleanup_on_close(self):
        """Handle cleanup when window is closing"""
        if self.current_worker and self.current_worker.isRunning():
            # Disconnect signals to prevent issues during shutdown
            try:
                self.current_worker.progress.disconnect()
                self.current_worker.finished.disconnect()
                self.current_worker.error.disconnect()
                if hasattr(self.current_worker, 'tables_loaded'):
                    self.current_worker.tables_loaded.disconnect()
            except Exception:
                pass
            # Cancel and stop the thread
            self.current_worker.cancel()
            self.current_worker.quit()
            if not self.current_worker.wait(2000):
                # Force terminate if it doesn't stop gracefully
                self.current_worker.terminate()
                self.current_worker.wait(500)
    
    def generate_customer_pdf(self):
        """Generate PDF report for the selected customer."""
        if not hasattr(self.window, 'customer_combo'):
            logger.error("Customer combo widget not found!")
            return
        customer_id = self.window.customer_combo.currentData()
        if not customer_id:
            QMessageBox.warning(
                self.window,
                "No Customer Selected",
                "Please select a customer from the dropdown."
            )
            return
        self.window.statusBar().showMessage(f"Generating PDF report for customer {customer_id}...")
        worker = DashboardWorker("generate_customer_pdf", customer_id)
        worker.pdf_generated.connect(self.on_pdf_generated)
        worker.error.connect(lambda msg: QMessageBox.critical(
            self.window,
            "PDF Generation Failed",
            msg
        ))
        worker.finished.connect(worker.deleteLater)
        worker.start()
