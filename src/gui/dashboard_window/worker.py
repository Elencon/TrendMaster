r"""
C:\Economy\Invest\TrendMaster\src\gui\dashboard_window\worker.py
Background worker for dashboard operations
"""

from PySide6.QtCore import Signal

from ..base_worker import BaseWorker
import pymysql.cursors
from src.database.db_manager import DatabaseManager
from src.database.pdf_generator import CustomerOrderPDFGenerator


class DashboardWorker(BaseWorker):
    """Worker thread for dashboard database operations"""

    tables_loaded = Signal(list)
    sales_data_loaded = Signal(float)
    customers_loaded = Signal(list)
    employees_loaded = Signal(list)
    manager_loaded = Signal(dict)
    pdf_generated = Signal(str)

    def __init__(self, operation: str, *args):
        super().__init__(operation, *args)

        # Operation dispatch map
        self._operations = {
            "fetch_tables": self._fetch_tables,
            "fetch_sales": self._fetch_sales,
            "fetch_customers": self._fetch_customers,
            "fetch_employees": self._fetch_employees,
            "fetch_my_manager": self._fetch_my_manager,
            "generate_customer_pdf": self._generate_customer_pdf
        }

    def run(self):
        """Main execution method with module availability check"""
        super().run()

    def _fetch_tables(self):
        """Fetch list of database tables"""
        db_manager = None
        try:
            # Lazy import - only load when actually executing
            from src.database.db_manager import DatabaseManager

            self.progress.emit("Connecting to database...")
            db_manager = DatabaseManager()

            self.progress.emit("Fetching table list...")
            tables = db_manager.get_all_tables()

            if tables:
                self.tables_loaded.emit(tables)
                self.finished.emit(f"Found {len(tables)} tables in database")
            else:
                # If no tables found, load from schema definitions as fallback
                try:
                    from src.database.schema_manager import SCHEMA_DEFINITIONS
                    schema_tables = list(SCHEMA_DEFINITIONS.keys())
                    if schema_tables:
                        self.tables_loaded.emit(schema_tables)
                        self.finished.emit(f"Loaded {len(schema_tables)} expected tables from schema")
                    else:
                        self.finished.emit("No tables found in database")
                        self.tables_loaded.emit([])
                except Exception:
                    self.finished.emit("No tables found in database")
                    self.tables_loaded.emit([])

        except Exception as e:
            self.error.emit(f"Failed to fetch tables: {str(e)}")
        finally:
            # Let garbage collector handle cleanup
            db_manager = None

    def _fetch_sales(self):
        """Fetch total sales data"""
        db_manager = None
        try:
            from src.database.db_manager import DatabaseManager

            self.progress.emit("Calculating total sales...")
            db_manager = DatabaseManager()

            total_sales = db_manager.get_total_sales()
            self.sales_data_loaded.emit(total_sales)
            self.finished.emit("Sales data loaded")

        except Exception as e:
            self.error.emit(f"Failed to fetch sales: {str(e)}")
        finally:
            db_manager = None

    def _fetch_customers(self):
        """Fetch customers list"""

        db_manager = None
        try:
            from src.database.db_manager import DatabaseManager

            self.progress.emit("Loading customers...")
            db_manager = DatabaseManager()

            with db_manager.get_connection() as conn:
                cursor = conn.cursor(pymysql.cursors.DictCursor)

                cursor.execute('''
                    SELECT customer_id, first_name, last_name, email 
                    FROM customers ORDER BY last_name, first_name''')
                customers = cursor.fetchall()
                cursor.close()

            self.customers_loaded.emit(customers)
            self.finished.emit(f"Loaded {len(customers)} customers")

        except Exception as e:
            self.error.emit(f"Failed to fetch customers: {str(e)}")
        finally:
            db_manager = None

    def _fetch_employees(self):
        """Fetch users with Employee role"""

        db_manager = None
        try:
            from src.database.db_manager import DatabaseManager

            self.progress.emit("Loading employees...")
            db_manager = DatabaseManager()
            # Log connection stats early for diagnostics
            try:
                stats = db_manager.get_connection_stats()
                import logging as _logging
                _logging.getLogger(__name__).debug(f"Employee fetch connection stats: {stats}")
            except Exception:
                pass

            with db_manager.get_connection() as conn:
                import logging as _logging
                logger = _logging.getLogger(__name__)
                if conn is None:
                    logger.error("No database connection available for employee fetch")
                    raise RuntimeError("No database connection available for employee fetch")
                # Try mysql.connector style first, then PyMySQL style
                cursor = conn.cursor(pymysql.cursors.DictCursor)

                logger.debug("Running employee fetch SQL...")
                cursor.execute('''
                    SELECT u.username, s.name, s.last_name, s.email, u.active, u.role
                    FROM users u
                    LEFT JOIN staffs s ON u.staff_id = s.staff_id
                    WHERE u.role = 'Employee' AND u.active = 1
                    ORDER BY s.last_name, s.name
                ''')
                employees = cursor.fetchall()
                logger.debug(f"Employee fetch returned {len(employees)} rows: {employees}")
                cursor.close()
            self.employees_loaded.emit(employees)
            self.finished.emit(f"Loaded {len(employees)} employee users")

        except Exception as e:
            self.error.emit(f"Failed to fetch employees: {str(e)}")
        finally:
            db_manager = None

    def _fetch_my_manager(self):
        """Fetch current user's manager information"""

        try:
            from src.database.db_manager import DatabaseManager
            from auth.session import SessionManager

            self.progress.emit("Loading manager information...")
            db_manager = DatabaseManager()
            session = SessionManager()

            # Get current user's staff_id
            username = session.get_username()

            with db_manager.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)

                # Get user's staff_id
                cursor.execute('''
                    SELECT staff_id FROM users WHERE username = %s
                ''', (username,))
                user_data = cursor.fetchone()

                if not user_data or not user_data.get('staff_id'):
                    self.manager_loaded.emit({})
                    cursor.close()
                    return

                staff_id = user_data['staff_id']

                # Get manager_id for this staff member
                cursor.execute('''
                    SELECT manager_id FROM staffs WHERE staff_id = %s
                ''', (staff_id,))
                staff_data = cursor.fetchone()

                if not staff_data or not staff_data.get('manager_id'):
                    self.manager_loaded.emit({})
                    cursor.close()
                    return

                manager_id = staff_data['manager_id']

                # Get manager's information
                cursor.execute('''
                    SELECT staff_id, first_name, last_name, email, phone
                    FROM staffs
                    WHERE staff_id = %s
                ''', (manager_id,))
                manager_data = cursor.fetchone()
                cursor.close()

            if manager_data:
                self.manager_loaded.emit(manager_data)
                self.finished.emit("Loaded manager information")
            else:
                self.manager_loaded.emit({})

        except Exception as e:
            self.error.emit(f"Failed to fetch manager: {str(e)}")

    def _generate_customer_pdf(self):
        """Generate PDF report for customer"""

        customer_id = self.args[0] if self.args else None
        if not customer_id:
            self.error.emit("No customer ID provided")
            return

        try:

            self.progress.emit(f"Fetching customer data for ID {customer_id}...")
            db_manager = DatabaseManager()

            with db_manager.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)

                # Get customer info
                cursor.execute('''
                    SELECT customer_id, first_name, last_name, email, phone, 
                           street, city, state, zip_code
                    FROM customers 
                    WHERE customer_id = %s
                ''', (customer_id,))
                customer_data = cursor.fetchone()

                if not customer_data:
                    self.error.emit(f"Customer ID {customer_id} not found")
                    cursor.close()
                    return

                # Get customer orders with totals
                cursor.execute('''
                    SELECT o.order_id, o.order_date, o.order_status,
                           COUNT(oi.item_id) as item_count,
                           SUM(oi.quantity * oi.list_price * (1 - oi.discount)) as total_amount
                    FROM orders o
                    LEFT JOIN order_items oi ON o.order_id = oi.order_id
                    WHERE o.customer_id = %s
                    GROUP BY o.order_id, o.order_date, o.order_status
                    ORDER BY o.order_date DESC
                ''', (customer_id,))
                orders_data = cursor.fetchall()
                cursor.close()

            # Generate PDF
            self.progress.emit("Generating PDF report...")
            pdf_gen = CustomerOrderPDFGenerator()
            filepath = pdf_gen.generate_customer_report(customer_data, orders_data)

            self.pdf_generated.emit(filepath)
            self.finished.emit(f"PDF generated: {filepath}")

        except ImportError:
            self.error.emit("reportlab library required. Install with: pip install reportlab")
        except Exception as e:
            self.error.emit(f"Failed to generate PDF: {str(e)}")
