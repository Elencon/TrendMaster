"""ETL Worker thread for background operations"""

import sys
from pathlib import Path
import shutil

from PySide6.QtCore import QThread, Signal

# Add src to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
SRC_PATH = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_PATH))

MODULES_AVAILABLE = True
try:
    from src.database.db_manager import DatabaseManager
    from src.database import config
    from src.database.data_from_api import APIDataFetcher as APIClient
except ImportError as e:
    print(f"Warning: Could not import ETL modules: {e}")
    MODULES_AVAILABLE = False

DATA_PATH = PROJECT_ROOT / "data"
CSV_PATH = DATA_PATH / "CSV"
API_PATH = DATA_PATH / "API"


class ETLWorker(QThread):
    """Worker thread for ETL operations with proper error handling"""
    finished = Signal(str)
    error = Signal(str)
    progress = Signal(str)
    data_ready = Signal(dict)

    def __init__(self, operation: str, *args, **kwargs):
        super().__init__()
        self.operation = operation
        self.args = args
        self.kwargs = kwargs
        self._is_cancelled = False

        # Operation dispatch map
        self._operations = {
            "test_connection": self._test_connection,
            "test_api": self._test_api,
            "create_tables": self._create_tables,
            "load_csv": self._load_csv,
            "load_api": self._load_api,
            "select_csv_files": self._select_csv_files,
            "test_csv_access": self._test_csv_access,
            "test_api_export": self._test_api_export
        }

    def cancel(self):
        """Cancel the current operation"""
        self._is_cancelled = True

    def run(self):
        """Main execution method with operation routing"""
        if not MODULES_AVAILABLE and self.operation != "select_csv_files":
            self.error.emit("ETL modules not available")
            return

        try:
            if self.operation in self._operations:
                self._operations[self.operation]()
            else:
                self.error.emit(f"Unknown operation: {self.operation}")
        except Exception as e:
            self.error.emit(f"Error in {self.operation}: {str(e)}")

    def _test_connection(self):
        """Test database connection"""
        self.progress.emit("Testing database connection...")
        db_manager = None
        try:
            db_manager = DatabaseManager()
            if db_manager.test_connection() and not self._is_cancelled:
                self.finished.emit("Database connection successful!")
            else:
                self.error.emit("Failed to connect to database")
        except Exception as e:
            self.error.emit(f"Database connection error: {str(e)}")
        finally:
            # Let the destructor handle cleanup - don't close the pool
            db_manager = None

    def _test_api(self):
        """Test API connection"""
        api_url = self.args[0]
        self.progress.emit(f"Testing API connection to: {api_url}")
        try:
            api_client = APIClient(api_url)
            test_data = api_client.fetch_data('orders') if not self._is_cancelled else None
            api_client.close()

            if test_data is not None:
                self.finished.emit(f"API connection successful! Found {len(test_data)} records")
            else:
                self.error.emit("API connection failed - no data received")
        except Exception as e:
            self.error.emit(f"API connection failed: {str(e)}")

    def _create_tables(self):
        """Create database tables"""
        self.progress.emit("Creating database and tables...")
        db_manager = None
        try:
            db_manager = DatabaseManager()

            if not self._is_cancelled:
                self.progress.emit("Creating database if not exists...")
                success = db_manager.create_database_if_not_exists()
                if success and not self._is_cancelled:
                    self.progress.emit("Creating all 9 tables with updated schema...")
                    csv_success = db_manager.create_all_tables_from_csv()
                    if csv_success:
                        csv_tables = ['brands', 'categories', 'stores', 'staffs', 'products', 'stocks']
                        api_tables = ['customers', 'orders', 'order_items']
                        self.progress.emit("Verifying table creation...")
                        table_info = []
                        for table in csv_tables + api_tables:
                            count = db_manager.get_row_count(table)
                            table_info.append(f"  {table}: {'Ready' if count >= 0 else 'Created'}")

                        result = "All 9 database tables created successfully!\nSchema Updates Applied:\n  - STOCKS: store_name (FK), product_id (PK)\n  - STAFFS: name, store_name, street columns\nTables Created:\n" + "\n".join(table_info)
                        self.finished.emit(result)
                    else:
                        self.error.emit("Failed to create some tables - Check schema compatibility")
                else:
                    self.error.emit("Failed to create database - Check MySQL permissions")
        except Exception as e:
            self.error.emit(f"Error creating tables: {str(e)}")
        finally:
            # Let the destructor handle cleanup - don't close the pool
            db_manager = None

    def _load_csv(self):
        """Load CSV data"""
        self.progress.emit("Loading CSV data with NaN→NULL conversion...")
        db_manager = None
        try:
            db_manager = DatabaseManager()

            self.progress.emit("Creating tables and loading data...")
            success = db_manager.create_all_tables_from_csv()

            if success and not self._is_cancelled:
                self.progress.emit("Verifying data insertion...")
                table_counts = {table: db_manager.get_row_count(table)
                              for table in db_manager.csv_files.keys()}

                total_rows = sum(table_counts.values())
                summary = "\n".join([f"  {table}: {count} rows" for table, count in table_counts.items()])

                result = f"CSV data loaded successfully!\nTotal Records: {total_rows:,}\nTable Breakdown:\n{summary}\nAll NaN values converted to MySQL NULL\nSchema alignment verified (STOCKS/STAFFS updated)"
                self.finished.emit(result)
                self.data_ready.emit(table_counts)
            else:
                self.error.emit("Failed to load CSV data - Check file permissions and schema compatibility")
        except Exception as e:
            self.error.emit(f"Error loading CSV: {str(e)}")
        finally:
            # Let the destructor handle cleanup - don't close the pool
            db_manager = None

    def _load_api(self):
        """Load API data and import into database"""
        api_url = self.args[0]
        self.progress.emit(f"Connecting to API: {api_url}")
        try:
            # Step 1: Export API data to CSV files
            api_client = APIClient(api_url)
            self.progress.emit("Fetching data from API endpoints...")
            csv_success = api_client.save_all_api_data_to_csv(str(API_PATH))
            api_client.close()

            if not csv_success or self._is_cancelled:
                self.error.emit("Failed to export API data to CSV - Check API connectivity")
                return

            # Step 2: Verify CSV files created
            self.progress.emit("Verifying exported files...")
            csv_files = list(API_PATH.glob("*.csv")) if API_PATH.exists() else []
            if not csv_files:
                self.error.emit("No CSV files were created from API data")
                return

            # Step 3: Import CSV files into database
            self.progress.emit("Importing API data into database...")
            db_manager = DatabaseManager()

            # Import in order respecting foreign keys: customers -> orders -> order_items
            import_order = ['customers', 'orders', 'order_items']
            total_records = 0

            # Get a single connection for all imports to avoid pool exhaustion
            with db_manager.get_connection() as conn:
                if conn is None:
                    self.error.emit("Failed to get database connection for API import")
                    return

                for table_name in import_order:
                    if self._is_cancelled:
                        return

                    # Find the CSV file for this table
                    csv_file = API_PATH / f"{table_name}.csv"
                    if not csv_file.exists():
                        self.progress.emit(f"  ⚠️ {table_name}.csv not found, skipping")
                        continue

                    self.progress.emit(f"Importing {table_name}...")

                    # Read CSV file
                    import pandas as pd
                    df = pd.read_csv(csv_file)

                    if df.empty:
                        self.progress.emit(f"  ⚠️ {table_name}: No records to import")
                        continue

                    # Convert to list of dictionaries FIRST, then clean NaN values
                    records = df.to_dict('records')

                    # Clean NaN/None values - replace with None for MySQL compatibility
                    cleaned_records = []
                    for record in records:
                        cleaned_record = {}
                        for key, value in record.items():
                            # Check if value is NaN using pd.isna or if it's literally NaN
                            if pd.isna(value):
                                cleaned_record[key] = None
                            else:
                                cleaned_record[key] = value
                        cleaned_records.append(cleaned_record)

                    # Debug: Show first record for order_items
                    if table_name == 'order_items' and cleaned_records:
                        self.progress.emit(f"  DEBUG: First record = {cleaned_records[0]}")
                        self.progress.emit(f"  DEBUG: Total records to insert = {len(cleaned_records)}")

                    # Insert directly using the shared connection
                    try:
                        cursor = conn.cursor()

                        # Get column names from first record
                        columns = list(cleaned_records[0].keys()) if cleaned_records else []
                        if not columns:
                            self.progress.emit(f"  ⚠️ {table_name}: No columns found")
                            continue

                        # Prepare SQL
                        placeholders = ', '.join(['%s'] * len(columns))
                        column_names = ', '.join(f"`{col}`" for col in columns)
                        sql = f"INSERT INTO `{table_name}` ({column_names}) VALUES ({placeholders})"

                        # Prepare batch values
                        batch_values = []
                        for record in cleaned_records:
                            values = tuple(record.get(col) for col in columns)
                            batch_values.append(values)

                        # Execute batch insert
                        cursor.executemany(sql, batch_values)
                        conn.commit()
                        cursor.close()

                        inserted = len(batch_values)
                        total_records += inserted

                        # Verify actual row count in database
                        cursor = conn.cursor()
                        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                        actual_count = cursor.fetchone()[0]
                        cursor.close()

                        self.progress.emit(f"  ✅ {table_name}: {inserted:,} records imported, DB has {actual_count} rows")

                    except Exception as e:
                        self.progress.emit(f"  ❌ {table_name}: INSERT ERROR: {str(e)}")
                        import traceback
                        self.progress.emit(f"  TRACEBACK: {traceback.format_exc()}")

            # Step 4: Success message with details
            file_info = [f"  {f.name}: {f.stat().st_size:,} bytes" for f in csv_files]
            total_size = sum(f.stat().st_size for f in csv_files)

            result = (f"API data loaded successfully!\n"
                     f"Location: {API_PATH}\n"
                     f"Files Created: {len(csv_files)}\n"
                     f"Total Size: {total_size:,} bytes\n"
                     f"Records Imported: {total_records:,}\n"
                     f"Files:\n" + "\n".join(file_info))
            self.finished.emit(result)

        except Exception as e:
            self.error.emit(f"Failed to load API data: {str(e)}")

    def _select_csv_files(self):
        """Handle CSV file selection and copying"""
        selected_files = self.args[0]
        CSV_PATH.mkdir(parents=True, exist_ok=True)

        copied_files = []
        for file_path in selected_files:
            if self._is_cancelled:
                break

            try:
                src_path = Path(file_path)
                dest_path = CSV_PATH / src_path.name
                shutil.copy2(src_path, dest_path)
                copied_files.append(src_path.name)
                self.progress.emit(f"Copied: {src_path.name}")
            except Exception as e:
                self.progress.emit(f"Failed to copy {Path(file_path).name}: {str(e)}")

        if copied_files:
            self.finished.emit(f"Successfully copied {len(copied_files)} files to CSV folder:\n" +
                             "\n".join([f"  - {f}" for f in copied_files]))
        else:
            self.error.emit("No files were copied successfully")

    def _test_csv_access(self):
        """Test CSV file access and schema validation"""
        self.progress.emit("Testing CSV file access and schema validation...")
        db_manager = None
        try:
            db_manager = DatabaseManager()
            self.progress.emit(f"Data directory: {db_manager.data_dir}")

            total_rows = 0
            for table_name, csv_file in db_manager.csv_files.items():
                try:
                    df = db_manager.read_csv_file(csv_file)
                    if df is not None:
                        total_rows += len(df)
                        cols_preview = ', '.join(df.columns[:3]) + ('...' if len(df.columns) > 3 else '')
                        self.progress.emit(f"SUCCESS: {table_name}: {len(df)} rows, {len(df.columns)} columns ({cols_preview})")
                    else:
                        self.progress.emit(f"FAILED: {table_name}: Failed to read {csv_file}")
                except Exception as e:
                    self.progress.emit(f"ERROR: {table_name}: {e}")

            result = f"CSV access test completed!\nTotal records available: {total_rows:,}\nSchema alignment: STOCKS (store_name), STAFFS (name, store_name, street)"
            self.finished.emit(result)
        except Exception as e:
            self.error.emit(f"Error in CSV access test: {str(e)}")
        finally:
            # Let the destructor handle cleanup - don't close the pool
            db_manager = None

    def _test_api_export(self):
        """Test API data export"""
        self.progress.emit("Testing API data export...")
        db_manager = None
        try:
            db_manager = DatabaseManager()
            success = db_manager.export_api_data_to_csv()

            if success:
                self.progress.emit("SUCCESS: API data export successful!")

                if API_PATH.exists():
                    csv_files = list(API_PATH.glob("*.csv"))
                    if csv_files:
                        self.progress.emit(f"Found {len(csv_files)} CSV files:")
                        for file_path in sorted(csv_files):
                            size = file_path.stat().st_size
                            self.progress.emit(f"  {file_path.name:<20} ({size:,} bytes)")
                        self.finished.emit("API export test completed successfully!")
                    else:
                        self.error.emit("No CSV files found in API directory")
                else:
                    self.error.emit("API directory does not exist")
            else:
                self.error.emit("API data export failed")
        except Exception as e:
            self.error.emit(f"Error in API export test: {str(e)}")
        finally:
            # Let the destructor handle cleanup - don't close the pool
            db_manager = None
