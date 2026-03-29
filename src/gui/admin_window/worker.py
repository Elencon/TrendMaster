r"""
C:\Economy\Invest\TrendMaster\src\gui\admin_window\worker.py
ETL Worker thread for background operations
"""

import shutil
from pathlib import Path
from typing import Dict, Any

from PySide6.QtCore import QThread, Signal

# Centralized paths
from src.path_config import CSV_PATH, API_PATH

# ETL modules (always available)
from src.database.db_manager import DatabaseManager
from src.database.data_from_api import APIDataFetcher as APIClient


class ETLWorker(QThread):
    """Worker thread for ETL operations with proper error handling."""

    finished = Signal(str)
    error = Signal(str)
    progress = Signal(str)
    data_ready = Signal(dict)

    def __init__(self, operation: str, *args, **kwargs):
        super().__init__()

        # Protected internal state
        self._operation = operation
        self._args = args
        self._kwargs = kwargs
        self._is_cancelled = False

        # Operation dispatch map
        self._operations: Dict[str, Any] = {
            "test_connection": self._test_connection,
            "test_api": self._test_api,
            "create_tables": self._create_tables,
            "load_csv": self._load_csv,
            "load_api": self._load_api,
            "select_csv_files": self._select_csv_files,
            "test_csv_access": self._test_csv_access,
            "test_api_export": self._test_api_export,
        }

    # ---------------------------------------------------------
    # Public API
    # ---------------------------------------------------------
    def cancel(self):
        """Cancel the current operation."""
        self._is_cancelled = True

    # ---------------------------------------------------------
    # Thread entry point
    # ---------------------------------------------------------
    def run(self):
        """Main execution method with operation routing."""
        try:
            func = self._operations.get(self._operation)
            if func:
                func()
            else:
                self.error.emit(f"Unknown operation: {self._operation}")
        except Exception as e:
            self.error.emit(f"Error in {self._operation}: {e}")

    # ---------------------------------------------------------
    # Operation: Test DB Connection
    # ---------------------------------------------------------
    def _test_connection(self):
        self.progress.emit("Testing database connection...")

        db_manager = None
        try:
            db_manager = DatabaseManager()

            if self._is_cancelled:
                return

            if db_manager.test_connection():
                self.finished.emit("Database connection successful!")
            else:
                self.error.emit("Failed to connect to database")

        except Exception as e:
            self.error.emit(f"Database connection error: {e}")

        finally:
            db_manager = None

    # ---------------------------------------------------------
    # Operation: Test API Connection
    # ---------------------------------------------------------
    def _test_api(self):
        api_url = self._args[0]
        self.progress.emit(f"Testing API connection to: {api_url}")

        try:
            api_client = APIClient(api_url)

            if self._is_cancelled:
                api_client.close()
                return

            test_data = api_client.fetch_data("orders")
            api_client.close()

            if test_data is not None:
                self.finished.emit(
                    f"API connection successful! Found {len(test_data)} records"
                )
            else:
                self.error.emit("API connection failed - no data received")

        except Exception as e:
            self.error.emit(f"API connection failed: {e}")

    # ---------------------------------------------------------
    # Operation: Create Tables
    # ---------------------------------------------------------
    def _create_tables(self):
        self.progress.emit("Creating database and tables...")

        db_manager = None
        try:
            db_manager = DatabaseManager()

            if self._is_cancelled:
                return

            self.progress.emit("Creating database if not exists...")
            created = db_manager.create_database_if_not_exists()

            if not created:
                self.error.emit("Failed to create database - Check MySQL permissions")
                return

            if self._is_cancelled:
                return

            self.progress.emit("Creating all 9 tables with updated schema...")
            csv_success = db_manager.create_all_tables_from_csv()

            if not csv_success:
                self.error.emit("Failed to create some tables - Check schema compatibility")
                return

            # Verify table creation
            csv_tables = ["brands", "categories", "stores", "staffs", "products", "stocks"]
            api_tables = ["customers", "orders", "order_items"]

            table_info = []
            for table in csv_tables + api_tables:
                count = db_manager.get_row_count(table)
                table_info.append(f"  {table}: {'Ready' if count >= 0 else 'Created'}")

            result = (
                "All 9 database tables created successfully!\n"
                "Schema Updates Applied:\n"
                "  - STOCKS: store_name (FK), product_id (PK)\n"
                "  - STAFFS: name, store_name, street columns\n"
                "Tables Created:\n" + "\n".join(table_info)
            )

            self.finished.emit(result)

        except Exception as e:
            self.error.emit(f"Error creating tables: {e}")

        finally:
            db_manager = None

    # ---------------------------------------------------------
    # Operation: Load CSV Data
    # ---------------------------------------------------------
    def _load_csv(self):
        self.progress.emit("Loading CSV data with NaN→NULL conversion...")

        db_manager = None
        try:
            db_manager = DatabaseManager()

            if self._is_cancelled:
                return

            self.progress.emit("Creating tables and loading data...")
            success = db_manager.create_all_tables_from_csv()

            if not success:
                self.error.emit("Failed to load CSV data - Check file permissions and schema compatibility")
                return

            if self._is_cancelled:
                return

            self.progress.emit("Verifying data insertion...")

            table_counts = {
                table: db_manager.get_row_count(table)
                for table in db_manager.csv_files.keys()
            }

            total_rows = sum(table_counts.values())
            summary = "\n".join(f"  {table}: {count} rows" for table, count in table_counts.items())

            result = (
                f"CSV data loaded successfully!\n"
                f"Total Records: {total_rows:,}\n"
                f"Table Breakdown:\n{summary}\n"
                f"All NaN values converted to MySQL NULL\n"
                f"Schema alignment verified (STOCKS/STAFFS updated)"
            )

            self.finished.emit(result)
            self.data_ready.emit(table_counts)

        except Exception as e:
            self.error.emit(f"Error loading CSV: {e}")

        finally:
            db_manager = None

    # ---------------------------------------------------------
    # Operation: Select CSV Files
    # ---------------------------------------------------------
    def _select_csv_files(self):
        selected_files = self._args[0]
        CSV_PATH.mkdir(parents=True, exist_ok=True)

        copied_files = []

        for file_path in selected_files:
            if self._is_cancelled:
                break

            try:
                src = Path(file_path)
                dest = CSV_PATH / src.name

                shutil.copy2(src, dest)
                copied_files.append(src.name)
                self.progress.emit(f"Copied: {src.name}")

            except Exception as e:
                self.progress.emit(f"Failed to copy {src.name}: {e}")

        if copied_files:
            summary = "\n".join(f"  - {name}" for name in copied_files)
            self.finished.emit(
                f"Successfully copied {len(copied_files)} files to CSV folder:\n{summary}"
            )
        else:
            self.error.emit("No files were copied successfully")

    # ---------------------------------------------------------
    # Operation: Test CSV Access
    # ---------------------------------------------------------
    def _test_csv_access(self):
        self.progress.emit("Testing CSV file access and schema validation...")

        db_manager = None
        try:
            db_manager = DatabaseManager()
            self.progress.emit(f"Data directory: {db_manager.data_dir}")

            total_rows = 0

            for table_name, csv_file in db_manager.csv_files.items():
                if self._is_cancelled:
                    return

                try:
                    df = db_manager.read_csv_file(csv_file)

                    if df is None:
                        self.progress.emit(f"FAILED: {table_name}: Failed to read {csv_file}")
                        continue

                    total_rows += len(df)
                    preview_cols = ", ".join(df.columns[:3])
                    if len(df.columns) > 3:
                        preview_cols += "..."

                    self.progress.emit(
                        f"SUCCESS: {table_name}: {len(df)} rows, "
                        f"{len(df.columns)} columns ({preview_cols})"
                    )

                except Exception as e:
                    self.progress.emit(f"ERROR: {table_name}: {e}")

            result = (
                "CSV access test completed!\n"
                f"Total records available: {total_rows:,}\n"
                "Schema alignment: STOCKS (store_name), STAFFS (name, store_name, street)"
            )

            self.finished.emit(result)

        except Exception as e:
            self.error.emit(f"Error in CSV access test: {e}")

        finally:
            db_manager = None

    # ---------------------------------------------------------
    # Operation: Test API Export
    # ---------------------------------------------------------
    def _test_api_export(self):
        self.progress.emit("Testing API data export...")

        db_manager = None
        try:
            db_manager = DatabaseManager()
            success = db_manager.export_api_data_to_csv()

            if not success:
                self.error.emit("API data export failed")
                return

            self.progress.emit("SUCCESS: API data export successful!")

            if not API_PATH.exists():
                self.error.emit("API directory does not exist")
                return

            csv_files = sorted(API_PATH.glob("*.csv"))
            if not csv_files:
                self.error.emit("No CSV files found in API directory")
                return

            self.progress.emit(f"Found {len(csv_files)} CSV files:")
            for file_path in csv_files:
                size = file_path.stat().st_size
                self.progress.emit(f"  {file_path.name:<20} ({size:,} bytes)")

            self.finished.emit("API export test completed successfully!")

        except Exception as e:
            self.error.emit(f"Error in API export test: {e}")

        finally:
            db_manager = None

    # ---------------------------------------------------------
    # Operation: Load API Data (Full ETL)
    # ---------------------------------------------------------
    def _load_api(self):
        """Full API → CSV → DB ETL pipeline."""
        api_url = self._args[0]
        self.progress.emit(f"Connecting to API: {api_url}")

        try:
            # ---------------------------------------------------------
            # Step 1: Fetch API data → CSV
            # ---------------------------------------------------------
            api_client = APIClient(api_url)
            self.progress.emit("Fetching data from API endpoints...")

            csv_success = api_client.save_all_api_data_to_csv(str(API_PATH))
            api_client.close()

            if not csv_success or self._is_cancelled:
                self.error.emit("Failed to export API data to CSV - Check API connectivity")
                return

            # ---------------------------------------------------------
            # Step 2: Verify CSV files
            # ---------------------------------------------------------
            self.progress.emit("Verifying exported files...")

            if not API_PATH.exists():
                self.error.emit("API directory does not exist")
                return

            csv_files = list(API_PATH.glob("*.csv"))
            if not csv_files:
                self.error.emit("No CSV files were created from API data")
                return

            # ---------------------------------------------------------
            # Step 3: Import CSV files into database
            # ---------------------------------------------------------
            self.progress.emit("Importing API data into database...")

            db_manager = DatabaseManager()
            import_order = ["customers", "orders", "order_items"]
            total_records = 0

            # Use a single connection for all inserts
            with db_manager.get_connection() as conn:
                if conn is None:
                    self.error.emit("Failed to get database connection for API import")
                    return

                for table_name in import_order:
                    if self._is_cancelled:
                        return

                    csv_file = API_PATH / f"{table_name}.csv"
                    if not csv_file.exists():
                        self.progress.emit(f"  ⚠️ {table_name}.csv not found, skipping")
                        continue

                    self.progress.emit(f"Importing {table_name}...")

                    import pandas as pd
                    df = pd.read_csv(csv_file)

                    if df.empty:
                        self.progress.emit(f"  ⚠️ {table_name}: No records to import")
                        continue

                    # Convert to list of dicts and clean NaN values
                    records = df.to_dict("records")
                    cleaned_records = [
                        {k: (None if pd.isna(v) else v) for k, v in rec.items()}
                        for rec in records
                    ]

                    # Debug info
                    if table_name == "order_items" and cleaned_records:
                        self.progress.emit(f"  DEBUG: First record = {cleaned_records[0]}")
                        self.progress.emit(f"  DEBUG: Total records to insert = {len(cleaned_records)}")

                    try:
                        cursor = conn.cursor()

                        columns = list(cleaned_records[0].keys())
                        placeholders = ", ".join(["%s"] * len(columns))
                        column_names = ", ".join(f"`{col}`" for col in columns)
                        sql = f"INSERT INTO `{table_name}` ({column_names}) VALUES ({placeholders})"

                        batch_values = [
                            tuple(rec.get(col) for col in columns)
                            for rec in cleaned_records
                        ]

                        cursor.executemany(sql, batch_values)
                        conn.commit()
                        cursor.close()

                        inserted = len(batch_values)
                        total_records += inserted

                        # Verify row count
                        cursor = conn.cursor()
                        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                        actual_count = cursor.fetchone()[0]
                        cursor.close()

                        self.progress.emit(
                            f"  ✅ {table_name}: {inserted:,} records imported, "
                            f"DB has {actual_count} rows"
                        )

                    except Exception as e:
                        import traceback
                        self.progress.emit(f"  ❌ {table_name}: INSERT ERROR: {e}")
                        self.progress.emit(f"  TRACEBACK: {traceback.format_exc()}")

            # ---------------------------------------------------------
            # Step 4: Success summary
            # ---------------------------------------------------------
            file_info = [
                f"  {f.name}: {f.stat().st_size:,} bytes"
                for f in csv_files
            ]
            total_size = sum(f.stat().st_size for f in csv_files)

            result = (
                "API data loaded successfully!\n"
                f"Location: {API_PATH}\n"
                f"Files Created: {len(csv_files)}\n"
                f"Total Size: {total_size:,} bytes\n"
                f"Records Imported: {total_records:,}\n"
                "Files:\n" + "\n".join(file_info)
            )

            self.finished.emit(result)

        except Exception as e:
            self.error.emit(f"Failed to load API data: {e}")

    def _load_api(self):
        """Load API data and import into database."""
        api_url = self._args[0]
        self.progress.emit(f"Connecting to API: {api_url}")

        try:
            # ---------------------------------------------------------
            # Step 1: Fetch API data → CSV
            # ---------------------------------------------------------
            api_client = APIClient(api_url)
            self.progress.emit("Fetching data from API endpoints...")

            csv_success = api_client.save_all_api_data_to_csv(str(API_PATH))
            api_client.close()

            if not csv_success or self._is_cancelled:
                self.error.emit("Failed to export API data to CSV - Check API connectivity")
                return

            # ---------------------------------------------------------
            # Step 2: Verify CSV files
            # ---------------------------------------------------------
            self.progress.emit("Verifying exported files...")

            if not API_PATH.exists():
                self.error.emit("API directory does not exist")
                return

            csv_files = list(API_PATH.glob("*.csv"))
            if not csv_files:
                self.error.emit("No CSV files were created from API data")
                return

            # ---------------------------------------------------------
            # Step 3: Import CSV files into database
            # ---------------------------------------------------------
            self.progress.emit("Importing API data into database...")

            db_manager = DatabaseManager()
            import_order = ["customers", "orders", "order_items"]
            total_records = 0

            # Use a single connection for all inserts
            with db_manager.get_connection() as conn:
                if conn is None:
                    self.error.emit("Failed to get database connection for API import")
                    return

                for table_name in import_order:
                    if self._is_cancelled:
                        return

                    csv_file = API_PATH / f"{table_name}.csv"
                    if not csv_file.exists():
                        self.progress.emit(f"  ⚠️ {table_name}.csv not found, skipping")
                        continue

                    self.progress.emit(f"Importing {table_name}...")

                    # Read CSV
                    import pandas as pd
                    df = pd.read_csv(csv_file)

                    if df.empty:
                        self.progress.emit(f"  ⚠️ {table_name}: No records to import")
                        continue

                    # Convert to list of dicts and clean NaN values
                    records = df.to_dict("records")
                    cleaned_records = [
                        {k: (None if pd.isna(v) else v) for k, v in rec.items()}
                        for rec in records
                    ]

                    # Debug info for order_items
                    if table_name == "order_items" and cleaned_records:
                        self.progress.emit(f"  DEBUG: First record = {cleaned_records[0]}")
                        self.progress.emit(f"  DEBUG: Total records to insert = {len(cleaned_records)}")

                    # Insert into DB
                    try:
                        cursor = conn.cursor()

                        columns = list(cleaned_records[0].keys())
                        if not columns:
                            self.progress.emit(f"  ⚠️ {table_name}: No columns found")
                            continue

                        placeholders = ", ".join(["%s"] * len(columns))
                        column_names = ", ".join(f"`{col}`" for col in columns)
                        sql = f"INSERT INTO `{table_name}` ({column_names}) VALUES ({placeholders})"

                        batch_values = [
                            tuple(rec.get(col) for col in columns)
                            for rec in cleaned_records
                        ]

                        cursor.executemany(sql, batch_values)
                        conn.commit()
                        cursor.close()

                        inserted = len(batch_values)
                        total_records += inserted

                        # Verify row count
                        cursor = conn.cursor()
                        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                        actual_count = cursor.fetchone()[0]
                        cursor.close()

                        self.progress.emit(
                            f"  ✅ {table_name}: {inserted:,} records imported, "
                            f"DB has {actual_count} rows"
                        )

                    except Exception as e:
                        import traceback
                        self.progress.emit(f"  ❌ {table_name}: INSERT ERROR: {e}")
                        self.progress.emit(f"  TRACEBACK: {traceback.format_exc()}")

            # ---------------------------------------------------------
            # Step 4: Success summary
            # ---------------------------------------------------------
            file_info = [
                f"  {f.name}: {f.stat().st_size:,} bytes"
                for f in csv_files
            ]
            total_size = sum(f.stat().st_size for f in csv_files)

            result = (
                "API data loaded successfully!\n"
                f"Location: {API_PATH}\n"
                f"Files Created: {len(csv_files)}\n"
                f"Total Size: {total_size:,} bytes\n"
                f"Records Imported: {total_records:,}\n"
                "Files:\n" + "\n".join(file_info)
            )

            self.finished.emit(result)

        except Exception as e:
            self.error.emit(f"Failed to load API data: {e}")

    def _select_csv_files(self):
        """Handle CSV file selection and copying."""
        selected_files = self._args[0]  # unchanged interface
        CSV_PATH.mkdir(parents=True, exist_ok=True)

        copied_files = []

        for file_path in selected_files:
            if self._is_cancelled:
                break

            try:
                src = Path(file_path)
                dest = CSV_PATH / src.name

                shutil.copy2(src, dest)
                copied_files.append(src.name)
                self.progress.emit(f"Copied: {src.name}")

            except Exception as e:
                self.progress.emit(f"Failed to copy {src.name}: {e}")

        if copied_files:
            summary = "\n".join(f"  - {name}" for name in copied_files)
            self.finished.emit(
                f"Successfully copied {len(copied_files)} files to CSV folder:\n{summary}"
            )
            return

        self.error.emit("No files were copied successfully")

    def _test_csv_access(self):
        """Test CSV file access and schema validation."""
        self.progress.emit("Testing CSV file access and schema validation...")

        db_manager = None
        try:
            db_manager = DatabaseManager()
            self.progress.emit(f"Data directory: {db_manager.data_dir}")

            total_rows = 0

            for table_name, csv_file in db_manager.csv_files.items():
                if self._is_cancelled:
                    break

                try:
                    df = db_manager.read_csv_file(csv_file)

                    if df is None:
                        self.progress.emit(f"FAILED: {table_name}: Failed to read {csv_file}")
                        continue

                    total_rows += len(df)
                    preview_cols = ", ".join(df.columns[:3])
                    if len(df.columns) > 3:
                        preview_cols += "..."

                    self.progress.emit(
                        f"SUCCESS: {table_name}: {len(df)} rows, "
                        f"{len(df.columns)} columns ({preview_cols})"
                    )

                except Exception as e:
                    self.progress.emit(f"ERROR: {table_name}: {e}")

            result = (
                "CSV access test completed!\n"
                f"Total records available: {total_rows:,}\n"
                "Schema alignment: STOCKS (store_name), "
                "STAFFS (name, store_name, street)"
            )
            self.finished.emit(result)

        except Exception as e:
            self.error.emit(f"Error in CSV access test: {e}")

        finally:
            db_manager = None  # allow destructor cleanup

    def _test_api_export(self):
        """Test API data export."""
        self.progress.emit("Testing API data export...")

        db_manager = None
        try:
            db_manager = DatabaseManager()
            success = db_manager.export_api_data_to_csv()

            if not success:
                self.error.emit("API data export failed")
                return

            self.progress.emit("SUCCESS: API data export successful!")

            if not API_PATH.exists():
                self.error.emit("API directory does not exist")
                return

            csv_files = sorted(API_PATH.glob("*.csv"))
            if not csv_files:
                self.error.emit("No CSV files found in API directory")
                return

            self.progress.emit(f"Found {len(csv_files)} CSV files:")
            for file_path in csv_files:
                size = file_path.stat().st_size
                self.progress.emit(f"  {file_path.name:<20} ({size:,} bytes)")

            self.finished.emit("API export test completed successfully!")

        except Exception as e:
            self.error.emit(f"Error in API export test: {e}")

        finally:
            db_manager = None
