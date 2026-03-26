r"""
C:\Economy\Invest\TrendMaster\src\database\db_manager.py
Enhanced Database Manager with batch processing and connection pooling.
Provides comprehensive ETL operations with high-performance batch operations.
"""

import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from contextlib import contextmanager
import pandas as pd
from .pandas_optimizer import (
    PandasOptimizer,
    optimize_csv_reading,
)
from src.exceptions import ETLException, ErrorContext, create_database_error
from .data_validator import DataValidator
from src.logging_system import get_database_logger, performance_context
from src.config import get_config

# ────────────────────────────────────────────────
# Required imports (always present)
# ────────────────────────────────────────────────

from .connection_manager import DatabaseConnection
from .schema_manager import SCHEMA_DEFINITIONS, TABLE_COLUMNS

# Legacy config (optional)

legacy_config = get_config().database.to_dict()

# ────────────────────────────────────────────────
# Logger
# ────────────────────────────────────────────────

_logger = get_database_logger()

def _isna(value) -> bool:
    """
    Safely test whether *value* is a pandas NA/NaN scalar.

    ``pd.isna`` raises on array-like inputs and returns a bool array for
    sequences — neither of which we want here.  Guard with an explicit None
    check first so we only call ``isna`` when there is an actual value to
    inspect.
    """
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


class BatchProcessor:
    """High-performance batch processing for database operations."""

    def __init__(self, connection_manager: DatabaseConnection, batch_size: int = 1000):
        self.connection_manager = connection_manager
        self.batch_size = batch_size
        self.processed_count = 0
        self.error_count = 0

    def insert_batch(self, table_name: str, data: List[Dict],
                    progress_callback: Optional[callable] = None,
                    ignore_duplicates: bool = False) -> Tuple[int, int]:
        """
        Insert data in batches with progress tracking.

        Args:
            table_name: Target table name.
            data: List of row dictionaries.
            progress_callback: Optional progress callback function.
            ignore_duplicates: Use INSERT IGNORE for duplicate handling.

        Returns:
            Tuple of (successful_inserts, errors).
        """
        if not data:
            return 0, 0

        total_records = len(data)
        successful_inserts = 0
        errors = 0

        context_manager = performance_context(f"batch_insert_{table_name}", _logger)

        with context_manager:
            _logger.info("Starting batch insert of %d records into %s", total_records, table_name)

            columns = list(data[0].keys()) if data else []
            if not columns:
                _logger.error("No columns found in data")
                return 0, 1

            placeholders = ', '.join(['%s'] * len(columns))
            column_names = ', '.join(f"`{col}`" for col in columns)
            insert_type = "INSERT IGNORE" if ignore_duplicates else "INSERT"
            sql = f"{insert_type} INTO `{table_name}` ({column_names}) VALUES ({placeholders})"

            for i in range(0, total_records, self.batch_size):
                batch_end = min(i + self.batch_size, total_records)
                batch_data = data[i:batch_end]

                try:
                    with self.connection_manager.get_connection() as conn:
                        if conn is None:
                            _logger.error("Failed to get database connection")
                            errors += len(batch_data)
                            continue

                        batch_values = [
                            tuple(None if _isna(record.get(col)) else record.get(col) for col in columns)
                            for record in batch_data
                        ]

                        with conn.cursor() as cursor:
                            cursor.executemany(sql, batch_values)
                        conn.commit()

                        batch_success = len(batch_values)
                        successful_inserts += batch_success

                        if progress_callback:
                            progress = (batch_end / total_records) * 100
                            progress_callback(
                                f"Inserted {successful_inserts}/{total_records} records ({progress:.1f}%)"
                            )

                        _logger.debug(
                            "Batch %d: inserted %d records into %s",
                            i // self.batch_size + 1, batch_success, table_name,
                        )

                except Exception as e:
                    _logger.error(
                        "Error inserting batch %d into %s: %s",
                        i // self.batch_size + 1, table_name, e,
                    )
                    errors += len(batch_data)

                    context = ErrorContext(
                        operation="batch_insert",
                        component="batch_processor",
                        table_name=table_name,
                        record_count=len(batch_data)
                    )
                    db_error = create_database_error(
                        f"Batch insert failed for table {table_name}",
                        original_exception=e,
                        context=context
                    )
                    _logger.error("Structured error info: %s", db_error.to_dict())

            _logger.info(
                "Completed batch insert: %d/%d records inserted into %s, %d errors",
                successful_inserts, total_records, table_name, errors,
            )

            self.processed_count += successful_inserts
            self.error_count += errors

            return successful_inserts, errors

    @contextmanager
    def _dummy_context(self):
        """Dummy context manager when performance tracking is not available."""
        yield

    def update_batch(self, table_name: str, updates: List[Dict],
                    key_columns: List[str],
                    progress_callback: Optional[callable] = None) -> Tuple[int, int]:
        """
        Update records in batches.

        Args:
            table_name: Target table name.
            updates: List of update dictionaries.
            key_columns: Column names to use as WHERE conditions.
            progress_callback: Optional progress callback.

        Returns:
            Tuple of (successful_updates, errors).
        """
        if not updates:
            return 0, 0

        successful_updates = 0
        errors = 0
        total_records = len(updates)

        for i, record in enumerate(updates):
            try:
                with self.connection_manager.get_connection() as conn:
                    if conn is None:
                        errors += 1
                        continue

                    set_columns   = [col for col in record if col not in key_columns]
                    set_clause    = ', '.join(f"`{col}` = %s" for col in set_columns)
                    where_clause  = ' AND '.join(f"`{col}` = %s" for col in key_columns)
                    sql = f"UPDATE `{table_name}` SET {set_clause} WHERE {where_clause}"

                    set_values   = [None if _isna(record.get(col)) else record.get(col) for col in set_columns]
                    where_values = [record[col] for col in key_columns]

                    with conn.cursor() as cursor:
                        cursor.execute(sql, set_values + where_values)
                        successful_updates += cursor.rowcount
                    conn.commit()

                    if progress_callback and (i + 1) % self.batch_size == 0:
                        progress = ((i + 1) / total_records) * 100
                        progress_callback(f"Updated {i + 1}/{total_records} records ({progress:.1f}%)")

            except Exception as e:
                _logger.error("Error updating record %d: %s", i + 1, e)
                errors += 1

        return successful_updates, errors

    def upsert_batch(self, table_name: str, data: List[Dict],
                    key_columns: List[str],
                    progress_callback: Optional[callable] = None) -> Tuple[int, int, int]:
        """
        Insert or update records in batches (MySQL ON DUPLICATE KEY UPDATE).

        Args:
            table_name: Target table name.
            data: List of row dictionaries.
            key_columns: Columns that define uniqueness.
            progress_callback: Optional progress callback.

        Returns:
            Tuple of (inserts, updates, errors).
        """
        if not data:
            return 0, 0, 0

        inserts = 0
        updates = 0
        errors = 0
        total_records = len(data)

        columns = list(data[0].keys()) if data else []
        if not columns:
            return 0, 0, 1

        column_names  = ', '.join(f"`{col}`" for col in columns)
        placeholders  = ', '.join(['%s'] * len(columns))
        update_columns = [col for col in columns if col not in key_columns]
        update_clause = ', '.join(f"`{col}` = VALUES(`{col}`)" for col in update_columns)

        sql = (
            f"INSERT INTO `{table_name}` ({column_names}) "
            f"VALUES ({placeholders}) "
            f"ON DUPLICATE KEY UPDATE {update_clause}"
        )

        for i in range(0, total_records, self.batch_size):
            batch_end  = min(i + self.batch_size, total_records)
            batch_data = data[i:batch_end]

            try:
                with self.connection_manager.get_connection() as conn:
                    if conn is None:
                        errors += len(batch_data)
                        continue

                    batch_inserts = 0
                    batch_updates = 0

                    with conn.cursor() as cursor:
                        for record in batch_data:
                            values = [
                                None if _isna(record.get(col)) else record.get(col)
                                for col in columns
                            ]
                            cursor.execute(sql, values)
                            # MySQL: rowcount == 1 → insert, 2 → update, 0 → no-op
                            if cursor.rowcount == 1:
                                batch_inserts += 1
                            elif cursor.rowcount == 2:
                                batch_updates += 1

                    conn.commit()
                    inserts += batch_inserts
                    updates += batch_updates

                    if progress_callback:
                        progress = (batch_end / total_records) * 100
                        progress_callback(
                            f"Upserted {batch_end}/{total_records} records ({progress:.1f}%)"
                        )

            except Exception as e:
                _logger.error("Error in upsert batch: %s", e)
                errors += len(batch_data)

        return inserts, updates, errors

    def get_stats(self) -> Dict[str, int]:
        """Get processing statistics."""
        return {
            'processed_count': self.processed_count,
            'error_count': self.error_count,
            'batch_size': self.batch_size
        }


class DatabaseManager:
    """Enhanced Database Manager with batch processing and connection pooling."""

    def __init__(self, config: Dict = None, data_dir: Path = None, logger_instance=None,
                 enable_pooling: bool = True, pool_size: int = 5):
        """
        Initialize database manager with enhanced connection management.

        Args:
            config: Database configuration dict or ETLConfig instance.
            data_dir: Data directory path.
            logger_instance: Logger instance (unused; kept for API compatibility).
            enable_pooling: Enable connection pooling.
            pool_size: Connection pool size.
        """

        if config is None:
            etl_config = get_config()
            self.config = etl_config.database.to_dict()
            self.etl_config = etl_config
        elif isinstance(config, dict):
            self.config = config
            self.etl_config = None
        else:
            self.config = config.database.to_dict()
            self.etl_config = config

        self.data_dir = data_dir or Path(__file__).parent.parent.parent / 'data'
        self.csv_dir  = self.data_dir / 'CSV'
        self.api_dir  = self.data_dir / 'API'

        self.db_connection = DatabaseConnection(
            config=self.config,
            enable_pooling=enable_pooling,
            pool_size=pool_size
        )
        self.batch_processor = BatchProcessor(self.db_connection)

        if self.etl_config and hasattr(self.etl_config, "processing"):
            chunk_size = self.etl_config.processing.chunk_size
            max_memory_mb = self.etl_config.processing.max_memory_usage_mb
        else:
            chunk_size = 5000
            max_memory_mb = 512

        self.pandas_optimizer = PandasOptimizer(
            max_memory_usage_mb=max_memory_mb,
            chunk_size=chunk_size,
            optimize_dtypes=True
        )

        self.data_validator = DataValidator()

        self.csv_files = {
            'brands':     'brands.csv',
            'categories': 'categories.csv',
            'stores':     'stores.csv',
            'staffs':     'staffs.csv',
            'products':   'products.csv',
            'stocks':     'stocks.csv',
        }
        self.api_tables = {
            'customers':   'customers',
            'orders':      'orders',
            'order_items': 'order_items',
        }

        # Single source of truth — imported from schema_manager to avoid drift.
        # TABLE_COLUMNS is validated against SCHEMA_DEFINITIONS at import time.
        self.table_schemas = TABLE_COLUMNS

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    @contextmanager
    def get_connection(self):
        """Get database connection context manager."""
        with self.db_connection.get_connection() as conn:
            yield conn

    def test_connection(self) -> bool:
        """Test database connection."""
        return self.db_connection.test_connection()

    def create_database_if_not_exists(self, database_name: str = None) -> bool:
        """Create database if it doesn't exist."""
        return self.db_connection.create_database_if_not_exists(database_name)

    def get_connection_stats(self) -> Dict:
        """Get connection and batch-processing statistics."""
        stats = self.db_connection.get_connection_stats()
        stats.update(self.batch_processor.get_stats())
        return stats

    # ------------------------------------------------------------------
    # Schema / introspection
    # ------------------------------------------------------------------

    def get_all_tables(self) -> List[str]:
        """Return all table names present in the connected database."""
        try:
            with self.get_connection() as conn:
                if conn is None:
                    return []
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema = DATABASE()"
                    )
                    return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            _logger.error("Failed to get tables: %s", e)
            return []

    def get_row_count(self, table_name: str) -> int:
        """Return the row count for *table_name*, or -1 on error."""
        try:
            with self.get_connection() as conn:
                if conn is None:
                    return -1
                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                    result = cursor.fetchone()
                return result[0] if result else 0
        except Exception as e:
            _logger.debug("Error getting row count for %s: %s", table_name, e)
            return -1

    def get_total_sales(self) -> float:
        """Return total revenue from order_items (quantity × price × (1 − discount))."""
        try:
            with self.get_connection() as conn:
                if conn is None:
                    return 0.0
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT SUM(quantity * list_price * (1 - discount)) "
                        "FROM order_items"
                    )
                    result = cursor.fetchone()
                return float(result[0]) if result and result[0] is not None else 0.0
        except Exception as e:
            _logger.error("Failed to get total sales: %s", e)
            return 0.0

    # ------------------------------------------------------------------
    # DDL
    # ------------------------------------------------------------------

    def create_all_tables_from_csv(self) -> bool:
        """Create all database tables in foreign-key-safe order."""
        from .schema_manager import DEFAULT_TABLE_ORDER
        try:
            with self.get_connection() as conn:
                if conn is None:
                    _logger.error("Failed to get database connection for table creation")
                    return False

                with conn.cursor() as cursor:
                    for table_name in DEFAULT_TABLE_ORDER:
                        if table_name in SCHEMA_DEFINITIONS:
                            cursor.execute(SCHEMA_DEFINITIONS[table_name])
                            _logger.info("Created/verified table: %s", table_name)

                conn.commit()
                _logger.info("All tables created successfully")
                return True

        except Exception as e:
            _logger.error("Error creating tables: %s", e)
            return False

    # ------------------------------------------------------------------
    # Validation (belongs on DatabaseManager, not BatchProcessor)
    # ------------------------------------------------------------------

    def validate_dataframe(self, df: pd.DataFrame, table_name: str = None) -> Optional[Any]:
        """
        Validate *df* with the configured DataValidator.

        Args:
            df: DataFrame to validate.
            table_name: Name of the target table (used for logging context).

        Returns:
            ValidationSummary, or None if the validator is unavailable or *df* is empty.
        """
        if not self.data_validator or df is None or df.empty:
            return None

        _logger.info("Validating DataFrame for table '%s': %s", table_name, df.shape)

        auto_rules    = self.data_validator.create_schema_from_dataframe(df)
        original_rules = self.data_validator.rules.copy()
        for rule in auto_rules:
            if not any(r.name == rule.name for r in self.data_validator.rules):
                self.data_validator.add_rule(rule)

        try:
            summary = self.data_validator.validate_dataframe(df, stop_on_critical=False)

            if summary.failed_rules > 0:
                _logger.warning("Validation found issues: %d failed rules", summary.failed_rules)
                for failure in summary.get_critical_failures():
                    _logger.error("  Critical — %s: %s", failure.rule_name, failure.message)
                for failure in summary.get_errors()[:3]:
                    _logger.warning("  Error — %s: %s", failure.rule_name, failure.message)
            else:
                _logger.info("Validation passed: all %d rules successful", summary.total_rules)

            return summary
        finally:
            self.data_validator.rules = original_rules

    # ------------------------------------------------------------------
    # CSV / ETL
    # ------------------------------------------------------------------

    def read_csv_file(self, csv_filename: str) -> Optional[pd.DataFrame]:
        """Read a CSV file with optimised memory usage and structured error handling."""
        context = ErrorContext(
            operation="read_csv_file",
            component="database_manager",
            file_path=str(csv_filename)
        )

        try:
            file_path = self.csv_dir / csv_filename
            if not file_path.exists():
                error_msg = f"CSV file not found: {file_path}"
                _logger.error(error_msg)
                from src.exceptions import FileSystemError
                raise FileSystemError(
                    error_msg, error_code="CSV_FILE_NOT_FOUND", context=context
                )

            if self.pandas_optimizer:
                _logger.debug("Reading %s with pandas optimisation", csv_filename)
                df = optimize_csv_reading(file_path, optimize_dtypes=True)
            else:
                _logger.debug("Reading %s with standard pandas", csv_filename)
                df = pd.read_csv(file_path)

            df = df.where(pd.notna(df), None)

            if self.pandas_optimizer:
                profile = self.pandas_optimizer.get_data_profile(df)
                _logger.info(
                    "Read %s: %s shape, %.2fMB, %d categorical columns",
                    csv_filename, profile['shape'],
                    profile['memory_usage_mb'],
                    len(profile['categorical_columns']),
                )
            else:
                _logger.debug("Read %d rows from %s", len(df), csv_filename)

            if self.data_validator:
                table_name = csv_filename.replace('.csv', '')
                summary = self.validate_dataframe(df, table_name)
                if summary and summary.failed_rules > 0:
                    _logger.warning("Data validation found %d issues in %s", summary.failed_rules, csv_filename)
                    if summary.get_critical_failures():
                        _logger.error("Critical validation failures in %s — manual review advised", csv_filename)
                    else:
                        cleaned_df, fixes = self.data_validator.clean_data(df, summary)
                        if fixes:
                            _logger.info("Applied automatic fixes to %s: %s", csv_filename, fixes)
                            df = cleaned_df

            return df

        except Exception as e:
            error_msg = f"Error reading CSV file {csv_filename}: {e}"
            _logger.error(error_msg)

            if not isinstance(e, ETLException):
                if "memory" in str(e).lower():
                    from src.exceptions import MemoryError as ETLMemoryError
                    raise ETLMemoryError(
                        f"Memory error reading CSV file: {e}", context=context, original_exception=e
                    )
                else:
                    from src.exceptions import FileSystemError
                    raise FileSystemError(
                        error_msg, error_code="CSV_READ_ERROR", context=context, original_exception=e
                    )
            elif isinstance(e, ETLException):
                raise

            return None

    def import_csv_data(self) -> bool:
        """Import all CSV data with batch processing."""
        try:
            total_inserted = 0
            total_errors   = 0

            import_order = ['brands', 'categories', 'stores', 'staffs', 'products', 'stocks']

            for table_name in import_order:
                if table_name not in self.csv_files:
                    continue

                df = self.read_csv_file(self.csv_files[table_name])
                if df is None:
                    _logger.warning("Skipping %s — file not found or read error", table_name)
                    continue

                records = df.to_dict('records')
                if not records:
                    _logger.warning("No records found in %s", self.csv_files[table_name])
                    continue

                inserted, errors = self.batch_processor.insert_batch(
                    table_name=table_name,
                    data=records,
                    progress_callback=lambda msg, t=table_name: _logger.info("%s: %s", t, msg),
                    ignore_duplicates=True,
                )

                total_inserted += inserted
                total_errors   += errors
                _logger.info("Table %s: %d inserted, %d errors", table_name, inserted, errors)

            _logger.info(
                "CSV import completed: %d total records inserted, %d errors",
                total_inserted, total_errors,
            )
            return total_errors == 0

        except Exception as e:
            _logger.error("Error importing CSV data: %s", e)
            return False

    def export_api_data_to_csv(self) -> bool:
        """Export API data to CSV files (placeholder for compatibility)."""
        try:
            self.api_dir.mkdir(parents=True, exist_ok=True)
            _logger.info("API data export functionality — placeholder implementation")
            return True
        except Exception as e:
            _logger.error("Error exporting API data: %s", e)
            return False

    def verify_data(self) -> Dict[str, int]:
        """Return row counts for all known tables."""
        all_tables = list(self.csv_files) + list(self.api_tables)
        results    = {t: self.get_row_count(t) for t in all_tables}
        _logger.info("Data verification completed: %s", results)
        return results

    # ------------------------------------------------------------------
    # Public batch API (thin wrappers that temporarily override batch_size)
    # ------------------------------------------------------------------

    def _with_batch_size(self, size: Optional[int]):
        """Context manager that temporarily overrides the batch processor's batch_size."""
        @contextmanager
        def _cm():
            if size is None:
                yield
                return
            original = self.batch_processor.batch_size
            self.batch_processor.batch_size = size
            try:
                yield
            finally:
                self.batch_processor.batch_size = original
        return _cm()

    def batch_insert(self, table_name: str, data: List[Dict],
                    batch_size: int = None, ignore_duplicates: bool = False,
                    progress_callback: callable = None) -> Tuple[int, int]:
        """High-performance batch insert with optional batch-size override."""
        with self._with_batch_size(batch_size):
            return self.batch_processor.insert_batch(
                table_name=table_name, data=data,
                progress_callback=progress_callback,
                ignore_duplicates=ignore_duplicates,
            )

    def batch_update(self, table_name: str, updates: List[Dict],
                    key_columns: List[str], batch_size: int = None,
                    progress_callback: callable = None) -> Tuple[int, int]:
        """High-performance batch update with optional batch-size override."""
        with self._with_batch_size(batch_size):
            return self.batch_processor.update_batch(
                table_name=table_name, updates=updates,
                key_columns=key_columns, progress_callback=progress_callback,
            )

    def batch_upsert(self, table_name: str, data: List[Dict],
                    key_columns: List[str], batch_size: int = None,
                    progress_callback: callable = None) -> Tuple[int, int, int]:
        """High-performance batch upsert with optional batch-size override."""
        with self._with_batch_size(batch_size):
            return self.batch_processor.upsert_batch(
                table_name=table_name, data=data,
                key_columns=key_columns, progress_callback=progress_callback,
            )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close_connections(self):
        """Return connections to the pool. The pool itself manages its own lifecycle."""
        pass

    def __del__(self):
        pass


# Legacy compatibility
def create_api_tables_and_csv():
    """Legacy compatibility function."""
    return DatabaseManager().export_api_data_to_csv()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="ETL Database Manager")
    parser.add_argument("--create-tables", action="store_true", help="Create database tables")
    parser.add_argument("--import-csv",    action="store_true", help="Import CSV data")
    parser.add_argument("--verify",        action="store_true", help="Verify data")
    parser.add_argument("--pool-size",     type=int, default=5,    help="Connection pool size")
    parser.add_argument("--batch-size",    type=int, default=1000, help="Batch processing size")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    try:
        db_manager = DatabaseManager(enable_pooling=True, pool_size=args.pool_size)
        db_manager.batch_processor.batch_size = args.batch_size

        if not db_manager.test_connection():
            _logger.error("Failed to connect to database")
            sys.exit(1)
        _logger.info("Database connection successful")

        if not db_manager.create_database_if_not_exists():
            _logger.error("Failed to create database")
            sys.exit(1)

        if args.create_tables:
            _logger.info("Creating database tables...")
            if not db_manager.create_all_tables_from_csv():
                _logger.error("Failed to create tables")
                sys.exit(1)
            _logger.info("Tables created successfully")

        if args.import_csv:
            _logger.info("Importing CSV data...")
            if not db_manager.import_csv_data():
                _logger.error("Failed to import CSV data")

        if args.verify:
            _logger.info("Verifying data...")
            for table, count in db_manager.verify_data().items():
                _logger.info("Table %s: %d records", table, count)

        _logger.info("Connection statistics: %s", db_manager.get_connection_stats())

    except Exception as e:
        _logger.error("Error in database manager: %s", e)
        sys.exit(1)
    finally:
        if 'db_manager' in locals():
            db_manager.close_connections()
