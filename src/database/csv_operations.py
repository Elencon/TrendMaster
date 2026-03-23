"""
CSV import operations for database manager.
Extracted from db_manager for better modularity.
"""

import logging
import pandas as pd
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from .utilities import DataUtils, safe_operation
from .batch_operations import BatchProcessor

logger = logging.getLogger(__name__)


class CSVImporter:
    """Handle CSV file import operations."""

    _CSV_SUBDIR = "CSV"

    def __init__(
        self,
        connection_manager,
        data_dir: Path,
        table_columns: Dict[str, List[str]],
        pandas_optimizer=None,
        batch_size: int = 1000,
    ) -> None:
        self.connection_manager = connection_manager
        self.data_dir = data_dir
        self.table_columns = table_columns
        self.pandas_optimizer = pandas_optimizer
        self.batch_processor = BatchProcessor(connection_manager, batch_size=batch_size)

    # ------------------------------------------------------------------
    # Path helper
    # ------------------------------------------------------------------

    def _csv_path(self, filename: str) -> Path:
        """Resolve a CSV filename to its full path."""
        return self.data_dir / self._CSV_SUBDIR / filename

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def discover_csv_files(self, schema_definitions: Dict[str, str]) -> Dict[str, str]:
        """Discover available CSV files based on schema definitions.

        Returns:
            Mapping of table names to CSV filenames.
        """
        csv_files: Dict[str, str] = {}
        csv_dir = self.data_dir / self._CSV_SUBDIR

        if not csv_dir.exists():
            logger.warning("CSV directory not found: %s", csv_dir)
            return csv_files

        for table_name in schema_definitions:
            csv_file = f"{table_name}.csv"
            if (csv_dir / csv_file).exists():
                csv_files[table_name] = csv_file
                logger.debug("Found CSV file for %s: %s", table_name, csv_file)
            else:
                logger.debug("No CSV file found for %s", table_name)

        return csv_files

    # ------------------------------------------------------------------
    # Import
    # ------------------------------------------------------------------

    def import_all_csv_data(
        self,
        csv_files: Dict[str, str],
        import_order: Optional[List[str]] = None,
        progress_callback: Optional[Callable] = None,
    ) -> bool:
        """Import all CSV data respecting foreign-key order.

        Returns:
            True if every eligible table imported successfully.
        """
        default_order = [
            "brands", "categories", "stores", "staffs", "products",
            "stocks", "customers", "orders", "order_items",
        ]
        order = import_order or default_order

        with safe_operation("CSV import all", logger):
            total_records = 0
            successful_imports = 0
            failed_imports = 0

            for table_name in order:
                if table_name not in csv_files:
                    logger.debug("Skipping %s — no CSV file available", table_name)
                    continue

                try:
                    records_imported = self.import_csv_file(
                        table_name, csv_files[table_name], progress_callback
                    )
                    if records_imported > 0:
                        total_records += records_imported
                        successful_imports += 1
                        logger.info("✅ %s: %d records imported", table_name, records_imported)
                    else:
                        logger.warning("⚠️ %s: no records imported", table_name)
                        failed_imports += 1

                except Exception as e:
                    logger.error("❌ %s: import failed — %s", table_name, e)
                    failed_imports += 1

            logger.info(
                "CSV import complete: %d tables succeeded, %d failed, %d total records",
                successful_imports, failed_imports, total_records,
            )
            return failed_imports == 0

    def import_csv_file(
        self,
        table_name: str,
        csv_filename: str,
        progress_callback: Optional[Callable] = None,
    ) -> int:
        """Import a single CSV file into the specified table.

        Returns:
            Number of records imported.
        """
        csv_path = self._csv_path(csv_filename)

        if not csv_path.exists():
            logger.error("CSV file not found: %s", csv_path)
            return 0

        with safe_operation(f"CSV import {table_name}", logger):
            df = self._read_csv_optimized(csv_path)
            if df is None or df.empty:
                logger.warning("No data found in %s", csv_filename)
                return 0

            df = DataUtils.clean_dataframe(df)
            schema = self.table_columns.get(table_name, [])
            records = DataUtils.dataframe_to_records(df, schema)

            if not records:
                logger.warning("No valid records found in %s", csv_filename)
                return 0

            inserted, failed = self.batch_processor.insert_batch(
                table_name,
                records,
                progress_callback=progress_callback,
                ignore_duplicates=True,
            )

            if failed > 0:
                logger.warning("%s: %d records failed to import", table_name, failed)

            return inserted

    # ------------------------------------------------------------------
    # Validation & introspection
    # ------------------------------------------------------------------

    def validate_csv_file(
        self, csv_filename: str, table_schema: List[str]
    ) -> tuple[bool, List[str]]:
        """Validate CSV file columns against the expected table schema.

        Returns:
            ``(is_valid, error_messages)``
        """
        csv_path = self._csv_path(csv_filename)

        if not csv_path.exists():
            return False, [f"CSV file not found: {csv_filename}"]

        try:
            csv_columns = list(pd.read_csv(csv_path, nrows=0).columns)
            schema_set = set(table_schema)
            csv_set = set(csv_columns)

            errors: List[str] = []
            missing = sorted(schema_set - csv_set)
            extra = sorted(csv_set - schema_set)
            if missing:
                errors.append(f"Missing required columns: {missing}")
            if extra:
                errors.append(f"Unexpected columns: {extra}")

            return not errors, errors

        except Exception as e:
            return False, [f"Failed to validate CSV structure: {e}"]

    def get_csv_info(self, csv_filename: str) -> Dict[str, Any]:
        """Return metadata about a CSV file without loading it fully into memory."""
        csv_path = self._csv_path(csv_filename)

        info: Dict[str, Any] = {
            "filename": csv_filename,
            "path": str(csv_path),
            "exists": csv_path.exists(),
            "size_bytes": 0,
            "row_count": 0,
            "columns": [],
            "errors": [],
        }

        if not csv_path.exists():
            info["errors"].append("File not found")
            return info

        try:
            info["size_bytes"] = csv_path.stat().st_size

            # Header only — avoids loading the full file
            header_df = pd.read_csv(csv_path, nrows=0)
            info["columns"] = list(header_df.columns)

            # Row count without reading all data into memory
            with csv_path.open("r", encoding="utf-8", errors="replace") as fh:
                info["row_count"] = max(0, sum(1 for _ in fh) - 1)  # subtract header

        except Exception as e:
            info["errors"].append(f"Analysis failed: {e}")

        return info

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def get_import_statistics(self) -> Dict[str, Any]:
        """Return import statistics from the batch processor."""
        return self.batch_processor.get_stats()

    def reset_statistics(self) -> None:
        """Reset import statistics."""
        self.batch_processor.reset_stats()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _read_csv_optimized(self, csv_path: Path) -> Optional[pd.DataFrame]:
        """Read CSV via the pandas optimizer if available, else plain read."""
        try:
            if self.pandas_optimizer and hasattr(self.pandas_optimizer, "optimize_csv_reading"):
                return self.pandas_optimizer.optimize_csv_reading(csv_path, auto_optimize=True)
            return pd.read_csv(csv_path)
        except Exception as e:
            logger.error("Failed to read %s: %s", csv_path, e)
            return None