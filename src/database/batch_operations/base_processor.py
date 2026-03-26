"""
Base batch processor with common functionality shared across all specialized processors.
"""

import logging
from typing import Any, Callable

try:
    from sqlalchemy import inspect as sa_inspect
    HAS_SQLALCHEMY = True
except ImportError:
    HAS_SQLALCHEMY = False

from src.database.utilities import DataUtils, OperationStats

logger = logging.getLogger(__name__)


class BaseBatchProcessor:
    """Base class for all batch processors providing shared utilities."""

    def __init__(
        self,
        connection_manager: Any,
        data_validator: Any | None = None,
        batch_size: int = 1000
    ):
        """
        Initialize base batch processor.

        Args:
            connection_manager: Database connection manager instance
            data_validator: Optional validator for record data
            batch_size: Number of records to process per batch (default: 1000)
        """
        self.connection_manager = connection_manager
        self.data_validator = data_validator
        self.batch_size = batch_size
        self.stats = OperationStats()
        self.logger = logger

    def validate_records(
        self,
        records: list[dict]
    ) -> tuple[list[dict], list[str]]:
        """
        Validate records if a validator is configured.

        Args:
            records: List of record dictionaries

        Returns:
            Tuple of (validated_records, list_of_validation_error_messages)
        """
        if self.data_validator:
            return DataUtils.validate_records(records, self.data_validator)
        return records, []

    def update_progress(
        self,
        current: int,
        total: int,
        table_name: str,
        progress_callback: Callable[[int, int, str], None] | None = None
    ) -> None:
        """
        Report progress via callback if provided. Callback failures are
        caught and logged as warnings so they never abort a batch operation.

        Args:
            current: Number of records processed so far
            total: Total number of records to process
            table_name: Name of the table being processed
            progress_callback: Optional callable(current, total, table_name)
        """
        if progress_callback:
            try:
                progress_callback(current, total, table_name)
            except Exception as e:
                self.logger.warning(f"Progress callback failed: {e}")

    def log_batch_result(
        self,
        operation: str,
        batch_num: int,
        records_affected: int
    ) -> None:
        """
        Log result of a processed batch.

        Args:
            operation: Type of operation ('insert', 'update', 'upsert', 'delete')
            batch_num: Batch sequence number
            records_affected: Number of rows affected by this batch
        """
        self.logger.debug(
            f"Batch {batch_num} {operation}: {records_affected:,} records affected"
        )

    def handle_batch_error(
        self,
        error: Exception,
        batch_size: int,
        operation: str
    ) -> int:
        """
        Handle error during batch processing and update stats.

        Args:
            error: The exception that occurred
            batch_size: Size of the failed batch
            operation: Type of operation that failed

        Returns:
            Number of records considered failed (equals batch_size)
        """
        error_msg = f"Batch {operation} failed: {error}"
        self.logger.error(error_msg)
        self.stats.add_operation(records_failed=batch_size, error=error_msg)
        return batch_size

    def get_stats(self) -> dict[str, Any]:
        """Return current operation statistics."""
        return self.stats.get_stats()

    def get_stats_summary(self) -> str:
        """Return human-readable summary of statistics."""
        return self.stats.get_summary()

    def reset_stats(self) -> None:
        """Reset all collected statistics."""
        self.stats.reset()

    def _get_connection_context(self):
        """Context manager helper — can be used in subclasses."""
        return self.connection_manager.get_connection()

    def infer_schema(self, table_name: str) -> list[str]:
        """
        Infer column names for a table from the database.

        Returns:
            List of column name strings, or empty list if unavailable.
        """
        if not HAS_SQLALCHEMY:
            self.logger.warning("SQLAlchemy is not installed — cannot infer schema.")
            return []

        try:
            inspector = sa_inspect(self.connection_manager.engine)
            columns = inspector.get_columns(table_name)
            return [col["name"] for col in columns]
        except Exception as e:
            self.logger.warning(f"Could not infer schema for {table_name}: {e}")
            return []
