r"""
C:\Economy\Invest\TrendMaster\src\database\batch_operations\base_processor.py
Base batch processor with common functionality shared across all specialized processors.
"""

import logging
from typing import Any, Callable
from src.database.utilities import DataUtils, OperationStats

_logger = logging.getLogger(__name__)


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
        self._connection_manager = connection_manager
        self._data_validator = data_validator
        self._batch_size = batch_size

        # Internal stats and logger
        self._stats = OperationStats()
        self._logger = _logger

    # -----------------------------
    # Public Properties
    # -----------------------------

    @property
    def batch_size(self) -> int:
        """Get current batch size."""
        return self._batch_size

    @batch_size.setter
    def batch_size(self, value: int) -> None:
        """Set batch size."""
        self._batch_size = value

    @property
    def data_validator(self) -> Any | None:
        """Get data validator."""
        return self._data_validator

    @property
    def stats(self) -> OperationStats:
        """Get statistics object."""
        return self._stats

    @property
    def logger(self) -> logging.Logger:
        """Get logger instance."""
        return self._logger

    # -----------------------------
    # Public API
    # -----------------------------

    def validate_records(
        self,
        records: list[dict]
    ) -> tuple[list[dict], list[str]]:
        """
        Validate records if a validator is configured.
        """
        if self._data_validator:
            return DataUtils.validate_records(records, self._data_validator)
        return records, []

    def update_progress(
        self,
        current: int,
        total: int,
        table_name: str,
        progress_callback: Callable[[int, int, str], None] | None = None
    ) -> None:
        """
        Report progress via callback if provided.
        """
        if progress_callback:
            try:
                progress_callback(current, total, table_name)
            except Exception as e:
                self._logger.warning(f"Progress callback failed: {e}")

    def log_batch_result(
        self,
        operation: str,
        batch_num: int,
        records_affected: int
    ) -> None:
        """Log result of a processed batch."""
        self._logger.debug(
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
        """
        error_msg = f"Batch {operation} failed: {error}"
        self._logger.error(error_msg)
        self._stats.add_operation(records_failed=batch_size, error=error_msg)
        return batch_size

    def get_stats(self) -> dict[str, Any]:
        """Return current operation statistics."""
        return self._stats.get_stats()

    def get_stats_summary(self) -> str:
        """Return human-readable summary of statistics."""
        return self._stats.get_summary()

    def reset_stats(self) -> None:
        """Reset all collected statistics."""
        self._stats.reset()

    def infer_schema(self, table_name: str) -> list[str]:
        return self._connection_manager.get_schema(table_name)

    # -----------------------------
    # Internal helpers
    # -----------------------------

    def _get_connection_context(self):
        """Internal: context manager helper — can be used in subclasses."""
        return self._connection_manager.get_connection()
