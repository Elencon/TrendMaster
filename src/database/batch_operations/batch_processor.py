"""
Main batch processor that coordinates all specialized batch operations.
Provides a unified interface while delegating to specialized processors.
"""

import logging
from typing import Any, Callable

from .base_processor import BaseBatchProcessor
from .insert_processor import InsertProcessor
from .update_processor import UpdateProcessor
from .upsert_processor import UpsertProcessor
from .delete_processor import DeleteProcessor
from ..utilities import DataUtils

try:
    import pyarrow as pa
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

logger = logging.getLogger(__name__)


class BatchProcessor(BaseBatchProcessor):
    """
    Main coordinator for batch database operations.

    Delegates specific operations (insert, update, upsert, delete) to specialized
    processor classes while providing a unified interface and aggregated statistics.

    Note: Individual operations use explicit transactions within their processors.
    When using multiple operations in sequence, the caller may want to manage
    a higher-level transaction if atomicity across different operation types is required.
    """

    def __init__(
        self,
        connection_manager: Any,
        data_validator: Any | None = None,
        batch_size: int = 1000
    ):
        """Initialize the main batch processor and all sub-processors."""
        super().__init__(connection_manager, data_validator, batch_size)

        self.insert_processor = InsertProcessor(connection_manager, data_validator, batch_size)
        self.update_processor = UpdateProcessor(connection_manager, data_validator, batch_size)
        self.upsert_processor = UpsertProcessor(connection_manager, data_validator, batch_size)
        self.delete_processor = DeleteProcessor(connection_manager, data_validator, batch_size)

        self._processors = [
            self.insert_processor,
            self.update_processor,
            self.upsert_processor,
            self.delete_processor,
        ]

    # -------------------------------------------------------------------------
    # Internal normalization
    # -------------------------------------------------------------------------

    def _normalize_records(
        self,
        records: list[dict] | Any,
        table_schema: list[str] | None = None
    ) -> list[dict]:
        """
        Normalize input into list[dict].

        Handles list[dict] directly. Converts pyarrow Table or RecordBatch
        via DataUtils if pyarrow is available. Falls back to list() for other
        iterables. Raises TypeError for unsupported types.
        """
        if isinstance(records, list):
            return records

        if HAS_PYARROW:
            if isinstance(records, pa.Table):
                return DataUtils.arrow_to_records(records, table_schema)
            if isinstance(records, pa.RecordBatch):
                return DataUtils.arrow_to_records(
                    pa.Table.from_batches([records]), table_schema
                )

        try:
            return list(records)
        except Exception:
            raise TypeError(f"Unsupported record type: {type(records)}")

    # -------------------------------------------------------------------------
    # INSERT
    # -------------------------------------------------------------------------

    def insert_batch(
        self,
        table_name: str,
        records: Any,
        progress_callback: Callable | None = None,
        ignore_duplicates: bool = True,
        validate_data: bool = True
    ) -> tuple[int, int]:
        """Insert records in batches. Delegates to InsertProcessor."""
        schema = self.infer_schema(table_name)
        normalized = self._normalize_records(records, schema)
        return self.insert_processor.insert_batch(
            table_name, normalized, progress_callback, ignore_duplicates, validate_data
        )

    # -------------------------------------------------------------------------
    # UPDATE
    # -------------------------------------------------------------------------

    def update_batch(
        self,
        table_name: str,
        records: Any,
        key_columns: list[str],
        progress_callback: Callable | None = None
    ) -> tuple[int, int]:
        """Update records in batches. Delegates to UpdateProcessor."""
        schema = self.infer_schema(table_name)
        normalized = self._normalize_records(records, schema)
        return self.update_processor.update_batch(
            table_name, normalized, key_columns, progress_callback
        )

    # -------------------------------------------------------------------------
    # UPSERT
    # -------------------------------------------------------------------------

    def upsert_batch(
        self,
        table_name: str,
        records: Any,
        key_columns: list[str],
        progress_callback: Callable | None = None
    ) -> tuple[int, int, int]:
        """Upsert records in batches. Delegates to UpsertProcessor."""
        schema = self.infer_schema(table_name)
        normalized = self._normalize_records(records, schema)
        return self.upsert_processor.upsert_batch(
            table_name, normalized, key_columns, progress_callback
        )

    # -------------------------------------------------------------------------
    # DELETE
    # -------------------------------------------------------------------------

    def delete_batch(
        self,
        table_name: str,
        conditions: Any,
        progress_callback: Callable | None = None
    ) -> tuple[int, int]:
        """Delete records in batches. Delegates to DeleteProcessor."""
        schema = self.infer_schema(table_name)
        normalized = self._normalize_records(conditions, schema)
        return self.delete_processor.delete_batch(
            table_name, normalized, progress_callback
        )

    # -------------------------------------------------------------------------
    # Statistics
    # -------------------------------------------------------------------------

    def get_stats(self) -> dict[str, Any]:
        """Get aggregated statistics from all processors."""
        combined: dict[str, Any] = {
            'total_operations': 0,
            'total_records_processed': 0,
            'total_records_inserted': 0,
            'total_records_updated': 0,
            'total_records_deleted': 0,
            'total_records_failed': 0,
            'processor_stats': {},
        }

        for processor in self._processors:
            name = processor.__class__.__name__
            stats = processor.get_stats()
            combined['processor_stats'][name] = stats
            combined['total_operations'] += stats.get('total_operations', 0)
            combined['total_records_processed'] += stats.get('records_processed', 0)
            combined['total_records_inserted'] += stats.get('records_inserted', 0)
            combined['total_records_updated'] += stats.get('records_updated', 0)
            combined['total_records_deleted'] += stats.get('records_deleted', 0)
            combined['total_records_failed'] += stats.get('records_failed', 0)

        return combined

    def get_stats_summary(self) -> str:
        """Get human-readable statistics summary."""
        stats = self.get_stats()

        lines = [
            "=== Batch Processor Statistics ===",
            f"Total Operations:      {stats['total_operations']}",
            f"Records Processed:     {stats['total_records_processed']:,}",
            f"Records Inserted:      {stats['total_records_inserted']:,}",
            f"Records Updated:       {stats['total_records_updated']:,}",
            f"Records Deleted:       {stats['total_records_deleted']:,}",
            f"Records Failed:        {stats['total_records_failed']:,}",
            "",
            "=== Per-Processor Breakdown ===",
        ]

        for name, s in stats['processor_stats'].items():
            if s.get('records_processed', 0) > 0:
                lines.extend([
                    f"{name}:",
                    f"  Processed: {s.get('records_processed', 0):,}",
                    f"  Inserted:  {s.get('records_inserted', 0):,}",
                    f"  Updated:   {s.get('records_updated', 0):,}",
                    f"  Deleted:   {s.get('records_deleted', 0):,}",
                    f"  Failed:    {s.get('records_failed', 0):,}",
                    "",
                ])

        return "\n".join(lines)

    def reset_stats(self) -> None:
        """Reset statistics for all processors and self."""
        for processor in self._processors:
            processor.reset_stats()
        self.stats.reset()

    # -------------------------------------------------------------------------
    # Utilities
    # -------------------------------------------------------------------------

    def set_batch_size(self, batch_size: int) -> None:
        """Update batch size for this processor and all sub-processors."""
        self.batch_size = batch_size
        for processor in self._processors:
            processor.batch_size = batch_size

    def get_processor(self, operation_type: str) -> BaseBatchProcessor:
        """
        Get a specific sub-processor by operation type.

        Args:
            operation_type: One of 'insert', 'update', 'upsert', 'delete'

        Returns:
            The corresponding specialized processor instance

        Raises:
            ValueError: If operation_type is not recognised
        """
        processor_map = {
            'insert': self.insert_processor,
            'update': self.update_processor,
            'upsert': self.upsert_processor,
            'delete': self.delete_processor,
        }

        if operation_type not in processor_map:
            raise ValueError(
                f"Unknown operation type: {operation_type!r}. "
                f"Valid types: {list(processor_map.keys())}"
            )

        return processor_map[operation_type]

    def get_operation_summary(self) -> dict[str, int]:
        """Get high-level count of operations by type."""
        return {
            'insert_operations': self.insert_processor.get_stats().get('total_operations', 0),
            'update_operations': self.update_processor.get_stats().get('total_operations', 0),
            'upsert_operations': self.upsert_processor.get_stats().get('total_operations', 0),
            'delete_operations': self.delete_processor.get_stats().get('total_operations', 0),
        }
