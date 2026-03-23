"""
Database operation statistics tracking.
"""

import logging
import time
from datetime import datetime

logger = logging.getLogger(__name__)


class OperationStats:
    """Track database operation statistics with core metrics."""

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        """Reset all statistics to their initial state."""
        self._stats: dict = {
            "total_operations":        0,
            "total_records_processed": 0,
            "total_records_inserted":  0,
            "total_records_updated":   0,
            "total_records_deleted":   0,
            "total_records_failed":    0,
            "total_execution_time":    0.0,
            "operations_by_type":      {},
            "operations_by_table":     {},
            "errors":                  [],
            "start_time":              None,
            "last_operation_time":     None,
        }

    # -------------------------------------------------------------------------
    # Timing
    # -------------------------------------------------------------------------

    def start_operation(self) -> float:
        """
        Record the start of an operation.

        Returns:
            Monotonic start timestamp (from time.monotonic()).
        """
        ts = time.monotonic()
        if self._stats["start_time"] is None:
            self._stats["start_time"] = ts
        return ts

    def end_operation(
        self,
        start_time: float,
        operation_type: str = "unknown",
        table_name: str | None = None,
    ) -> float:
        """
        Record the end of an operation and accumulate timing stats.

        Args:
            start_time: Value returned by start_operation().
            operation_type: Label for the operation (e.g. 'insert').
            table_name: Optional table name for per-table tracking.

        Returns:
            Operation duration in seconds.
        """
        duration = time.monotonic() - start_time
        self._stats["total_execution_time"] += duration
        self._stats["last_operation_time"] = time.monotonic()

        # Per-type tracking
        by_type = self._stats["operations_by_type"]
        if operation_type not in by_type:
            by_type[operation_type] = {"count": 0, "total_time": 0.0, "avg_time": 0.0}
        entry = by_type[operation_type]
        entry["count"] += 1
        entry["total_time"] += duration
        entry["avg_time"] = entry["total_time"] / entry["count"]

        # Per-table tracking
        if table_name:
            by_table = self._stats["operations_by_table"]
            if table_name not in by_table:
                by_table[table_name] = {
                    "operations": 0,
                    "records_processed": 0,
                    "total_time": 0.0,
                }
            by_table[table_name]["operations"] += 1
            by_table[table_name]["total_time"] += duration

        return duration

    # -------------------------------------------------------------------------
    # Record counting
    # -------------------------------------------------------------------------

    def add_operation(
        self,
        records_processed: int = 0,
        records_inserted: int = 0,
        records_updated: int = 0,
        records_deleted: int = 0,
        records_failed: int = 0,
        error: str | None = None,
        operation_type: str | None = None,
        table_name: str | None = None,
    ) -> None:
        """
        Accumulate record counts and optionally log an error entry.

        Args:
            records_processed: Total records attempted.
            records_inserted: Records successfully inserted.
            records_updated: Records successfully updated.
            records_deleted: Records successfully deleted.
            records_failed: Records that failed.
            error: Optional error message to record.
            operation_type: Label for the operation type.
            table_name: Optional table name for per-table tracking.
        """
        s = self._stats
        s["total_operations"]        += 1
        s["total_records_processed"] += records_processed
        s["total_records_inserted"]  += records_inserted
        s["total_records_updated"]   += records_updated
        s["total_records_deleted"]   += records_deleted
        s["total_records_failed"]    += records_failed

        if table_name:
            by_table = s["operations_by_table"]
            if table_name not in by_table:
                by_table[table_name] = {
                    "operations": 0,
                    "records_processed": 0,
                    "total_time": 0.0,
                }
            by_table[table_name]["records_processed"] += records_processed

        if error:
            s["errors"].append({
                "timestamp":      datetime.now().isoformat(),
                "error":          error,
                "operation_type": operation_type,
                "table_name":     table_name,
            })

    # -------------------------------------------------------------------------
    # Reporting
    # -------------------------------------------------------------------------

    def get_stats(self) -> dict:
        """Return a shallow copy of the current statistics dictionary."""
        return self._stats.copy()

    def get_summary(self) -> str:
        """
        Return a compact one-line human-readable summary of statistics.

        Example:
            Operations: 5 | Processed: 1,000 | Inserted: 950 | Updated: 30 |
            Deleted: 10 | Failed: 10 | Time: 1.23s
        """
        s = self._stats
        parts = [
            f"Operations: {s['total_operations']}",
            f"Processed: {s['total_records_processed']:,}",
            f"Inserted: {s['total_records_inserted']:,}",
            f"Updated: {s['total_records_updated']:,}",
            f"Deleted: {s['total_records_deleted']:,}",
            f"Failed: {s['total_records_failed']:,}",
            f"Time: {s['total_execution_time']:.2f}s",
        ]
        if s["errors"]:
            parts.append(f"Errors: {len(s['errors'])}")
        return " | ".join(parts)