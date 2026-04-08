"""
Database operation statistics tracking (production-grade).

Features:
- Accurate timing using monotonic clock
- Per-operation-type and per-table metrics
- Structured error tracking
- Clean summaries (compact + detailed)
- Safe, predictable behavior
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any


logger = logging.getLogger(__name__)


# ============================================================================
# Data Models
# ============================================================================

@dataclass(slots=True)
class OperationError:
    """Structured representation of an operation error."""
    timestamp: str
    error: str
    operation_type: str | None = None
    table_name: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ============================================================================
# Main Stats Class
# ============================================================================

class OperationStats:
    """
    Track database operation statistics with robust metrics.

    Notes:
    - Uses monotonic clock for timing accuracy
    - Not internally thread-safe (wrap externally if needed)
    - Fully backward-compatible API (dict-based outputs)
    """

    def __init__(self) -> None:
        self.reset()

    # ---------------------------------------------------------------------
    # Lifecycle
    # ---------------------------------------------------------------------

    def reset(self) -> None:
        """Reset all statistics to initial state."""
        self._stats: dict[str, Any] = {
            "total_operations": 0,
            "total_records_processed": 0,
            "total_records_inserted": 0,
            "total_records_updated": 0,
            "total_records_deleted": 0,
            "total_records_failed": 0,
            "total_execution_time": 0.0,
            "operations_by_type": {},
            "operations_by_table": {},
            "errors": [],  # list[OperationError]
            "start_time": None,
            "last_operation_time": None,
        }

    # ---------------------------------------------------------------------
    # Timing
    # ---------------------------------------------------------------------

    def start_operation(self) -> float:
        """
        Mark the start of an operation.

        Returns:
            Monotonic timestamp to pass to `end_operation()`.
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
        Record operation completion and update timing stats.

        Returns:
            Duration in seconds.
        """
        duration = time.monotonic() - start_time

        s = self._stats
        s["total_execution_time"] += duration
        s["last_operation_time"] = time.monotonic()

        # ---- per operation type ----
        by_type = s["operations_by_type"]
        entry = by_type.setdefault(
            operation_type,
            {"count": 0, "total_time": 0.0, "avg_time": 0.0},
        )
        entry["count"] += 1
        entry["total_time"] += duration
        entry["avg_time"] = entry["total_time"] / entry["count"]

        # ---- per table ----
        if table_name:
            by_table = s["operations_by_table"]
            table_entry = by_table.setdefault(
                table_name,
                {"operations": 0, "records_processed": 0, "total_time": 0.0},
            )
            table_entry["operations"] += 1
            table_entry["total_time"] += duration

        return duration

    # ---------------------------------------------------------------------
    # Record Counting + Errors
    # ---------------------------------------------------------------------

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
        Accumulate record statistics and optionally log an error.
        """
        s = self._stats

        s["total_operations"] += 1
        s["total_records_processed"] += records_processed
        s["total_records_inserted"] += records_inserted
        s["total_records_updated"] += records_updated
        s["total_records_deleted"] += records_deleted
        s["total_records_failed"] += records_failed

        # ---- per table records ----
        if table_name:
            by_table = s["operations_by_table"]
            table_entry = by_table.setdefault(
                table_name,
                {"operations": 0, "records_processed": 0, "total_time": 0.0},
            )
            table_entry["records_processed"] += records_processed

        # ---- error tracking ----
        if error:
            s["errors"].append(
                OperationError(
                    timestamp=datetime.now().isoformat(),
                    error=error,
                    operation_type=operation_type,
                    table_name=table_name,
                )
            )

    # ---------------------------------------------------------------------
    # Reporting
    # ---------------------------------------------------------------------

    def get_stats(self) -> dict[str, Any]:
        """
        Return a safe shallow copy of statistics.

        Errors are converted to dicts for compatibility.
        """
        s = self._stats.copy()
        s["errors"] = [
            e.to_dict() if isinstance(e, OperationError) else e
            for e in s["errors"]
        ]
        return s

    def get_summary(self) -> str:
        """Return a compact one-line summary."""
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

    def get_detailed_summary(self) -> str:
        """Return a multi-line detailed summary (ideal for logs)."""
        s = self._stats

        lines = [
            "=== Operation Statistics ===",
            f"Total Operations      : {s['total_operations']}",
            f"Records Processed     : {s['total_records_processed']:,}",
            f"Records Inserted      : {s['total_records_inserted']:,}",
            f"Records Updated       : {s['total_records_updated']:,}",
            f"Records Deleted       : {s['total_records_deleted']:,}",
            f"Records Failed        : {s['total_records_failed']:,}",
            f"Total Execution Time  : {s['total_execution_time']:.3f}s",
        ]

        # ---- per type ----
        if s["operations_by_type"]:
            lines.append("\nOperations by Type:")
            for op_type, data in s["operations_by_type"].items():
                lines.append(
                    f"  {op_type:12} : {data['count']:6,} ops | "
                    f"{data['total_time']:.3f}s | avg {data['avg_time']:.4f}s"
                )

        # ---- per table ----
        if s["operations_by_table"]:
            lines.append("\nOperations by Table:")
            for table, data in s["operations_by_table"].items():
                lines.append(
                    f"  {table:12} : {data['operations']:6,} ops | "
                    f"{data['records_processed']:6,} rows | "
                    f"{data['total_time']:.3f}s"
                )

        # ---- errors ----
        if s["errors"]:
            lines.append(f"\nTotal Errors: {len(s['errors'])}")

        return "\n".join(lines)