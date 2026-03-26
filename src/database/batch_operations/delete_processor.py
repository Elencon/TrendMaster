"""
Specialized processor for batch delete operations with bulk optimization.
"""

import logging
from collections import defaultdict
from typing import Callable

from .base_processor import BaseBatchProcessor
from src.database.utilities import DatabaseUtils, safe_operation

logger = logging.getLogger(__name__)


class DeleteProcessor(BaseBatchProcessor):
    """Handle batch delete operations — prefers bulk IN clause where possible."""

    def delete_batch(
        self,
        table_name: str,
        conditions: list[dict],
        progress_callback: Callable | None = None
    ) -> tuple[int, int]:
        """
        Delete records in batches with bulk optimization.

        For single-column conditions, uses DELETE ... WHERE col IN (...)
        with deduplication and chunking for large value sets. For
        multi-column conditions, falls back to per-batch executemany.

        Args:
            table_name: Target table name
            conditions: List of condition dicts identifying rows to delete
            progress_callback: Optional callable(current, total, table_name)

        Returns:
            Tuple of (deleted_count, failed_count)
        """
        if not conditions:
            return 0, 0

        total_deleted = 0
        total_failed = 0

        with safe_operation(f"batch delete from {table_name}", self.logger):
            with self.connection_manager.get_connection() as conn:
                if not conn:
                    self.stats.add_operation(
                        records_failed=len(conditions),
                        error="No database connection"
                    )
                    return 0, len(conditions)

                cursor = conn.cursor()
                original_autocommit = conn.autocommit
                try:
                    conn.autocommit = False

                    # Group conditions by their key pattern to detect
                    # single-column vs composite deletes
                    groups: dict[tuple, list] = defaultdict(list)
                    for cond in conditions:
                        key = tuple(sorted(cond.keys()))
                        groups[key].append(cond)

                    for key_cols, group in groups.items():
                        column_list = list(key_cols)

                        if len(column_list) == 1:
                            # Single-column delete — use IN clause for efficiency
                            col = column_list[0]
                            values = [
                                c[col] for c in group
                                if col in c and c[col] is not None
                            ]
                            # Deduplicate while preserving order
                            values = list(dict.fromkeys(values))

                            if not values:
                                continue

                            # Chunk to stay within DB placeholder limits
                            CHUNK_SIZE = 4000
                            for start in range(0, len(values), CHUNK_SIZE):
                                chunk = values[start:start + CHUNK_SIZE]
                                chunk_num = (start // CHUNK_SIZE) + 1
                                placeholders = ','.join(['%s'] * len(chunk))
                                sql = (
                                    f"DELETE FROM `{table_name}` "
                                    f"WHERE `{col}` IN ({placeholders})"
                                )
                                try:
                                    cursor.execute(sql, chunk)
                                    deleted = max(0, cursor.rowcount)
                                    total_deleted += deleted
                                    self.log_batch_result("delete", chunk_num, deleted)
                                except Exception as e:
                                    conn.rollback()
                                    total_failed += len(chunk)
                                    error_msg = (
                                        f"Delete chunk {chunk_num} failed "
                                        f"({len(chunk)} values): {e}"
                                    )
                                    logger.error(error_msg)
                                    self.stats.add_operation(
                                        records_failed=len(chunk),
                                        error=error_msg
                                    )

                        else:
                            # Composite-key delete — use executemany with WHERE clause
                            sql = DatabaseUtils.generate_delete_sql(table_name, column_list)

                            for start in range(0, len(group), self.batch_size):
                                batch = group[start:start + self.batch_size]
                                batch_num = (start // self.batch_size) + 1

                                try:
                                    data_tuples = DatabaseUtils.records_to_tuples(
                                        batch, column_list
                                    )
                                    cursor.executemany(sql, data_tuples)
                                    deleted = max(0, cursor.rowcount)
                                    total_deleted += deleted
                                    self.log_batch_result("delete", batch_num, deleted)
                                except Exception as e:
                                    conn.rollback()
                                    total_failed += len(batch)
                                    error_msg = (
                                        f"Delete batch {batch_num} failed "
                                        f"({len(batch)} records): {e}"
                                    )
                                    logger.error(error_msg)
                                    self.stats.add_operation(
                                        records_failed=len(batch),
                                        error=error_msg
                                    )

                    conn.commit()
                    self.update_progress(
                        len(conditions), len(conditions),
                        table_name, progress_callback
                    )

                except Exception as outer_e:
                    logger.error(f"Delete transaction failed: {outer_e}")
                    conn.rollback()
                    total_failed = len(conditions) - total_deleted
                    raise

                finally:
                    conn.autocommit = original_autocommit
                    cursor.close()

        self.stats.add_operation(
            records_processed=len(conditions),
            records_deleted=total_deleted,
            records_failed=total_failed
        )

        logger.info(
            f"Batch delete complete: {total_deleted:,} deleted, {total_failed:,} failed"
        )
        return total_deleted, total_failed
