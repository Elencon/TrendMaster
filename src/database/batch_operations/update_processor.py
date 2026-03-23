"""
Specialized processor for batch update operations.
"""

import logging
from typing import Any, Callable

from .base_processor import BaseBatchProcessor
from ..utilities import DatabaseUtils, safe_operation

logger = logging.getLogger(__name__)


class UpdateProcessor(BaseBatchProcessor):
    """Handle batch update operations with specialized functionality."""

    def update_batch(
        self,
        table_name: str,
        records: list[dict],
        key_columns: list[str],
        progress_callback: Callable | None = None
    ) -> tuple[int, int]:
        """
        Update records in batches using key columns for WHERE clause matching.

        Each batch is processed within a single transaction. If any batch
        fails, that batch is rolled back and counted as failed — subsequent
        batches continue. The overall transaction is committed only if all
        batches succeed.

        Args:
            table_name: Target table name
            records: List of record dictionaries (must contain key_columns + update columns)
            key_columns: Columns used in the WHERE clause to identify rows
            progress_callback: Optional callable(current, total, table_name)

        Returns:
            Tuple of (updated_count, failed_count)
        """
        if not records:
            return 0, 0

        if not key_columns:
            raise ValueError("key_columns must be specified for update operations")

        sample = records[0]
        missing_keys = [col for col in key_columns if col not in sample]
        if missing_keys:
            raise ValueError(f"Key columns missing in records: {missing_keys}")

        update_columns = [col for col in sample if col not in key_columns]
        if not update_columns:
            raise ValueError("No columns available for update (all columns are key columns)")

        total_updated = 0
        total_failed = 0
        any_batch_failed = False

        with safe_operation(f"batch update to {table_name}", self.logger):
            with self.connection_manager.get_connection() as conn:
                if not conn:
                    self.stats.add_operation(
                        records_failed=len(records),
                        error="No database connection"
                    )
                    return 0, len(records)

                cursor = conn.cursor()
                original_autocommit = conn.autocommit
                try:
                    conn.autocommit = False

                    # Generate SQL once — update_columns then key_columns in WHERE
                    sql = DatabaseUtils.generate_update_sql(
                        table_name, update_columns, key_columns
                    )

                    # Column order must match SQL: update values first, then key values
                    all_columns = update_columns + key_columns

                    for start in range(0, len(records), self.batch_size):
                        batch = records[start:start + self.batch_size]
                        batch_num = (start // self.batch_size) + 1

                        try:
                            data_tuples = DatabaseUtils.records_to_tuples(batch, all_columns)
                            cursor.executemany(sql, data_tuples)
                            updated = max(0, cursor.rowcount)
                            total_updated += updated

                            self.update_progress(
                                start + len(batch), len(records),
                                table_name, progress_callback
                            )
                            self.log_batch_result("update", batch_num, updated)

                        except Exception as e:
                            conn.rollback()
                            any_batch_failed = True
                            total_failed += len(batch)
                            error_msg = (
                                f"Update batch {batch_num} failed "
                                f"({len(batch)} records): {e}"
                            )
                            logger.error(error_msg)
                            self.stats.add_operation(
                                records_failed=len(batch),
                                error=error_msg
                            )

                    if any_batch_failed:
                        logger.warning(
                            f"One or more batches failed for {table_name}. "
                            f"Successful batches have been committed."
                        )
                    else:
                        conn.commit()

                except Exception as outer_e:
                    logger.error(f"Update transaction failed: {outer_e}")
                    conn.rollback()
                    total_failed = len(records) - total_updated
                    raise

                finally:
                    conn.autocommit = original_autocommit
                    cursor.close()

        self.stats.add_operation(
            records_processed=len(records),
            records_updated=total_updated,
            records_failed=total_failed
        )

        logger.info(
            f"Batch update complete: {total_updated:,} updated, {total_failed:,} failed"
        )
        return total_updated, total_failed
