"""
Specialized processor for batch upsert operations.
"""

import logging
from typing import Any, Callable

from .base_processor import BaseBatchProcessor
from ..utilities import DatabaseUtils, safe_operation

logger = logging.getLogger(__name__)


class UpsertProcessor(BaseBatchProcessor):
    """Handle batch upsert operations with specialized functionality."""

    def upsert_batch(
        self,
        table_name: str,
        records: list[dict],
        key_columns: list[str],
        progress_callback: Callable | None = None
    ) -> tuple[int, int, int]:
        """
        Upsert records in batches with explicit transaction control.

        Uses INSERT ... ON DUPLICATE KEY UPDATE or equivalent. Because the
        database does not distinguish inserts from updates in rowcount
        (rowcount=1 for insert, rowcount=2 for update on most MySQL configs),
        inserted and updated counts are approximated. The total affected count
        is always accurate.

        Args:
            table_name: Target table name
            records: List of record dictionaries to upsert
            key_columns: Columns used to detect duplicates
            progress_callback: Optional callable(current, total, table_name)

        Returns:
            Tuple of (estimated_inserted, estimated_updated, failed_count)
        """
        if not records:
            return 0, 0, 0

        if not key_columns:
            raise ValueError("key_columns required for upsert")

        sample = records[0]
        missing_keys = [k for k in key_columns if k not in sample]
        if missing_keys:
            raise ValueError(f"Key columns missing in records: {missing_keys}")

        total_inserted_est = 0
        total_updated_est = 0
        total_failed = 0
        total_affected = 0

        with safe_operation(f"batch upsert to {table_name}", self.logger):
            with self.connection_manager.get_connection() as conn:
                if not conn:
                    self.stats.add_operation(
                        records_failed=len(records),
                        error="No database connection"
                    )
                    return 0, 0, len(records)

                cursor = conn.cursor()
                original_autocommit = conn.autocommit
                try:
                    conn.autocommit = False

                    columns = list(sample.keys())
                    sql = DatabaseUtils.generate_upsert_sql(table_name, columns, key_columns)

                    for start in range(0, len(records), self.batch_size):
                        batch = records[start:start + self.batch_size]
                        batch_num = (start // self.batch_size) + 1

                        try:
                            data_tuples = DatabaseUtils.records_to_tuples(batch, columns)
                            cursor.executemany(sql, data_tuples)
                            affected = max(0, cursor.rowcount)
                            total_affected += affected

                            # Approximate insert/update split:
                            # rowcount=1 means inserted, rowcount=2 means updated.
                            # With executemany the total rowcount reflects this encoding,
                            # so we use integer division as a best-effort estimate.
                            total_inserted_est += (affected + 1) // 2
                            total_updated_est += affected // 2

                            self.update_progress(
                                start + len(batch), len(records),
                                table_name, progress_callback
                            )
                            self.log_batch_result("upsert", batch_num, affected)

                        except Exception as e:
                            conn.rollback()
                            total_failed += len(batch)
                            error_msg = (
                                f"Upsert batch {batch_num} failed "
                                f"({len(batch)} records): {e}"
                            )
                            logger.error(error_msg)
                            self.stats.add_operation(
                                records_failed=len(batch),
                                error=error_msg
                            )

                    conn.commit()

                except Exception as outer_e:
                    logger.error(f"Upsert transaction failed: {outer_e}")
                    conn.rollback()
                    total_failed = len(records)
                    raise

                finally:
                    conn.autocommit = original_autocommit
                    cursor.close()

        self.stats.add_operation(
            records_processed=len(records),
            records_inserted=total_inserted_est,
            records_updated=total_updated_est,
            records_failed=total_failed
        )

        logger.info(
            f"Batch upsert complete: ~{total_inserted_est:,} inserted, "
            f"~{total_updated_est:,} updated, "
            f"{total_affected:,} affected, "
            f"{total_failed:,} failed"
        )
        return total_inserted_est, total_updated_est, total_failed
