"""
Specialized processor for batch insert operations.
"""

import logging
from typing import Any, Callable

from .base_processor import BaseBatchProcessor
from ..utilities import DatabaseUtils, safe_operation

logger = logging.getLogger(__name__)


class InsertProcessor(BaseBatchProcessor):
    """Handle batch insert operations with specialized functionality."""

    def insert_batch(
        self,
        table_name: str,
        records: list[dict],
        progress_callback: Callable | None = None,
        ignore_duplicates: bool = True,
        validate_data: bool = True
    ) -> tuple[int, int]:
        """
        Insert records in batches with explicit transaction control.

        Each batch is executed within a single transaction. If any batch
        fails, that batch is rolled back and counted as failed — subsequent
        batches continue. The overall transaction is committed only if all
        batches succeed; otherwise a warning is logged.

        Args:
            table_name: Target table name
            records: List of record dictionaries to insert
            progress_callback: Optional callable(current, total, table_name)
            ignore_duplicates: If True, uses INSERT IGNORE or equivalent
            validate_data: If True and a validator is configured, validates records first

        Returns:
            Tuple of (inserted_count, failed_count)
        """
        if not records:
            return 0, 0

        if validate_data and self.data_validator:
            records, validation_errors = self.validate_records(records)
            if validation_errors:
                logger.warning(
                    f"Validation warnings during insert: {len(validation_errors)} issues"
                )

        total_inserted = 0
        total_failed = 0
        any_batch_failed = False

        with safe_operation(f"batch insert into {table_name}", self.logger):
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

                    # Generate SQL once — all records share the same schema
                    sql = DatabaseUtils.generate_insert_sql(
                        table_name,
                        records[0],
                        ignore_duplicates=ignore_duplicates
                    )
                    columns = list(records[0].keys())

                    for start in range(0, len(records), self.batch_size):
                        batch = records[start:start + self.batch_size]
                        batch_num = (start // self.batch_size) + 1

                        try:
                            data_tuples = DatabaseUtils.records_to_tuples(batch, columns)
                            cursor.executemany(sql, data_tuples)
                            inserted = max(0, cursor.rowcount)
                            total_inserted += inserted

                            self.update_progress(
                                start + len(batch), len(records),
                                table_name, progress_callback
                            )
                            self.log_batch_result("insert", batch_num, inserted)

                        except Exception as e:
                            conn.rollback()
                            any_batch_failed = True
                            total_failed += len(batch)
                            error_msg = (
                                f"Insert batch {batch_num} failed "
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
                    logger.error(f"Insert transaction failed: {outer_e}")
                    conn.rollback()
                    total_failed = len(records) - total_inserted

                finally:
                    conn.autocommit = original_autocommit
                    cursor.close()

        self.stats.add_operation(
            records_processed=len(records),
            records_inserted=total_inserted,
            records_failed=total_failed
        )

        logger.info(
            f"Batch insert complete: {total_inserted:,} inserted, {total_failed:,} failed"
        )
        return total_inserted, total_failed
