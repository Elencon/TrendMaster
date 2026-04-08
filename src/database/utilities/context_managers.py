"""
Context managers for safe database operations and transactions.
Provides structured logging, automatic commit/rollback, and resource cleanup.
"""

from __future__ import annotations

import logging
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

logger = logging.getLogger(__name__)


@contextmanager
def safe_operation(
    operation_name: str,
    logger_instance: logging.Logger | None = None,
) -> Generator[None, None, None]:
    """
    Context manager for safe database (or any) operations with structured logging.

    Logs start and successful completion.
    On exception: logs the error and re-raises (caller retains control).
    """
    log = logger_instance or logger

    log.info("Starting %s", operation_name)

    try:
        yield
        log.info("Completed %s", operation_name)
    except Exception as e:  # noqa: BLE001 - we want to log and re-raise
        log.error("Failed %s: %s", operation_name, e, exc_info=True)
        raise


@contextmanager
def db_transaction(
    connection: Any,
    logger_instance: logging.Logger | None = None,
) -> Generator[Any, None, None]:
    """
    Context manager for database transactions with automatic commit/rollback.

    - Commits if the block exits cleanly.
    - Rolls back on any exception.
    - Logs rollback failures as errors but does **not** suppress the original exception.
    """
    log = logger_instance or logger

    log.debug("Starting database transaction")

    try:
        yield connection
        connection.commit()
        log.debug("Transaction committed successfully")
    except Exception as original_error:
        try:
            connection.rollback()
            log.warning("Transaction rolled back due to: %s", original_error)
        except Exception as rollback_error:  # noqa: BLE001
            log.error(
                "Rollback failed after error %s. Rollback error: %s",
                original_error,
                rollback_error,
                exc_info=True,
            )
        raise  # Re-raise the original error


@contextmanager
def managed_cursor(
    connection: Any,
    logger_instance: logging.Logger | None = None,
) -> Generator[Any, None, None]:
    """
    Context manager for database cursors with guaranteed cleanup.

    Creates a cursor on entry and ensures it is closed on exit,
    even if an exception occurs. Cursor close errors are logged as warnings.
    """
    log = logger_instance or logger
    cursor: Any = None

    try:
        cursor = connection.cursor()
        yield cursor
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception as e:  # noqa: BLE001
                log.warning("Failed to close cursor: %s", e, exc_info=False)