"""
Context managers for safe database operations and transactions.
"""

import logging
from contextlib import contextmanager
from typing import Any, Generator

logger = logging.getLogger(__name__)


@contextmanager
def safe_operation(
    operation_name: str,
    logger_instance: Any | None = None,
) -> Generator[None, None, None]:
    """
    Context manager for safe database operations with structured logging.

    Logs start and completion. Re-raises any exception after logging so
    the caller retains full control over error handling.

    Args:
        operation_name: Human-readable name for the operation.
        logger_instance: Logger to use. Defaults to the module logger.

    Yields:
        None — provides exception handling and logging only.

    Example:
        with safe_operation("insert batch", logger):
            cursor.execute(sql, data)
    """
    log = logger_instance or logger
    log.info("Starting %s", operation_name)
    try:
        yield
        log.info("Completed %s", operation_name)
    except Exception as e:
        log.error("Failed %s: %s", operation_name, e)
        raise


@contextmanager
def db_transaction(
    connection: Any,
    logger_instance: Any | None = None,
) -> Generator[Any, None, None]:
    """
    Context manager for database transactions with automatic commit/rollback.

    Commits on clean exit. Rolls back on any exception, logs both the
    original error and any rollback failure, then re-raises the original.

    Args:
        connection: Database connection object.
        logger_instance: Logger to use. Defaults to the module logger.

    Yields:
        The database connection for use inside the transaction.

    Example:
        with db_transaction(conn) as txn:
            cursor = txn.cursor()
            cursor.execute("INSERT INTO ...")
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
            log.warning("Transaction rolled back: %s", original_error)
        except Exception as rollback_error:
            log.error("Rollback failed: %s", rollback_error)
        raise


@contextmanager
def managed_cursor(
    connection: Any,
    logger_instance: Any | None = None,
) -> Generator[Any, None, None]:
    """
    Context manager for database cursors with guaranteed cleanup.

    Opens a cursor on entry and closes it on exit regardless of whether
    an exception occurred. Cursor close failures are logged as warnings
    and do not suppress the original exception.

    Args:
        connection: Database connection object.
        logger_instance: Logger to use. Defaults to the module logger.

    Yields:
        An open database cursor.

    Example:
        with managed_cursor(conn) as cursor:
            cursor.execute("SELECT * FROM table")
            results = cursor.fetchall()
    """
    log = logger_instance or logger
    cursor = None
    try:
        cursor = connection.cursor()
        yield cursor
    except Exception as e:
        log.error("Cursor operation failed: %s", e)
        raise
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception as e:
                log.warning("Error closing cursor: %s", e)
