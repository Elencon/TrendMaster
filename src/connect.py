"""MySQL connection module with RetryHandler-based retry mechanism and context manager."""

import logging
from contextlib import contextmanager

try:
    import mysql.connector
    MYSQL_CONNECTOR_AVAILABLE = True
except ImportError:
    MYSQL_CONNECTOR_AVAILABLE = False

try:
    import pymysql
    PYMYSQL_AVAILABLE = True
except ImportError:
    PYMYSQL_AVAILABLE = False

from dotenv import dotenv_values

from src.common.retry import RetryHandler, RetryConfig

logger = logging.getLogger(__name__)


# ────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────

def _load_config() -> dict:
    """
    Load database configuration.

    Tries the structured config module first. Falls back to .env file
    values, then to hardcoded defaults. Never mutates os.environ.
    """
    try:
        from config import get_config
        return get_config().database.to_dict()
    except ImportError:
        pass

    env = dotenv_values()

    return {
        "user":     env.get("DB_USER",     "root"),
        "password": env.get("DB_PASSWORD", ""),
        "host":     env.get("DB_HOST",     "127.0.0.1"),
        "database": env.get("DB_NAME",     "trend_master"),
        "port":     int(env.get("DB_PORT", "3306")),
    }


config = _load_config()


# ────────────────────────────────────────────────
# Driver helpers
# ────────────────────────────────────────────────

def _connect_pymysql(config: dict):
    """Connect using PyMySQL, stripping keys it does not support."""
    pymysql_config = {k: v for k, v in config.items() if k != "raise_on_warnings"}
    return pymysql.connect(**pymysql_config)


def _connect_mysql_connector(config: dict):
    """Connect using mysql-connector-python."""
    return mysql.connector.connect(**config)


def _attempt_connection(config: dict):
    """
    Make a single connection attempt using the available driver.

    Raises:
        RuntimeError: If no MySQL driver is installed (not retryable).
        Exception: Any driver-level connection error (retryable).
    """
    if not PYMYSQL_AVAILABLE and not MYSQL_CONNECTOR_AVAILABLE:
        raise RuntimeError(
            "No MySQL driver available. Install pymysql or mysql-connector-python."
        )

    if PYMYSQL_AVAILABLE:
        return _connect_pymysql(config)
    return _connect_mysql_connector(config)


# ────────────────────────────────────────────────
# Public API
# ────────────────────────────────────────────────

def _make_default_retry_handler() -> RetryHandler:
    """Create a RetryHandler that retries all connection exceptions."""
    return RetryHandler(config=RetryConfig(
        max_attempts=3,
        base_delay=2.0,
        max_delay=10.0,
        exponential=True,
        jitter=True,
        retry_on_exception=(Exception,),
    ))


async def connect_to_mysql(
    config: dict,
    retry_handler: RetryHandler,
):
    """
    Establish a connection to MySQL using the provided RetryHandler.

    All retry logic — attempt count, delays, backoff strategy — is owned
    entirely by the RetryHandler. Uses PyMySQL as the primary driver,
    falling back to mysql-connector-python.

    Args:
        config: MySQL connection parameters.
        retry_handler: RetryHandler instance that controls retry behaviour.
            Use _make_default_retry_handler() for sensible defaults.

    Returns:
        A MySQL connection object on success, or None on failure.

    Example:
        handler = RetryHandler(RetryConfig(max_attempts=5, base_delay=1.0,
                                           retry_on_exception=(Exception,)))
        conn = await connect_to_mysql(config, handler)
    """
    try:
        return await retry_handler.execute(_attempt_connection, config)
    except RuntimeError as err:
        # Driver not installed — not retryable
        logger.error("No MySQL driver available: %s", err)
        return None
    except Exception as err:
        logger.error("Failed to connect to MySQL after all retry attempts: %s", err)
        return None


@contextmanager
def mysql_connection(
    config: dict,
    retry_handler: RetryHandler,
):
    """
    Synchronous context manager for MySQL database connections.

    Acquires a connection via connect_to_mysql on entry and closes it on
    exit, even if an exception is raised inside the block. Yields None if
    connection failed — callers should guard against this.

    For async code, call connect_to_mysql directly and manage the
    connection lifecycle manually.

    Args:
        config: MySQL connection parameters.
        retry_handler: RetryHandler instance that controls retry behaviour.

    Yields:
        A MySQL connection object, or None if connection failed.

    Example:
        handler = _make_default_retry_handler()
        with mysql_connection(config, handler) as conn:
            if conn is None:
                raise RuntimeError("Could not connect to database")
            cursor = conn.cursor()
            ...
    """
    import asyncio

    try:
        conn = asyncio.get_event_loop().run_until_complete(
            connect_to_mysql(config, retry_handler)
        )
    except RuntimeError:
        conn = asyncio.run(connect_to_mysql(config, retry_handler))

    try:
        yield conn
    finally:
        if conn is not None:
            conn.close()