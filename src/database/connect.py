r"""
C:\Economy\Invest\TrendMaster\src\database\connect.py
Production MySQL connection helpers with retry, transactions,
sync/async support and DictCursor convenience.

Usage:
    from src.database.connect import mysql_cursor_sync

    with mysql_cursor_sync() as cur:
        cur.execute("SELECT 1")
        print(cur.fetchone())

        Run from project root:
        python -m src.database.connect
"""

import asyncio
import logging
from contextlib import contextmanager, asynccontextmanager
from typing import Any, Dict, Optional
from copy import deepcopy
import pymysql
import pymysql.cursors

from src.config import get_config
from src.common.retry import RetryHandler, RetryConfig

_logger = logging.getLogger(__name__)

# ────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────

_DB_CFG = get_config().database
DEFAULT_CONFIG_DICT = deepcopy(_DB_CFG.to_dict())

# Aliases for backward compatibility
config = _DB_CFG
logger = _logger

def _default_retry() -> RetryHandler:
    return RetryHandler(
        RetryConfig(
            max_attempts=_DB_CFG.max_retry_attempts,
            base_delay=_DB_CFG.retry_delay,
            max_delay=10,
            exponential=True,
            jitter=True,
            retry_on_exception=(Exception,),
        )
    )

# ────────────────────────────────────────────────
# Connection creation
# ────────────────────────────────────────────────

def _connect(cfg: Dict[str, Any], **kwargs):
    """
    Create a MySQL connection using PyMySQL.
    **kwargs captures internal flags like run_sync_in_thread passed by RetryHandler.
    """
    params = cfg.copy()

    core_keys = {"host", "user", "password", "database", "port", "autocommit"}
    connection_args = {k: params[k] for k in core_keys if k in params}

    # Defaults
    connection_args["connect_timeout"] = params.get("connect_timeout", 30)

    return _connect_pymysql(params, connection_args)


def _connect_pymysql(params: Dict[str, Any], connection_args: Dict[str, Any]):
    """Connect using PyMySQL."""
    _logger.debug(
        "Connecting via PyMySQL: user=%s host=%s db=%s",
        params.get("user"),
        params.get("host"),
        params.get("database"),
    )

    # Simplified extras
    extras = {
        "charset": params.get("charset", "utf8mb4"),
        "cursorclass": params.get(
            "cursorclass",
            pymysql.cursors.DictCursor,
        ),
    }

    if params.get("init_command"):
        extras["init_command"] = params["init_command"]

    if "ssl" in params:
        extras["ssl"] = params["ssl"]

    if "unix_socket" in params:
        extras["unix_socket"] = params["unix_socket"]

    return pymysql.connect(
        **connection_args,
        **extras,
    )

# ────────────────────────────────────────────────
# Cursor creation
# ────────────────────────────────────────────────

def _create_dict_cursor(conn: Any):
    """Return a DictCursor for PyMySQL connection."""
    return conn.cursor()

# ────────────────────────────────────────────────
# Sync / Async connect wrappers
# ────────────────────────────────────────────────

def connect_sync(
    config: Optional[Dict[str, Any]] = None,
    retry: Optional[RetryHandler] = None,
) -> Any:
    """Synchronous database connection with retry support."""
    cfg = deepcopy(config or DEFAULT_CONFIG_DICT)
    handler = retry or _default_retry()

    return handler.execute_sync(_connect, cfg)


async def connect_async(
    config: Optional[Dict[str, Any]] = None,
    retry: Optional[RetryHandler] = None,
) -> Any:
    """Asynchronous database connection with retry support."""
    cfg = deepcopy(config or DEFAULT_CONFIG_DICT)
    handler = retry or _default_retry()

    return await handler.execute_async(
        _connect,
        cfg,
        run_sync_in_thread=True,
    )

# Alias for backward compatibility
connect_to_mysql = connect_sync

# ────────────────────────────────────────────────
# Context managers
# ────────────────────────────────────────────────

@contextmanager
def mysql_connection(config: Optional[Dict[str, Any]] = None,
                     retry: Optional[RetryHandler] = None,
                     autocommit: bool = False):
    conn = None
    cfg = deepcopy(config or DEFAULT_CONFIG_DICT)

    if autocommit:
        cfg["autocommit"] = True

    try:
        conn = connect_sync(cfg, retry)

        if hasattr(conn, "ping"):
            try:
                conn.ping(reconnect=True)
            except Exception:
                pass

        yield conn

        if not cfg.get("autocommit") and conn:
            conn.commit()

    except Exception:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        raise

    finally:
        if conn:
            try:
                conn.close()
            except Exception as err:
                _logger.warning("Error closing connection: %s", err)


@contextmanager
def mysql_cursor_sync(config: Optional[Dict[str, Any]] = None,
                      retry: Optional[RetryHandler] = None,
                      autocommit: bool = False):
    with mysql_connection(config, retry, autocommit) as conn:
        cur = _create_dict_cursor(conn)
        try:
            yield cur
        finally:
            try:
                cur.close()
            except Exception:
                pass


@asynccontextmanager
async def mysql_cursor_async(config: Optional[Dict[str, Any]] = None,
                             retry: Optional[RetryHandler] = None,
                             autocommit: bool = False):
    conn = None
    cur = None
    cfg = deepcopy(config or DEFAULT_CONFIG_DICT)

    if autocommit:
        cfg["autocommit"] = True

    try:
        conn = await connect_async(cfg, retry)

        if hasattr(conn, "ping"):
            await asyncio.to_thread(conn.ping, reconnect=True)

        cur = _create_dict_cursor(conn)
        yield cur

        if not cfg.get("autocommit") and conn:
            await asyncio.to_thread(conn.commit)

    except Exception:
        if conn:
            try:
                await asyncio.to_thread(conn.rollback)
            except Exception:
                pass
        raise

    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass

        if conn:
            try:
                await asyncio.to_thread(conn.close)
            except Exception as err:
                _logger.warning("Error closing async connection: %s", err)
