r"""
C:\Economy\Invest\TrendMaster\src\connect.py
Production MySQL connection helpers with retry, transactions,
sync/async support and DictCursor convenience.

Usage:
    from src.connect import mysql_cursor_sync

    with mysql_cursor_sync() as cur:
        cur.execute("SELECT 1")
        print(cur.fetchone())
"""

import asyncio
import logging
from contextlib import contextmanager, asynccontextmanager
from datetime import datetime
from typing import Any, Dict, Optional, Generator, AsyncGenerator
from copy import deepcopy
from enum import Enum

from src.config import get_config
from src.common.retry import RetryHandler, RetryConfig

_logger = logging.getLogger(__name__)

# ────────────────────────────────────────────────
# Driver Enum
# ────────────────────────────────────────────────

class MySQLDriver(str, Enum):
    PYMYSQL = "pymysql"
    MYSQL_CONNECTOR = "mysql-connector"

# ────────────────────────────────────────────────
# Driver detection
# ────────────────────────────────────────────────

from src.database.connection_manager import (
    MYSQL_CONNECTOR_AVAILABLE,
    PYMYSQL_AVAILABLE,
    mysql,
    pymysql,
)

# ────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────

_DB_CFG = get_config().database
DEFAULT_CONFIG_DICT = deepcopy(_DB_CFG.to_dict())

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

def _connect(cfg: Dict[str, Any]):
    """
    Create a MySQL connection using either PyMySQL or mysql-connector.
    Supports explicit driver selection via cfg["driver"] = MySQLDriver.
    """
    params = cfg.copy()

    driver: Optional[MySQLDriver] = params.get("driver")

    core_keys = {"host", "user", "password", "database", "port", "autocommit"}
    connection_args = {k: params[k] for k in core_keys if k in params}
    connection_args["connect_timeout"] = params.get("connect_timeout", 30)

    # ────────────────────────────────────────────────
    # Explicit driver selection
    # ────────────────────────────────────────────────
    if driver == MySQLDriver.PYMYSQL:
        if not PYMYSQL_AVAILABLE:
            raise RuntimeError("PyMySQL selected but not installed.")
        return _connect_pymysql(params, connection_args)

    if driver == MySQLDriver.MYSQL_CONNECTOR:
        if not MYSQL_CONNECTOR_AVAILABLE:
            raise RuntimeError("mysql-connector selected but not installed.")
        return _connect_mysql_connector(params, connection_args)

    # ────────────────────────────────────────────────
    # Auto-detect driver
    # ────────────────────────────────────────────────
    if PYMYSQL_AVAILABLE:
        return _connect_pymysql(params, connection_args)

    if MYSQL_CONNECTOR_AVAILABLE:
        return _connect_mysql_connector(params, connection_args)

    raise RuntimeError("No MySQL driver installed.")


def _connect_pymysql(params: Dict[str, Any], connection_args: Dict[str, Any]):
    """Connect using PyMySQL (collation not supported)."""
    _logger.debug("Connecting via PyMySQL: %s@%s", params.get("user"), params.get("host"))

    return pymysql.connect(
        **connection_args,
        charset=params.get("charset", "utf8mb4"),
        init_command=params.get("init_command"),
        cursorclass=pymysql.cursors.DictCursor,
    )


def _connect_mysql_connector(params: Dict[str, Any], connection_args: Dict[str, Any]):
    """Connect using mysql-connector (collation not supported)."""
    _logger.debug("Connecting via mysql-connector: %s@%s", params.get("user"), params.get("host"))

    if params.get("raise_on_warnings") is not None:
        connection_args["raise_on_warnings"] = params["raise_on_warnings"]

    connection_args.update({
        "charset": params.get("charset", "utf8mb4"),
        "init_command": params.get("init_command"),
        "use_unicode": True,
    })

    return mysql.connector.connect(**connection_args)

# ────────────────────────────────────────────────
# Cursor creation
# ────────────────────────────────────────────────

def _create_dict_cursor(conn: Any):
    """Return a dict-like cursor for either driver."""
    if PYMYSQL_AVAILABLE and isinstance(conn, pymysql.Connection):
        return conn.cursor()

    if MYSQL_CONNECTOR_AVAILABLE:
        try:
            return conn.cursor(dictionary=True)
        except Exception:
            pass

    return conn.cursor()

# ────────────────────────────────────────────────
# Sync / Async connect wrappers
# ────────────────────────────────────────────────

def connect_sync(config: Optional[Dict[str, Any]] = None,
                 retry: Optional[RetryHandler] = None):
    cfg = deepcopy(config or DEFAULT_CONFIG_DICT)
    handler = retry or _default_retry()
    return handler.execute_sync(_connect, cfg)


async def connect_async(config: Optional[Dict[str, Any]] = None,
                        retry: Optional[RetryHandler] = None):
    cfg = deepcopy(config or DEFAULT_CONFIG_DICT)
    handler = retry or _default_retry()
    return await handler.execute_async(
        _connect,
        cfg,
        run_sync_in_thread=True,
    )

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

        if not cfg.get("autocommit"):
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

        if not cfg.get("autocommit"):
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


# ────────────────────────────────────────────────
# LOCAL TESTS
# ────────────────────────────────────────────────

"""
Run from project root:

python -m src.connect
"""

import asyncio
import logging
import sys
from datetime import datetime

from src.connect import (
    connect_sync,
    connect_async,
    mysql_cursor_sync,
    mysql_cursor_async,
    _default_retry,
)

from connection_manager import (
    PYMYSQL_AVAILABLE,
    MYSQL_CONNECTOR_AVAILABLE,
)


# ────────────────────────────────────────────────
# Helper
# ────────────────────────────────────────────────

def detect_driver(conn):
    """Return driver name based on connection object."""
    mod = type(conn).__module__.lower()

    if "pymysql" in mod:
        return "PyMySQL"
    if "mysql.connector" in mod:
        return "mysql-connector"

    return f"Unknown ({mod})"


# ────────────────────────────────────────────────
# Sync tests
# ────────────────────────────────────────────────

def run_sync_tests():
    print("🧪 Running Synchronous Tests...\n")

    handler = _default_retry()

    try:

        # ------------------------------------------------
        # 1. Direct connection test
        # ------------------------------------------------

        print("1. connect_sync()")

        conn = connect_sync(retry=handler)
        driver = detect_driver(conn)

        assert conn is not None
        print(f"   ✅ Connection OK (driver: {driver})")

        conn.close()

        # ------------------------------------------------
        # 2. Cursor test
        # ------------------------------------------------

        print("\n2. mysql_cursor_sync() query")

        with mysql_cursor_sync(retry=handler) as cur:
            cur.execute("SELECT VERSION() AS version")
            result = cur.fetchone()

            assert result is not None
            assert "version" in result

            print(f"   ✅ Query OK → MySQL {result['version']}")

        # ------------------------------------------------
        # 3. Rollback simulation
        # ------------------------------------------------

        print("\n3. Transaction rollback simulation")

        try:
            with mysql_cursor_sync(retry=handler) as cur:
                raise RuntimeError("Simulated failure")

        except RuntimeError:
            print("   ✅ Rollback correctly triggered")

        print("\n🎉 All synchronous tests PASSED!\n")

    except Exception as e:
        print(f"❌ Sync test failed: {e}")
        raise


# ────────────────────────────────────────────────
# Async tests
# ────────────────────────────────────────────────

async def run_async_tests():
    print("🧪 Running Asynchronous Tests...\n")

    handler = _default_retry()

    try:

        # ------------------------------------------------
        # 1. Direct async connection
        # ------------------------------------------------

        print("1. connect_async()")

        conn = await connect_async(retry=handler)

        driver = detect_driver(conn)

        assert conn is not None
        print(f"   ✅ Async connection OK (driver: {driver})")

        await asyncio.to_thread(conn.close)

        # ------------------------------------------------
        # 2. Async cursor test
        # ------------------------------------------------

        print("\n2. mysql_cursor_async() query")

        async with mysql_cursor_async(retry=handler) as cur:

            await asyncio.to_thread(cur.execute, "SELECT VERSION() AS version")
            result = await asyncio.to_thread(cur.fetchone)

            assert result is not None
            assert "version" in result

            print(f"   ✅ Async query OK → MySQL {result['version']}")

        print("\n🎉 All asynchronous tests PASSED!\n")

    except Exception as e:
        print(f"❌ Async test failed: {e}")
        raise


# ────────────────────────────────────────────────
# Main runner
# ────────────────────────────────────────────────

def run_all_tests():

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    print("=" * 70)
    print("🚀 MySQL Connection Module - Local Test Suite")
    print(f"Start time : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    if not (PYMYSQL_AVAILABLE or MYSQL_CONNECTOR_AVAILABLE):
        print("❌ No MySQL driver found.")
        sys.exit(1)

    print("Available drivers:")
    print(f"   PyMySQL: {'YES' if PYMYSQL_AVAILABLE else 'NO'}")
    print(f"   mysql-connector: {'YES' if MYSQL_CONNECTOR_AVAILABLE else 'NO'}")
    print()

    try:
        run_sync_tests()
        asyncio.run(run_async_tests())

        print("=" * 70)
        print("🎉 ALL TESTS PASSED SUCCESSFULLY!")

    except Exception as e:
        print(f"\n❌ Some tests failed: {e}")
        sys.exit(1)


# ────────────────────────────────────────────────

if __name__ == "__main__":
    run_all_tests()