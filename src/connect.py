r"""
C:\Economy\Invest\TrendMaster\src\connect.py
Production MySQL connection helpers with retry, transactions,
sync/async support and DictCursor convenience.

Usage:
    from src.connect import mysql_cursor

    with mysql_cursor() as cur:
        cur.execute("SELECT 1")
        print(cur.fetchone())
"""

import asyncio
import logging
import sys
from contextlib import contextmanager, asynccontextmanager
from datetime import datetime
from typing import Any, Dict, Optional, Generator, AsyncGenerator

from config import get_config
from src.common.retry import RetryHandler, RetryConfig

_logger = logging.getLogger(__name__)

# ────────────────────────────────────────────────
# Driver detection
# ────────────────────────────────────────────────

try:
    import pymysql
    import pymysql.cursors
    PYMYSQL_AVAILABLE = True
except ImportError:
    PYMYSQL_AVAILABLE = False

try:
    import mysql.connector
    MYSQL_CONNECTOR_AVAILABLE = True
except ImportError:
    MYSQL_CONNECTOR_AVAILABLE = False

# ────────────────────────────────────────────────
# Configuration Integration
# ────────────────────────────────────────────────

_DB_CFG = get_config().database
DEFAULT_CONFIG_DICT = _DB_CFG.to_dict()
config = DEFAULT_CONFIG_DICT
# ────────────────────────────────────────────────
# Retry configuration
# ────────────────────────────────────────────────

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
    Connects using parameters from MySQLConfig. Handles driver-specific 
    mappings for charset, collation, and sql_mode.
    """
    # 1. Start with the dict from MySQLConfig.to_dict()
    # This already contains charset, collation, and init_command (sql_mode)
    params = cfg.copy()

    # 2. Extract core connection info
    # Drivers use these consistently
    core_keys = {"host", "user", "password", "database", "port", "autocommit"}
    connection_args = {k: params[k] for k in core_keys if k in params}
    
    # Set a default timeout if not provided
    connection_args["connect_timeout"] = params.get("connect_timeout", 30)

    # 3. Handle Driver Specifics
    if PYMYSQL_AVAILABLE:
        _logger.debug("Connecting via PyMySQL: %s@%s", params.get('user'), params.get('host'))
        return pymysql.connect(
            **connection_args,
            charset=params.get("charset", "utf8mb4"),
            collation=params.get("collation", "utf8mb4_unicode_ci"),
            init_command=params.get("init_command"),  # Applies SET sql_mode=...
            cursorclass=pymysql.cursors.DictCursor
        )

    if MYSQL_CONNECTOR_AVAILABLE:
        _logger.debug("Connecting via mysql-connector: %s@%s", params.get('user'), params.get('host'))
        # mysql-connector uses 'init_command' but also 'raise_on_warnings'
        if params.get("raise_on_warnings") is not None:
            connection_args["raise_on_warnings"] = params["raise_on_warnings"]
        
        # Merge back charset/collation/init_command
        connection_args.update({
            "charset": params.get("charset"),
            "collation": params.get("collation"),
            "init_command": params.get("init_command"),
            "use_unicode": True
        })
        
        return mysql.connector.connect(**connection_args)

    raise RuntimeError("No MySQL driver installed.")


def _create_dict_cursor(conn: Any):
    """Create a dict-like cursor for both drivers."""
    if PYMYSQL_AVAILABLE and hasattr(pymysql.connections, 'Connection') and isinstance(conn, pymysql.connections.Connection):
        return conn.cursor()
    
    if MYSQL_CONNECTOR_AVAILABLE:
        try:
            return conn.cursor(dictionary=True)
        except AttributeError:
            pass
            
    return conn.cursor()

# ────────────────────────────────────────────────
# Low-level connect functions (REFINED)
# ────────────────────────────────────────────────

def connect_sync(
    config: Optional[Dict[str, Any]] = None,
    retry: Optional[RetryHandler] = None,
):
    """Synchronous connection using RetryHandler.execute_sync."""
    cfg = config or DEFAULT_CONFIG_DICT
    handler = retry or _default_retry()
    # explicitly call the sync path
    return handler.execute_sync(_connect, cfg)


async def connect_async(
    config: Optional[Dict[str, Any]] = None,
    retry: Optional[RetryHandler] = None,
):
    """Async connection using RetryHandler.execute_async with thread offloading."""
    cfg = config or DEFAULT_CONFIG_DICT
    handler = retry or _default_retry()
    # explicitly call the async path with thread offloading
    return await handler.execute_async(
        _connect,
        cfg,
        run_sync_in_thread=True,
    )

# ────────────────────────────────────────────────
# Context managers
# ────────────────────────────────────────────────

@contextmanager
def mysql_connection(
    config: Optional[Dict[str, Any]] = None,
    retry: Optional[RetryHandler] = None,
    autocommit: bool = False,
) -> Generator[Any, None, None]:
    """Yields a connection, ensuring health check and transaction integrity."""
    conn = None
    # Use the dict from the config object if none provided
    cfg = config or DEFAULT_CONFIG_DICT
    if autocommit:
        cfg = {**cfg, "autocommit": True}

    try:
        conn = connect_sync(cfg, retry)

        # Health check
        if hasattr(conn, "ping"):
            try:
                conn.ping(reconnect=True)
            except Exception:
                # If ping fails, we let the retry handler (if used) or caller handle it
                pass

        yield conn

        if not cfg.get("autocommit", False):
            conn.commit()

    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                _logger.error("Rollback failed after exception: %s", e)
        raise
    finally:
        if conn:
            try:
                conn.close()
            except Exception as err:
                _logger.warning("Error closing connection: %s", err)


@contextmanager
def mysql_cursor(
    config: Optional[Dict[str, Any]] = None,
    retry: Optional[RetryHandler] = None,
    autocommit: bool = False,
) -> Generator[Any, None, None]:
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
async def mysql_cursor_async(
    config: Optional[Dict[str, Any]] = None,
    retry: Optional[RetryHandler] = None,
    autocommit: bool = False,
) -> AsyncGenerator[Any, None]:
    conn = None
    cur = None
    cfg = config or DEFAULT_CONFIG_DICT
    if autocommit:
        cfg = {**cfg, "autocommit": True}

    try:
        conn = await connect_async(cfg, retry)
        if hasattr(conn, "ping"):
            await asyncio.to_thread(conn.ping, reconnect=True)

        cur = _create_dict_cursor(conn)
        yield cur

        if not autocommit:
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
1: Run from the project root (the folder containing src and config).
python -m src.connect
"""

def run_sync_tests():
    print("🧪 Running Synchronous Tests...\n")
    handler = _default_retry()
    try:
        print("1. mysql_cursor() - DictCursor")
        with mysql_cursor(retry=handler) as cur:
            cur.execute("SELECT VERSION() AS version")
            result = cur.fetchone()
            print(f"   ✅ DictCursor OK → MySQL {result.get('version', 'N/A')}")

        print("\n2. Transaction rollback test")
        try:
            with mysql_cursor(retry=handler) as cur:
                raise RuntimeError("Simulated failure")
        except RuntimeError:
            print("   ✅ Rollback correctly triggered")

        print("\n🎉 All synchronous tests PASSED!\n")
    except Exception as e:
        print(f"❌ Sync test failed: {e}")
        raise

async def run_async_tests():
    print("🧪 Running Asynchronous Tests...\n")
    handler = _default_retry()
    try:
        async with mysql_cursor_async(retry=handler) as cur:
            await asyncio.to_thread(cur.execute, "SELECT VERSION() AS version")
            result = await asyncio.to_thread(cur.fetchone)
            print(f"   ✅ mysql_cursor_async() OK → MySQL {result.get('version', 'N/A')}")
        print("🎉 All asynchronous tests PASSED!\n")
    except Exception as e:
        print(f"❌ Async test failed: {e}")
        raise

def run_all_tests():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    print("=" * 70)
    print("🚀 MySQL Connection Module - Local Test Suite")
    print(f"Start time : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    if not (PYMYSQL_AVAILABLE or MYSQL_CONNECTOR_AVAILABLE):
        print("❌ No MySQL driver found.")
        sys.exit(1)

    try:
        run_sync_tests()
        asyncio.run(run_async_tests())
        print("=" * 70)
        print("🎉 ALL TESTS PASSED SUCCESSFULLY!")
    except Exception as e:
        print(f"\n❌ Some tests failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_all_tests()