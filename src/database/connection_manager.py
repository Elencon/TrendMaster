r"""
C:\Economy\Invest\TrendMaster\src\database\connection_manager.py
Standalone database connection manager — no external utility dependencies.
"""

import logging
import re
import threading
from contextlib import contextmanager
from typing import Dict, Optional

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Optional dependencies
# ---------------------------------------------------------------------------

try:
    import mysql.connector
    import mysql.connector.pooling as _pooling
except ImportError:
    mysql = None
    _pooling = None

try:
    from connect import connect_to_mysql, mysql_connection
except ImportError:
    connect_to_mysql = None
    mysql_connection = None

_MYSQL_AVAILABLE = mysql is not None
_CONNECT_AVAILABLE = connect_to_mysql is not None

# ---------------------------------------------------------------------------
# Constants / defaults
# ---------------------------------------------------------------------------

_DEFAULT_CONFIG: Dict = {
    "user": "root",
    "password": "",
    "host": "127.0.0.1",
    "database": "trend_master",
}

_DB_NAME_RE = re.compile(r"^\w+$")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _validate_db_name(name: str) -> None:
    """Raise ValueError if *name* is not a safe SQL identifier."""
    if not _DB_NAME_RE.match(name):
        raise ValueError(
            f"Unsafe database name {name!r}: only word characters are allowed."
        )


def _safe_close(conn) -> None:
    """Close *conn*, suppressing and logging any error."""
    if conn is None:
        return
    try:
        conn.close()
    except Exception as exc:
        _logger.debug("Ignored error while closing connection: %s", exc)


def _open_connection(config: Dict):
    """Open a single raw connection using whatever driver is available."""
    try:
        if _CONNECT_AVAILABLE:
            return connect_to_mysql(config)
        if _MYSQL_AVAILABLE:
            return mysql.connector.connect(**config)
    except Exception as exc:
        _logger.error("Connection creation failed: %s", exc)
    return None


def _test_alive(conn) -> bool:
    """Return True if *conn* appears to be alive."""
    try:
        if hasattr(conn, "is_connected"):
            return conn.is_connected()
        if hasattr(conn, "ping"):
            conn.ping(reconnect=False)
            return True
    except Exception:
        pass
    return False


# ---------------------------------------------------------------------------
# ConnectionPool
# ---------------------------------------------------------------------------


class ConnectionPool:
    """
    Thread-safe MySQL connection pool (native preferred, manual fallback).

    Manual pool blocks until a connection is available instead of creating
    unlimited new connections when exhausted.
    """

    def __init__(
        self,
        config: Dict,
        pool_size: int = 5,
        acquire_timeout: float = 30.0,
    ) -> None:
        self._config = config.copy()
        self._pool_size = pool_size
        self._acquire_timeout = acquire_timeout

        self._pool: list = []
        self._used: set = set()

        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)

        self._type = "manual"
        self._native = None

        self._init()

    # ------------------------------------------------------------------

    def _init(self) -> None:
        """Initialize native pool if available, otherwise manual pool."""

        if _MYSQL_AVAILABLE and _pooling is not None:
            try:
                cfg = {k: v for k, v in self._config.items() if k != "raise_on_warnings"}

                self._native = _pooling.MySQLConnectionPool(
                    pool_name="etl_pool",
                    pool_size=self._pool_size,
                    pool_reset_session=True,
                    **cfg,
                )

                self._type = "native"
                _logger.info("Native pool ready: %d connections", self._pool_size)
                return

            except Exception as exc:
                _logger.warning("Native pool failed (%s); using manual pool", exc)

        self._fill_manual_pool()

    def _fill_manual_pool(self) -> None:
        """Pre-create manual pool connections."""
        conns = []

        for _ in range(self._pool_size):
            conn = _open_connection(self._config)
            if conn:
                conns.append(conn)

        with self._lock:
            self._pool.extend(conns)

        _logger.info("Manual pool ready: %d/%d connections", len(conns), self._pool_size)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @contextmanager
    def get_connection(self):
        """Yield a connection from the pool; return it automatically."""
        conn = self._acquire()
        try:
            yield conn
        finally:
            self._release(conn)

    def close_all(self) -> None:
        """Close every connection in the pool."""
        with self._lock:
            all_conns = list(self._pool) + list(self._used)
            self._pool.clear()
            self._used.clear()

        for conn in all_conns:
            _safe_close(conn)

    def get_stats(self) -> Dict:
        """Return a snapshot of pool utilisation."""
        with self._lock:
            return {
                "type": self._type,
                "size": self._pool_size,
                "available": len(self._pool),
                "used": len(self._used),
            }

    # ------------------------------------------------------------------
    # Internal acquire / release
    # ------------------------------------------------------------------

    def _acquire(self):
        """Return a connection from the pool."""

        if self._type == "native":
            try:
                return self._native.get_connection()
            except Exception as exc:
                _logger.error("Native pool acquire failed: %s", exc)
                return None

        with self._not_empty:

            available = self._not_empty.wait_for(
                lambda: bool(self._pool),
                timeout=self._acquire_timeout,
            )

            if not available:
                raise TimeoutError(
                    f"Could not acquire connection within "
                    f"{self._acquire_timeout}s "
                    f"(pool size={self._pool_size}, in use={len(self._used)})"
                )

            conn = self._pool.pop()
            self._used.add(conn)

        return self._heal(conn)

    def _heal(self, conn):
        """Return *conn* if alive, otherwise replace it."""
        if _test_alive(conn):
            return conn

        _logger.warning("Stale connection detected — replacing")

        _safe_close(conn)

        replacement = _open_connection(self._config)

        if replacement is None:
            with self._not_empty:
                self._used.discard(conn)
                self._not_empty.notify()
            raise RuntimeError("Failed to open replacement connection")

        return replacement

    def _release(self, conn) -> None:
        """Return connection to pool and notify waiting thread."""

        if conn is None:
            return

        if self._type == "native":
            _safe_close(conn)
            return

        with self._not_empty:

            self._used.discard(conn)

            if len(self._pool) < self._pool_size and _test_alive(conn):
                self._pool.append(conn)
            else:
                _safe_close(conn)

            self._not_empty.notify()

# ---------------------------------------------------------------------------
# DatabaseConnection
# ---------------------------------------------------------------------------


class DatabaseConnection:
    """Database connection manager with optional pooling."""

    _pool: Optional[ConnectionPool] = None
    _pool_lock = threading.Lock()

    def __init__(
        self,
        config: Dict = None,
        enable_pooling: bool = True,
        pool_size: int = 5,
        acquire_timeout: float = 30.0,
    ) -> None:

        # prevent mutation of default config
        self._config = (config or _DEFAULT_CONFIG).copy()

        self._enable_pooling = enable_pooling
        self.connection_attempts = 0

        if enable_pooling and DatabaseConnection._pool is None:

            with DatabaseConnection._pool_lock:

                if DatabaseConnection._pool is None:

                    DatabaseConnection._pool = ConnectionPool(
                        self._config,
                        pool_size,
                        acquire_timeout,
                    )

    # ------------------------------------------------------------------

    @contextmanager
    def get_connection(self):
        """Yield a database connection (pooled or direct)."""

        if self._enable_pooling and DatabaseConnection._pool:

            with DatabaseConnection._pool.get_connection() as conn:

                if conn is None:
                    _logger.warning("Pool returned None connection")
                else:
                    self.connection_attempts += 1

                yield conn

        else:

            with self._direct_connection() as conn:
                yield conn

    @contextmanager
    def _direct_connection(self):
        """Yield a direct (non-pooled) connection."""

        if _CONNECT_AVAILABLE:

            with mysql_connection(self._config) as conn:

                if conn is not None:
                    self.connection_attempts += 1

                yield conn

            return

        conn = _open_connection(self._config)

        if conn is not None:
            self.connection_attempts += 1

        try:
            yield conn
        finally:
            _safe_close(conn)

    @contextmanager
    def get_connection_without_db(self, config: Dict):
        """Yield connection without selecting default database."""

        if _CONNECT_AVAILABLE:

            with mysql_connection(config) as conn:
                yield conn

            return

        conn = _open_connection(config)

        try:
            yield conn
        finally:
            _safe_close(conn)

    # ------------------------------------------------------------------
    # Database administration
    # ------------------------------------------------------------------

    def create_database_if_not_exists(self, database_name: str = None) -> bool:

        try:

            db_name = database_name or self._config.get("database", "trend_master")

            _validate_db_name(db_name)

            temp_config = {k: v for k, v in self._config.items() if k != "database"}

            with self.get_connection_without_db(temp_config) as conn:

                if conn is None:
                    return False

                with conn.cursor() as cur:

                    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
                    cur.execute(f"USE `{db_name}`")

                conn.commit()

            _logger.info("Database '%s' ready", db_name)

            return True

        except ValueError as exc:

            _logger.error("Invalid database name: %s", exc)
            return False

        except Exception as exc:

            _logger.error("Database creation error: %s", exc)
            return False

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def test_connection(self) -> bool:

        if _CONNECT_AVAILABLE:

            conn = connect_to_mysql(self._config, attempts=1)

            alive = conn is not None

            _safe_close(conn)

            return alive

        with self.get_connection() as conn:
            return conn is not None

    def get_connection_stats(self) -> Dict:

        stats: Dict = {
            "attempts": self.connection_attempts,
            "pooling_enabled": self._enable_pooling,
        }

        if self._enable_pooling and DatabaseConnection._pool:
            stats.update(DatabaseConnection._pool.get_stats())

        return stats

    def get_config_summary(self) -> Dict:

        summary = self._config.copy()

        if "password" in summary:

            pw = summary["password"]

            summary["password"] = "*" * len(pw) if pw else "empty"

        return summary

    # ------------------------------------------------------------------

    @classmethod
    def close_pool(cls) -> None:

        with cls._pool_lock:

            if cls._pool:

                cls._pool.close_all()

                cls._pool = None

                _logger.info("Connection pool closed")


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def create_connection_manager(
    config=None,
    enable_pooling: bool = True,
    pool_size: int = 5,
    acquire_timeout: float = 30.0,
) -> DatabaseConnection:
    """Return a configured DatabaseConnection."""
    return DatabaseConnection(config, enable_pooling, pool_size, acquire_timeout)