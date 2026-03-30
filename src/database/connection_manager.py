r"""
C:\Economy\Invest\TrendMaster\src\database\connection_manager.py
Standalone database connection manager — no external utility dependencies.
"""

import threading
import logging
import re
from contextlib import contextmanager
from typing import Dict, Any, Optional, Final
from collections import deque

import pymysql
import pymysql.cursors
from pymysql.connections import Connection

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helper functions (assumed to exist elsewhere - adjust as needed)
# ---------------------------------------------------------------------------

def _open_connection(config: Dict[str, Any]) -> Optional[Connection]:
    """
    Create a new PyMySQL connection.

    Returns:
        Connection object on success, None on any failure.
        Failures are logged but do not raise (to keep pool compatible).
    """
    if not config or not isinstance(config, dict):
        _logger.error("Invalid config passed to _open_connection")
        return None

    # Validate required fields
    required = {"user", "database"}
    missing = [k for k in required if k not in config or config[k] is None]
    if missing:
        _logger.error("Missing required database config fields: %s", missing)
        return None

    try:
        params = config.copy()

        connection_args: Dict[str, Any] = {
            "host": params.get("host", "localhost"),
            "user": params["user"],
            "password": params.get("password", ""),
            "database": params["database"],
            "port": params.get("port", 3306),
            "connect_timeout": params.get("connect_timeout", 30),
            "charset": params.get("charset", "utf8mb4"),
            "cursorclass": params.get("cursorclass", pymysql.cursors.DictCursor),
        }

        # Add optional parameters
        optional_keys = {
            "init_command", "ssl", "unix_socket",
            "read_timeout", "write_timeout", "autocommit",
        }

        for key in optional_keys:
            if key in params and params[key] is not None:
                connection_args[key] = params[key]

        # Establish connection
        conn = pymysql.connect(**connection_args)
        return conn

    except pymysql.Error as e:
        # Normal MySQL errors (wrong credentials, host down, etc.) — log cleanly
        _logger.error("MySQL connection failed: %s", e)
        return None
    except Exception:
        # Unexpected errors (programming bugs, import issues, etc.)
        _logger.exception("Unexpected error while creating connection")
        return None


def _safe_close(conn: Optional[Any]) -> None:
    if conn is None:
        return

    if not hasattr(conn, "close"):
        return

    try:
        conn.close()
    except Exception as exc:
        _logger.debug(
            "Ignored error while closing connection (%s): %s",
            type(exc).__name__,
            exc,
        )


def _test_alive(conn: Any) -> bool:
    """Test if the connection is still alive."""
    if conn is None:
        return False

    # Fast path: driver ping
    if hasattr(conn, "ping"):
        try:
            conn.ping(reconnect=False)
            return True
        except Exception:
            pass

    # Fallback: simple query
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        return True
    except Exception:
        return False

_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$") # Shared regex pattern

def _validate_name(name: str, kind: str = "name") -> str:
    """
    Validate a SQL identifier (e.g., table or database name).
    """
    if not isinstance(name, str):
        raise TypeError(f"{kind.capitalize()} name must be a string")

    if not name:
        raise ValueError(f"{kind.capitalize()} name cannot be empty")

    if not _NAME_RE.fullmatch(name):
        raise ValueError(
            f"Unsafe {kind} name {name!r}: must start with a letter or underscore "
            "and contain only letters, digits, and underscores"
        )

    return name.lower()  # optional normalization

# ---------------------------------------------------------------------------
# ConnectionPool
# ---------------------------------------------------------------------------

class ConnectionPool:
    """Thread-safe MySQL connection pool with improved safety and performance."""

    def __init__(
        self,
        config: Dict,
        pool_size: int = 5,
        acquire_timeout: float = 30.0,
    ) -> None:
        if pool_size < 1:
            raise ValueError("pool_size must be >= 1")

        self._config = config.copy()
        self._pool_size = pool_size
        self._acquire_timeout = acquire_timeout

        self._available: deque = deque()
        self._used: set = set()

        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)

        self._closed = False

        self._initialize_pool()

    def _initialize_pool(self) -> None:
        conns = []
        for _ in range(self._pool_size):
            try:
                conn = _open_connection(self._config)
                if conn:
                    conns.append(conn)
                else:
                    _logger.warning("Failed to create initial connection")
            except Exception as e:
                _logger.warning("Failed to create initial connection: %s", e)

        with self._lock:
            self._available.extend(conns)

        _logger.info("Pool initialized: %d/%d connections", len(conns), self._pool_size)

    @contextmanager
    def get_connection(self):
        """Context manager for acquiring a connection from the pool."""
        conn = self._acquire()
        try:
            yield conn
        finally:
            self._release(conn)

    def close_all(self) -> None:
        """Close all connections and shut down the pool."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            all_conns = list(self._available) + list(self._used)
            self._available.clear()
            self._used.clear()

        for conn in all_conns:
            _safe_close(conn)

        _logger.info("Connection pool closed")

    def get_stats(self) -> Dict:
        with self._lock:
            return {
                "size": self._pool_size,
                "available": len(self._available),
                "in_use": len(self._used),
                "closed": self._closed,
            }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _acquire(self) -> Any:
        """Acquire a connection (FIFO). Releases lock during slow operations."""
        with self._not_empty:
            if self._closed:
                raise RuntimeError("Pool is closed")

            ok = self._not_empty.wait_for(
                lambda: bool(self._available),
                timeout=self._acquire_timeout,
            )

            if not ok:
                raise TimeoutError(f"Connection pool exhausted after {self._acquire_timeout}s")

            conn = self._available.popleft()  # O(1) with deque

        # === Critical: Release lock before potentially slow network operations ===
        if not _test_alive(conn):
            _logger.warning("Replacing stale connection")
            _safe_close(conn)
            try:
                conn = _open_connection(self._config)
                if conn is None:
                    raise RuntimeError("Failed to recreate connection")
            except Exception as e:
                raise RuntimeError(f"Failed to recreate connection: {e}") from e

        # Re-acquire lock only for bookkeeping
        with self._not_empty:
            if self._closed:
                _safe_close(conn)
                raise RuntimeError("Pool was closed during acquisition")
            self._used.add(conn)
            return conn

    def _release(self, conn: Optional[Any]) -> None:
        if conn is None:
            return

        with self._not_empty:
            if conn not in self._used:
                _logger.warning("Releasing unknown connection")
                _safe_close(conn)
                return

            self._used.remove(conn)

            if self._closed:
                _safe_close(conn)
            elif len(self._available) < self._pool_size:
                self._available.append(conn)  # Return to pool
            else:
                # Pool at capacity → close extra
                _safe_close(conn)

            self._not_empty.notify()  # Wake one waiting thread

# ---------------------------------------------------------------------------
# DatabaseConnection
# ---------------------------------------------------------------------------

class DatabaseConnection:
    """Database connection manager with optional pooling."""

    def __init__(
        self,
        config: Dict | None = None,
        enable_pooling: bool = True,
        pool_size: int = 5,
        acquire_timeout: float = 30.0,
    ) -> None:
        if config is None or not isinstance(config, dict) or not config:
            raise ValueError("DatabaseConnection requires a non-empty config dictionary.")

        self._config = config.copy()
        self._enable_pooling = enable_pooling
        self.connection_attempts = 0

        self._pool: Optional[ConnectionPool] = None

        if enable_pooling:
            self._pool = ConnectionPool(
                self._config,
                pool_size=pool_size,
                acquire_timeout=acquire_timeout,
            )

    @contextmanager
    def get_connection(self):
        """Return a pooled or direct connection."""
        if self._enable_pooling and self._pool:
            with self._pool.get_connection() as conn:
                if conn:
                    self.connection_attempts += 1
                else:
                    _logger.warning("Pool returned None connection")
                yield conn
        else:
            with self._direct_connection() as conn:
                yield conn

    @contextmanager
    def _direct_connection(self):
        """Open a direct (non-pooled) connection."""
        conn = None
        try:
            conn = _open_connection(self._config)
            if conn:
                self.connection_attempts += 1
            yield conn
        except Exception:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise
        finally:
            _safe_close(conn)

    @contextmanager
    def get_connection_without_db(self, config: Optional[Dict] = None):
        """Open a connection without selecting a specific database."""
        cfg = config or {k: v for k, v in self._config.items() if k != "database"}
        conn = None
        try:
            conn = _open_connection(cfg)
            yield conn
        finally:
            _safe_close(conn)

    def get_schema(self, table_name: str) -> list[str]:
        """Return ordered list of column names for a table using INFORMATION_SCHEMA."""
        try:
            _validate_name(table_name)
            db_name = self._config.get("database")
            _validate_name(db_name)

            with self.get_connection() as conn:
                if not conn:
                    _logger.warning("No connection available for schema fetch")
                    return []

                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT COLUMN_NAME
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                        ORDER BY ORDINAL_POSITION
                        """,
                        (db_name, table_name),
                    )
                    rows = cur.fetchall()

                    if not rows:
                        _logger.info("No columns found for table %s.%s", db_name, table_name)
                        return []

                    # Handle both dict and tuple cursors
                    if isinstance(rows[0], dict):
                        return [row["COLUMN_NAME"] for row in rows if "COLUMN_NAME" in row]
                    return [row[0] for row in rows]

        except ValueError as e:
            _logger.warning("Validation error in get_schema: %s", e)
            return []
        except Exception as e:
            _logger.warning("Failed to get schema for '%s': %s", table_name, e)
            return []

    def create_database_if_not_exists(self, database_name: Optional[str] = None) -> bool:
        """Create the database if it does not already exist."""
        try:
            db_name = database_name or self._config.get("database")
            _validate_name(db_name)

            temp_config = {k: v for k, v in self._config.items() if k != "database"}

            with self.get_connection_without_db(temp_config) as conn:
                if not conn:
                    return False
                with conn.cursor() as cur:
                    cur.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
                conn.commit()

            _logger.info("Database '%s' is ready", db_name)
            return True

        except ValueError as exc:
            _logger.error("Invalid database name: %s", exc)
            return False
        except Exception as exc:
            _logger.error("Database creation failed: %s", exc)
            return False

    def test_connection(self) -> bool:
        """Test if a connection can be opened."""
        try:
            conn = _open_connection(self._config)
            alive = conn is not None
            _safe_close(conn)
            return alive
        except Exception:
            return False

    def get_connection_stats(self) -> Dict:
        stats = {
            "attempts": self.connection_attempts,
            "pooling_enabled": self._enable_pooling,
        }
        if self._enable_pooling and self._pool:
            stats.update(self._pool.get_stats())
        return stats

    def get_config_summary(self) -> Dict:
        """Return config summary with password masked."""
        summary = self._config.copy()
        if "password" in summary:
            pw = summary["password"]
            summary["password"] = "*" * len(str(pw)) if pw else "empty"
        return summary

    def close_pool(self) -> None:
        """Close the connection pool if enabled."""
        if self._pool:
            self._pool.close_all()
            self._pool = None
            _logger.info("Connection pool closed")


# Usage example:
# if __name__ == "__main__":
#     config = {"host": "localhost", "user": "root", "password": "", "database": "test"}
#     db = DatabaseConnection(config, enable_pooling=True, pool_size=10)
#     with db.get_connection() as conn:
#         ...
#     db.close_pool()

# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def create_connection_manager(
    config=None,
    enable_pooling: bool = True,
    pool_size: int = 5,
    acquire_timeout: float = 30.0,
) -> DatabaseConnection:
    return DatabaseConnection(config, enable_pooling, pool_size, acquire_timeout)
