r"""
C:\Economy\Invest\TrendMaster\src\database\connection_manager.py
Standalone database connection manager — no external utility dependencies.
"""
import threading
import logging
import re
from contextlib import contextmanager
from typing import Dict, Optional
import pymysql
import pymysql.cursors
from sqlalchemy import create_engine

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants / defaults
# ---------------------------------------------------------------------------

_DEFAULT_CONFIG: Dict = {
    "user": "root",
    "password": "",
    "host": "127.0.0.1",
    "database": "trend_master",
    # autocommit=False: callers must call conn.commit() explicitly.
    # This ensures writes are intentional and errors trigger a clean rollback.
    "autocommit": False,
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
    """Open a single raw PyMySQL connection."""
    try:
        params = config.copy()
        core_keys = {"host", "user", "password", "database", "port", "autocommit"}
        connection_args = {k: params[k] for k in core_keys if k in params}
        connection_args["connect_timeout"] = params.get("connect_timeout", 30)

        return pymysql.connect(
            **connection_args,
            charset=params.get("charset", "utf8mb4"),
            init_command=params.get("init_command"),
            cursorclass=pymysql.cursors.DictCursor,
        )
    except Exception as exc:
        _logger.error("Connection creation failed: %s", exc)
    return None


def _test_alive(conn) -> bool:
    """Return True if *conn* appears to be alive."""
    try:
        if hasattr(conn, "ping"):
            conn.ping(reconnect=False)
            return True
    except Exception:
        pass
    return False
# ---------------------------------------------------------------------------
# ConnectionManager (with SQLAlchemy engine)
# ---------------------------------------------------------------------------

class ConnectionManager:
    """
    High-level DB manager combining:
    - manual PyMySQL connection pool
    - SQLAlchemy engine for metadata inspection
    """

    def __init__(
        self,
        config: Optional[Dict] = None,
        pool_size: int = 5,
        acquire_timeout: float = 30.0,
    ) -> None:
        self._config = {**_DEFAULT_CONFIG, **(config or {})}

        # SQLAlchemy engine (for schema inspection, ORM, metadata)
        self.engine = create_engine(
            self._build_sqlalchemy_url(self._config),
            pool_pre_ping=True,
            future=True,
        )

        # Manual PyMySQL pool (for actual query execution)
        self._pool = ConnectionPool(
            self._config,
            pool_size=pool_size,
            acquire_timeout=acquire_timeout,
        )

    # ------------------------------------------------------------------

    def _build_sqlalchemy_url(self, cfg: Dict) -> str:
        user = cfg.get("user", "root")
        password = cfg.get("password", "")
        host = cfg.get("host", "127.0.0.1")
        database = cfg.get("database", "")
        port = cfg.get("port", 3306)

        return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"

    # ------------------------------------------------------------------

    @contextmanager
    def get_connection(self):
        """Proxy to the underlying PyMySQL pool."""
        with self._pool.get_connection() as conn:
            yield conn

    def close_all(self) -> None:
        """Close all pooled connections."""
        self._pool.close_all()

    def get_stats(self) -> Dict:
        """Return pool stats."""
        return self._pool.get_stats()

# ---------------------------------------------------------------------------
# ConnectionPool
# ---------------------------------------------------------------------------

class ConnectionPool:
    """
    Thread-safe manual MySQL connection pool.

    Design principles:
    - Heal on acquire ONLY
    - Acquire is atomic (pop → heal → track under one lock)
    - `_used` always reflects real checked-out connections
    - No dual tracking, no identity mismatch
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

        self._available: list = []
        self._used: set = set()

        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)

        self._initialize_pool()

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------

    def _initialize_pool(self) -> None:
        """Create initial pool connections."""
        conns = []

        for _ in range(self._pool_size):
            conn = _open_connection(self._config)
            if conn:
                conns.append(conn)

        with self._lock:
            self._available.extend(conns)

        _logger.info(
            "Pool initialized: %d/%d connections",
            len(conns),
            self._pool_size,
        )

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
        """Close all connections."""
        with self._lock:
            all_conns = list(self._available) + list(self._used)
            self._available.clear()
            self._used.clear()

        for conn in all_conns:
            _safe_close(conn)

        _logger.info("Connection pool closed")

    def get_stats(self) -> Dict:
        """Return pool stats."""
        with self._lock:
            return {
                "size": self._pool_size,
                "available": len(self._available),
                "in_use": len(self._used),
            }

    # ------------------------------------------------------------------
    # Internal acquire / release
    # ------------------------------------------------------------------

    def _acquire(self):
        """Acquire a connection, blocking until available."""
        with self._not_empty:
            ok = self._not_empty.wait_for(
                lambda: bool(self._available),
                timeout=self._acquire_timeout,
            )

            if not ok:
                raise TimeoutError(
                    f"Could not acquire connection within {self._acquire_timeout}s "
                    f"(pool size={self._pool_size}, in use={len(self._used)})"
                )

            conn = self._available.pop()

            # Heal INSIDE lock (atomic)
            if not _test_alive(conn):
                _logger.warning("Stale connection detected — replacing")
                _safe_close(conn)

                conn = _open_connection(self._config)
                if conn is None:
                    raise RuntimeError("Failed to open replacement connection")

            self._used.add(conn)

            return conn

    def _release(self, conn) -> None:
        """Return connection to the pool."""
        if conn is None:
            return

        with self._not_empty:
            self._used.discard(conn)

            if len(self._available) < self._pool_size:
                self._available.append(conn)
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
        """Yield a database connection (pooled or direct).

        autocommit is False by default — call conn.commit() after writes,
        or conn.rollback() on error.
        """

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
        """Yield a direct (non-pooled) connection.

        Rolls back automatically on exception; caller must commit() on success.
        """
        conn = _open_connection(self._config)

        if conn is not None:
            self.connection_attempts += 1

        try:
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
    def get_connection_without_db(self, config: Dict):
        """Yield connection without selecting default database."""
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
        conn = _open_connection(self._config)
        alive = conn is not None
        _safe_close(conn)
        return alive

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
