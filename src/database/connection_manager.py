"""
Standalone database connection manager - no external utility dependencies.
"""

import logging
import re
import threading
from contextlib import contextmanager
from typing import Dict, Optional

logger = logging.getLogger(__name__)

try:
    import mysql.connector
    import mysql.connector.pooling as pooling
    MYSQL_AVAILABLE = True
    POOLING_AVAILABLE = True
except ImportError:
    mysql = None
    pooling = None
    MYSQL_AVAILABLE = False
    POOLING_AVAILABLE = False

try:
    from connect import mysql_connection, config as default_config, connect_to_mysql
    CONNECT_AVAILABLE = True
except ImportError:
    CONNECT_AVAILABLE = False
    default_config = {
        'user': 'root', 'password': '', 'host': '127.0.0.1',
        'database': 'trend_master',
    }

# Only alphanumerics and underscores are valid in a database name.
_DB_NAME_RE = re.compile(r'^\w+$')


def _validate_db_name(name: str) -> None:
    """Raise ValueError if *name* is not a safe SQL identifier."""
    if not _DB_NAME_RE.match(name):
        raise ValueError(
            f"Unsafe database name {name!r}: only word characters (a-z, 0-9, _) are allowed."
        )


class ConnectionPool:
    """Thread-safe MySQL connection pool."""

    def __init__(self, config: Dict, pool_size: int = 5):
        self.config    = config.copy()
        self.pool_size = pool_size
        self._pool: list = []
        self._used: set  = set()
        self._lock = threading.Lock()

        self.mysql     = mysql.connector if MYSQL_AVAILABLE else None
        self.pooling   = pooling         if POOLING_AVAILABLE else None
        self.connect_fn = connect_to_mysql if CONNECT_AVAILABLE else None

        self._init_pool()

    # ------------------------------------------------------------------
    # Initialisation
    # ------------------------------------------------------------------

    def _init_pool(self) -> None:
        """Initialise the connection pool (native preferred, manual fallback)."""
        if self.pooling and self.connect_fn:
            try:
                self._native_pool = self.pooling.MySQLConnectionPool(
                    pool_name="etl_pool",
                    pool_size=self.pool_size,
                    pool_reset_session=True,
                    **self._clean_config(),
                )
                self._pool_type = 'native'
                logger.info("Native pool initialised: %d connections", self.pool_size)
                return
            except Exception as e:
                logger.warning("Native pool init failed (%s); falling back to manual pool", e)

        self._pool_type = 'manual'
        self._create_manual_pool()

    def _clean_config(self) -> Dict:
        """Return a copy of config with keys that upset the native pool removed."""
        cfg = self.config.copy()
        cfg.pop('raise_on_warnings', None)
        return cfg

    def _create_manual_pool(self) -> None:
        """Pre-populate the manual pool without holding the lock during I/O."""
        # Create connections *outside* the lock so slow network calls don't
        # starve concurrent acquires.
        connections = []
        for _ in range(self.pool_size):
            conn = self._create_connection()
            if conn:
                connections.append(conn)

        with self._lock:
            self._pool.extend(connections)

        logger.info("Manual pool ready: %d/%d connections", len(connections), self.pool_size)

    def _create_connection(self):
        """Open and return a new database connection, or None on failure."""
        try:
            if self.connect_fn:
                return self.connect_fn(self.config)
            return self.mysql.connect(**self.config)
        except Exception as e:
            logger.error("Connection creation failed: %s", e)
            return None

    # ------------------------------------------------------------------
    # Acquire / release
    # ------------------------------------------------------------------

    @contextmanager
    def get_connection(self):
        """Yield a connection from the pool and return it automatically."""
        conn = self._acquire()
        try:
            yield conn
        finally:
            self._release(conn)

    def _acquire(self):
        """Acquire a connection from the pool."""
        if self._pool_type == 'native':
            try:
                return self._native_pool.get_connection()
            except Exception as e:
                logger.error("Native pool acquire failed: %s", e)
                return None

        # Manual pool
        with self._lock:
            conn = self._pool.pop() if self._pool else None

        if conn is not None:
            conn = self._test_connection(conn)
            with self._lock:
                self._used.add(conn)
            return conn

        # Pool exhausted — open a new connection on the caller's thread.
        return self._create_connection()

    def _test_connection(self, conn):
        """Verify *conn* is alive; attempt reconnect and return a working connection."""
        try:
            if hasattr(conn, 'is_connected') and not conn.is_connected():
                conn.reconnect()
            elif hasattr(conn, 'ping'):
                conn.ping(reconnect=True)
            return conn
        except Exception as e:
            logger.warning("Connection health-check failed (%s); replacing with a new connection", e)
            self._safe_close(conn)
            return self._create_connection()

    def _release(self, conn) -> None:
        """Return *conn* to the pool, or close it if the pool is full."""
        if conn is None:
            return

        if self._pool_type == 'native':
            # Closing a native-pool connection returns it to the pool.
            self._safe_close(conn)
            return

        with self._lock:
            self._used.discard(conn)
            pool_has_room = len(self._pool) < self.pool_size

        if pool_has_room:
            try:
                alive = hasattr(conn, 'is_connected') and conn.is_connected()
            except Exception as e:
                logger.debug("is_connected() raised during release: %s", e)
                alive = False

            if alive:
                with self._lock:
                    self._pool.append(conn)
                return

        self._safe_close(conn)

    # ------------------------------------------------------------------
    # Housekeeping
    # ------------------------------------------------------------------

    def _safe_close(self, conn) -> None:
        """Close *conn*, logging any error at DEBUG level."""
        if conn is None:
            return
        try:
            conn.close()
        except Exception as e:
            logger.debug("Ignoring error while closing connection: %s", e)

    def close_all(self) -> None:
        """Close every connection in the pool (available and in-use)."""
        with self._lock:
            all_conns = list(self._pool) + list(self._used)
            self._pool.clear()
            self._used.clear()

        for conn in all_conns:
            self._safe_close(conn)

    def get_stats(self) -> Dict:
        """Return a snapshot of pool utilisation."""
        with self._lock:
            return {
                'type':      self._pool_type,
                'size':      self.pool_size,
                'available': len(self._pool),
                'used':      len(self._used),
            }


class DatabaseConnection:
    """Enhanced database connection manager with optional pooling."""

    _pool: Optional[ConnectionPool] = None
    _pool_lock = threading.Lock()

    def __init__(self, config: Dict = None, enable_pooling: bool = True, pool_size: int = 5):
        self.config = config or default_config
        self.connection_attempts = 0
        self.enable_pooling = enable_pooling

        if enable_pooling and DatabaseConnection._pool is None:
            with DatabaseConnection._pool_lock:
                if DatabaseConnection._pool is None:
                    DatabaseConnection._pool = ConnectionPool(self.config, pool_size)

    # ------------------------------------------------------------------
    # Connection context managers
    # ------------------------------------------------------------------

    @contextmanager
    def get_connection(self):
        """Yield a database connection (pooled or direct)."""
        if self.enable_pooling and DatabaseConnection._pool:
            with DatabaseConnection._pool.get_connection() as conn:
                if conn is not None:
                    self.connection_attempts += 1
                else:
                    logger.warning("Pool returned None connection")
                yield conn
        else:
            with self._direct_connection() as conn:
                yield conn

    @contextmanager
    def _direct_connection(self):
        """Yield a direct (non-pooled) connection."""
        if CONNECT_AVAILABLE:
            # mysql_connection is itself a context manager that owns the connection
            # lifecycle; we must not close it again in a finally block.
            with mysql_connection(self.config) as conn:
                if conn is not None:
                    self.connection_attempts += 1
                yield conn
        else:
            conn = None
            try:
                conn = mysql.connector.connect(**self.config) if MYSQL_AVAILABLE else None
                if conn is not None:
                    self.connection_attempts += 1
                yield conn
            except Exception as e:
                logger.error("Direct connection error: %s", e)
                yield None
            finally:
                self._safe_close(conn)

    @contextmanager
    def get_connection_without_db(self, config: Dict):
        """Yield a connection that has no default database selected."""
        if CONNECT_AVAILABLE:
            with mysql_connection(config) as conn:
                yield conn
        else:
            conn = None
            try:
                conn = mysql.connector.connect(**config) if MYSQL_AVAILABLE else None
                yield conn
            except Exception as e:
                logger.error("DB-less connection error: %s", e)
                yield None
            finally:
                self._safe_close(conn)

    # ------------------------------------------------------------------
    # Database administration
    # ------------------------------------------------------------------

    def create_database_if_not_exists(self, database_name: str = None) -> bool:
        """Create *database_name* if it does not already exist."""
        try:
            db_name = database_name or self.config.get('database', 'trend_master')
            _validate_db_name(db_name)

            temp_config = {k: v for k, v in self.config.items() if k != 'database'}

            with self.get_connection_without_db(temp_config) as conn:
                if conn is None:
                    return False

                with conn.cursor() as cursor:
                    # db_name is validated above — safe to interpolate.
                    cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
                    cursor.execute(f"USE `{db_name}`")
                conn.commit()

            logger.info("Database '%s' ready", db_name)
            return True

        except ValueError as e:
            logger.error("Invalid database name: %s", e)
            return False
        except Exception as e:
            logger.error("Database creation error: %s", e)
            return False

    # ------------------------------------------------------------------
    # Testing / introspection
    # ------------------------------------------------------------------

    def test_connection(self) -> bool:
        """Return True if the database is reachable."""
        if CONNECT_AVAILABLE:
            conn = connect_to_mysql(self.config, attempts=1)
            if conn:
                self._safe_close(conn)
                return True
            return False

        with self.get_connection() as conn:
            return conn is not None

    def get_connection_stats(self) -> Dict:
        """Return connection and pool statistics."""
        stats: Dict = {
            'attempts':        self.connection_attempts,
            'pooling_enabled': self.enable_pooling,
        }
        if self.enable_pooling and DatabaseConnection._pool:
            stats.update(DatabaseConnection._pool.get_stats())
        return stats

    def get_config_summary(self) -> Dict:
        """Return a sanitised copy of the config (password redacted)."""
        summary = self.config.copy()
        if 'password' in summary:
            pw = summary['password']
            summary['password'] = '*' * len(pw) if pw else 'empty'
        return summary

    # ------------------------------------------------------------------
    # Housekeeping
    # ------------------------------------------------------------------

    def _safe_close(self, conn) -> None:
        """Close *conn*, logging any error at DEBUG level."""
        if conn is None:
            return
        try:
            conn.close()
        except Exception as e:
            logger.debug("Ignoring error while closing connection: %s", e)

    @classmethod
    def close_pool(cls) -> None:
        """Shut down the shared connection pool."""
        with cls._pool_lock:
            if cls._pool:
                cls._pool.close_all()
                cls._pool = None
                logger.info("Connection pool closed")


# ---------------------------------------------------------------------------
# Factory helper (backward-compatible)
# ---------------------------------------------------------------------------

def create_connection_manager(
    config=None, enable_pooling: bool = True, pool_size: int = 5
) -> DatabaseConnection:
    """Instantiate and return a :class:`DatabaseConnection`."""
    return DatabaseConnection(config, enable_pooling, pool_size)