r"""
C:\Economy\Invest\TrendMaster\src\database\connection_manager.py
Standalone database connection manager — File 1 compatible,
with improved safety, pooling, and PyMySQL backend.
"""
import time
import threading
import logging
import re
from contextlib import contextmanager
from typing import Dict, Any, Optional
from collections import deque

import pymysql
import pymysql.cursors

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Validators
# ---------------------------------------------------------------------------

_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_name(name: str, kind: str = "name") -> str:
    if not isinstance(name, str):
        raise TypeError(f"{kind.capitalize()} name must be a string")

    if not _NAME_RE.fullmatch(name):
        raise ValueError(
            f"Unsafe {kind} name {name!r}: must contain only letters, digits, underscores."
        )

    return name


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def _open_connection(config: Dict) -> Optional[Any]:
    try:
        params = config.copy()

        core_keys = {"host", "user", "password", "database", "port", "autocommit"}
        args = {k: params[k] for k in core_keys if k in params}

        args["connect_timeout"] = params.get("connect_timeout", 30)

        return pymysql.connect(
            **args,
            charset=params.get("charset", "utf8mb4"),
            init_command=params.get("init_command"),
            cursorclass=pymysql.cursors.DictCursor,
        )
    except Exception as exc:
        _logger.error("Connection creation failed: %s", exc)
        return None

def _safe_close(conn) -> None:
    if conn is None:
        return
    try:
        conn.close()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Connection Pool
# ---------------------------------------------------------------------------

class ConnectionPool:
    """Thread-safe connection pool (Python 3.10+ optimized)."""

    def __init__(self, config: Dict, pool_size: int = 5, acquire_timeout: float = 30.0):
        if pool_size < 1:
            raise ValueError("pool_size must be >= 1")

        self._config = config.copy()
        self._pool_size = pool_size
        self._timeout = acquire_timeout

        self._available: deque[Any] = deque()
        self._used: set[Any] = set()

        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)

        self._closed = False

        self._initialize_pool()

    # -------------------------
    # Initialization
    # -------------------------
    def _initialize_pool(self) -> None:
        conns = []
        for _ in range(self._pool_size):
            conn = _open_connection(self._config)
            if conn:
                conns.append(conn)

        with self._lock:
            self._available.extend(conns)

        _logger.info("Pool initialized: %d/%d", len(conns), self._pool_size)

    # -------------------------
    # Public API
    # -------------------------
    @contextmanager
    def get_connection(self):
        conn = self._acquire()
        try:
            yield conn
        finally:
            self._release(conn)

    def get_stats(self) -> Dict:
        with self._lock:
            return {
                "size": self._pool_size,
                "available": len(self._available),
                "used": len(self._used),
                "closed": self._closed,
            }

    def close_all(self) -> None:
        with self._cond:
            if self._closed:
                return  # idempotent

            self._closed = True

            # Wake up ALL waiting threads so they can exit
            self._cond.notify_all()

            all_conns = list(self._available) + list(self._used)
            self._available.clear()
            self._used.clear()

        # Close connections outside the lock
        for conn in all_conns:
            _safe_close(conn)

        _logger.info("Pool closed")

    # -------------------------
    # Internal logic
    # -------------------------
    def _acquire(self):
        deadline = time.monotonic() + self._timeout

        while True:
            with self._cond:
                if self._closed:
                    raise RuntimeError("Pool is closed")

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Connection pool exhausted")

                ok = self._cond.wait_for(lambda: self._available, timeout=remaining)
                if not ok:
                    raise TimeoutError("Connection pool exhausted")

                conn = self._available.popleft()

            try:
                conn = self._ensure_connection(conn)
            except Exception:
                _logger.exception("Connection validation failed, retrying...")
                continue  # 🔥 retry instead of shrinking pool

            with self._cond:
                if self._closed:
                    _safe_close(conn)
                    raise RuntimeError("Pool closed during acquire")

                self._used.add(conn)
                return conn


    def _test_alive(self, conn) -> bool:
        try:
            if hasattr(conn, "ping"):
                conn.ping(reconnect=False)
            else:
                # fallback: lightweight query
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
            return True
        except Exception:
            return False

    def _ensure_connection(self, conn):
        """Ensure connection is alive, retry once if needed."""
        if conn and self._test_alive(conn):
            return conn

        _safe_close(conn)

        new_conn = _open_connection(self._config)
        if not new_conn:
            raise RuntimeError("Failed to create a new connection")

        return new_conn

    def _release(self, conn) -> None:
        if not conn:
            return

        with self._cond:
            if conn not in self._used:
                _safe_close(conn)
                return

            self._used.remove(conn)

            if self._closed:
                _safe_close(conn)
            elif len(self._available) < self._pool_size:
                self._available.append(conn)
            else:
                _safe_close(conn)

            self._cond.notify()

# ---------------------------------------------------------------------------
# DatabaseConnection 
# ---------------------------------------------------------------------------

class DatabaseConnection:
    """Database manager"""

    _pool: Optional[ConnectionPool] = None
    _pool_lock = threading.Lock()

    REQUIRED_KEYS = {"host", "user", "password",  "database"}  # add more if needed

    def __init__(
        self,
        config: Optional[Dict] = None,
        enable_pooling: bool = True,
        pool_size: int = 5,
        acquire_timeout: float = 30.0,
    ):
        # --- Validate config ---
        if config is None:
            raise ValueError("config is not provided")

        missing = self.REQUIRED_KEYS - config.keys()
        if missing:
            raise ValueError(f"Missing required config keys: {', '.join(missing)}")

        # Make a defensive copy
        self.config = config.copy()

        self.enable_pooling = enable_pooling
        self.connection_attempts = 0

        if enable_pooling:
            with DatabaseConnection._pool_lock:
                if DatabaseConnection._pool is None:
                    DatabaseConnection._pool = ConnectionPool(
                        self.config,
                        pool_size=pool_size,
                        acquire_timeout=acquire_timeout,
                    )

    @contextmanager
    def get_connection(self):
        if self.enable_pooling and DatabaseConnection._pool:
            try:
                with DatabaseConnection._pool.get_connection() as conn:
                    if conn:
                        self.connection_attempts += 1
                    yield conn
            except Exception as e:
                _logger.error("Pool error: %s", e)
                yield None
        else:
            with self._direct_connection() as conn:
                yield conn

    @contextmanager
    def _direct_connection(self):
        conn = None
        try:
            conn = _open_connection(self.config)
            if conn:
                self.connection_attempts += 1
            yield conn
        except Exception as e:
            _logger.error("Direct connection error: %s", e)
            yield None
        finally:
            _safe_close(conn)

    @contextmanager
    def get_connection_without_db(self, config: Optional[Dict] = None):
        cfg = config or {k: v for k, v in self.config.items() if k != "database"}
        conn = None
        try:
            conn = _open_connection(cfg)
            yield conn
        except Exception as e:
            _logger.error("DB-less connection error: %s", e)
            yield None
        finally:
            _safe_close(conn)

    def test_connection(self) -> bool:
        try:
            conn = _open_connection(self.config)
            ok = conn is not None
            _safe_close(conn)
            return ok
        except Exception:
            return False

    def get_connection_stats(self) -> Dict:
        stats = {
            "attempts": self.connection_attempts,
            "pooling_enabled": self.enable_pooling,
        }

        if self.enable_pooling and DatabaseConnection._pool:
            stats.update(DatabaseConnection._pool.get_stats())

        return stats

    def get_config_summary(self) -> Dict:
        summary = self.config.copy()
        if "password" in summary:
            pw = summary["password"]
            summary["password"] = "*" * len(str(pw)) if pw else "empty"
        return summary

    @classmethod
    def close_pool(cls):
        with cls._pool_lock:
            if cls._pool:
                cls._pool.close_all()
                cls._pool = None
                _logger.info("Connection pool closed")


# ---------------------------------------------------------------------------
# Factory 
# ---------------------------------------------------------------------------

def create_connection_manager(config=None, enable_pooling=True, pool_size=5):
    return DatabaseConnection(config, enable_pooling, pool_size)
