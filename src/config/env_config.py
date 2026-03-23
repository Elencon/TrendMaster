"""
Centralised environment configuration using .env file.
Provides typed access to environment variables with fallback defaults.

This module no longer depends directly on `os` outside of the backend adapter.
Environment access is abstracted through a backend protocol.
"""

import logging
import os
from pathlib import Path
from typing import Dict, Optional, Protocol

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Environment backend protocol
# ---------------------------------------------------------------------------

class EnvBackend(Protocol):
    """Protocol for environment variable providers."""

    def get(self, key: str) -> Optional[str]:
        ...

# ---------------------------------------------------------------------------
# Default backend using python-dotenv (if available)
# ---------------------------------------------------------------------------

class DotEnvBackend:
    """Environment backend that loads variables from .env + system env."""

    def __init__(self) -> None:
        self._vars: Dict[str, str] = {}

        # Load .env file first (lower priority)
        try:
            from dotenv import dotenv_values
            env_path = Path(__file__).parent.parent / ".env"
            if env_path.exists():
                self._vars.update(dotenv_values(env_path))
                _logger.info("Loaded environment variables from %s", env_path)
            else:
                _logger.info("No .env file found at %s", env_path)
        except ImportError:
            _logger.warning("python-dotenv not installed. Only system env will be used.")

        # Merge system environment last (highest priority)
        self._vars.update(os.environ)

    def get(self, key: str) -> Optional[str]:
        return self._vars.get(key)


# Global backend instance — swap out in tests via set_env_backend()
_backend: EnvBackend = DotEnvBackend()


def set_env_backend(backend: EnvBackend) -> None:
    """Replace the global environment backend (useful for testing)."""
    global _backend
    _backend = backend


# ---------------------------------------------------------------------------
# Helper functions (no direct os access)
# ---------------------------------------------------------------------------

def _env(key: str, default: str = "") -> str:
    """Return an environment variable as a string, or *default*."""
    value = _backend.get(key)
    return value if value is not None else default


def _env_int(key: str, default: int) -> int:
    """Return an environment variable coerced to int, or *default*."""
    raw = _backend.get(key)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        _logger.warning(
            "Env var %s='%s' is not a valid int; using default %s", key, raw, default
        )
        return default


def _env_bool(key: str, default: bool) -> bool:
    """Return an environment variable coerced to bool, or *default*."""
    raw = _backend.get(key)
    if raw is None:
        return default
    return raw.strip().lower() in ("true", "1", "yes", "on")


# ---------------------------------------------------------------------------
# EnvConfig — typed property-based access
# ---------------------------------------------------------------------------

class EnvConfig:
    """Typed, centralised access to environment variables."""

    # Re-export helpers as static methods for callers that hold an
    # EnvConfig reference rather than importing the module-level helpers.
    get      = staticmethod(_env)
    get_int  = staticmethod(_env_int)
    get_bool = staticmethod(_env_bool)

    # ------------------------------------------------------------------
    # Database
    # ------------------------------------------------------------------

    @property
    def db_host(self) -> str:
        return _env("DB_HOST", "localhost")

    @property
    def db_port(self) -> int:
        return _env_int("DB_PORT", 3306)

    @property
    def db_name(self) -> str:
        return _env("DB_NAME", "trend_master")

    @property
    def db_user(self) -> str:
        return _env("DB_USER", "root")

    @property
    def db_password(self) -> str:
        return _env("DB_PASSWORD", "")

    # ------------------------------------------------------------------
    # API
    # ------------------------------------------------------------------

    @property
    def api_url(self) -> str:
        return _env("API_URL", "https://etl-server.fly.dev")

    @property
    def api_key(self) -> Optional[str]:
        return _env("API_KEY") or None

    # ------------------------------------------------------------------
    # Security
    # ------------------------------------------------------------------

    @property
    def session_timeout_minutes(self) -> int:
        return _env_int("SESSION_TIMEOUT_MINUTES", 30)

    @property
    def max_login_attempts(self) -> int:
        return _env_int("MAX_LOGIN_ATTEMPTS", 5)

    @property
    def lockout_duration_minutes(self) -> int:
        return _env_int("LOCKOUT_DURATION_MINUTES", 15)

    # ------------------------------------------------------------------
    # Application
    # ------------------------------------------------------------------

    @property
    def environment(self) -> str:
        return _env("ENVIRONMENT", "development")

    @property
    def debug(self) -> bool:
        return _env_bool("DEBUG", False)

    @property
    def log_level(self) -> str:
        return _env("LOG_LEVEL", "INFO")


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

env_config = EnvConfig()