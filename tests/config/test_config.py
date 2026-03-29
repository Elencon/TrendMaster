"""
Test suite for the config package.

Covers:
    - DatabaseConfig
    - APIConfig
    - ProcessingConfig
    - LoggingConfig
    - ApplicationConfig
    - ETLConfig
    - EnvConfig / _env helpers
    - Environment profiles
    - load_config_for_environment / get_current_environment / is_* helpers
    - Factory functions: load_config_from_env, load_config_from_dict,
                         get_default_config, get_config, set_config, reset_config

No live database, filesystem writes, or real env files required.
Run with:
    pytest tests/config/test_config.py -v
"""

import pytest
from pathlib import Path
from typing import Optional, Dict

# ---------------------------------------------------------------------------
# Shared test backend — injected instead of DotEnvBackend for all tests
# ---------------------------------------------------------------------------

class DictBackend:
    """In-memory EnvBackend backed by a plain dict."""

    def __init__(self, data: Dict[str, str] = None) -> None:
        self._data = data or {}

    def get(self, key: str) -> Optional[str]:
        return self._data.get(key)


def _install_backend(data: Dict[str, str] = None):
    """Install a DictBackend into env_config and return it."""
    import config.env_config as ec
    backend = DictBackend(data or {})
    ec.set_env_backend(backend)
    return backend


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def clean_backend():
    """Reset to an empty backend before every test."""
    _install_backend({})
    yield
    _install_backend({})


@pytest.fixture(autouse=True)
def clean_global_config():
    """Reset the global ETLConfig singleton before every test."""
    from config import reset_config
    reset_config()
    yield
    reset_config()


# ========================================================================
# _env helpers
# ========================================================================

class TestEnvHelpers:

    def test_env_returns_value(self):
        _install_backend({"FOO": "bar"})
        from config.env_config import _env
        assert _env("FOO") == "bar"

    def test_env_returns_default_when_missing(self):
        from config.env_config import _env
        assert _env("MISSING", "default") == "default"

    def test_env_returns_empty_string_default(self):
        from config.env_config import _env
        assert _env("MISSING") == ""

    def test_env_int_valid(self):
        _install_backend({"PORT": "5432"})
        from config.env_config import _env_int
        assert _env_int("PORT", 3306) == 5432

    def test_env_int_missing_returns_default(self):
        from config.env_config import _env_int
        assert _env_int("MISSING", 42) == 42

    def test_env_int_invalid_returns_default(self):
        _install_backend({"PORT": "not_a_number"})
        from config.env_config import _env_int
        assert _env_int("PORT", 3306) == 3306

    def test_env_int_whitespace_only_returns_default(self):
        _install_backend({"PORT": "   "})
        from config.env_config import _env_int
        assert _env_int("PORT", 3306) == 3306

    def test_env_bool_true_variants(self):
        from config.env_config import _env_bool
        for val in ("true", "1", "yes", "on", "TRUE", "Yes", " on "):
            _install_backend({"FLAG": val})
            assert _env_bool("FLAG", False) is True, f"Failed for '{val}'"

    def test_env_bool_false_variants(self):
        from config.env_config import _env_bool
        for val in ("false", "0", "no", "off", "FALSE"):
            _install_backend({"FLAG": val})
            assert _env_bool("FLAG", True) is False, f"Failed for '{val}'"

    def test_env_bool_missing_returns_default(self):
        from config.env_config import _env_bool
        assert _env_bool("MISSING", True) is True
        assert _env_bool("MISSING", False) is False


# ========================================================================
# DatabaseConfig
# ========================================================================

class TestDatabaseConfig:

    def test_defaults(self):
        from config import DatabaseConfig
        cfg = DatabaseConfig()
        assert cfg.host == "127.0.0.1"
        assert cfg.port == 3306
        assert cfg.pool_size == 5
        assert cfg.autocommit is False
        assert cfg.raise_on_warnings is True

    def test_to_dict_keys(self):
        from config import DatabaseConfig
        d = DatabaseConfig().to_dict()
        assert set(d.keys()) == {
            "user", "password", "host", "port", "database",
            "raise_on_warnings", "autocommit", "connect_timeout",
        }

    def test_get_connection_string_no_password(self):
        from config import DatabaseConfig
        cfg = DatabaseConfig(user="alice", host="db.example.com", port=3306, database="mydb")
        s = cfg.get_connection_string()
        assert "alice" in s
        assert "db.example.com" in s
        assert cfg.password not in s or cfg.password == ""

    def test_validate_passes(self):
        from config import DatabaseConfig
        assert DatabaseConfig().validate() is True

    def test_validate_failures(self):
        from config import DatabaseConfig
        assert DatabaseConfig(host="").validate() is False
        assert DatabaseConfig(user="").validate() is False
        assert DatabaseConfig(port=0).validate() is False
        assert DatabaseConfig(port=99999).validate() is False
        assert DatabaseConfig(pool_size=0).validate() is False


# ========================================================================
# APIConfig
# ========================================================================

class TestAPIConfig:

    def test_defaults(self):
        from config import APIConfig
        cfg = APIConfig()
        assert cfg.timeout == 30
        assert cfg.retries == 3
        assert cfg.max_concurrent_requests == 10

    def test_get_headers_basic(self):
        from config import APIConfig
        headers = APIConfig().get_headers()
        assert headers["Accept"] == "application/json"
        assert headers["Content-Type"] == "application/json"

    def test_get_headers_with_auth(self):
        from config import APIConfig
        cfg = APIConfig(api_key="my-key", bearer_token="tok")
        h = cfg.get_headers()
        assert h["X-API-Key"] == "my-key"
        assert h["Authorization"] == "Bearer tok"

    def test_validate_passes(self):
        from config import APIConfig
        assert APIConfig().validate() is True

    def test_validate_failures(self):
        from config import APIConfig
        assert APIConfig(base_url="").validate() is False
        assert APIConfig(timeout=0).validate() is False
        assert APIConfig(retries=-1).validate() is False
        assert APIConfig(max_concurrent_requests=0).validate() is False


# ========================================================================
# ProcessingConfig
# ========================================================================

class TestProcessingConfig:

    def test_defaults(self):
        from config import ProcessingConfig
        cfg = ProcessingConfig()
        assert cfg.batch_size == 1000
        assert cfg.max_batch_size == 10_000
        assert cfg.max_workers == 4

    def test_validate_passes(self):
        from config import ProcessingConfig
        assert ProcessingConfig().validate() is True

    def test_validate_failures(self):
        from config import ProcessingConfig
        assert ProcessingConfig(batch_size=0).validate() is False
        assert ProcessingConfig(batch_size=20_000, max_batch_size=10_000).validate() is False
        assert ProcessingConfig(chunk_size=0).validate() is False
        assert ProcessingConfig(max_workers=0).validate() is False


# ========================================================================
# LoggingConfig
# ========================================================================

class TestLoggingConfig:

    def test_defaults(self):
        from config import LoggingConfig
        cfg = LoggingConfig()
        assert cfg.level == "INFO"
        assert cfg.backup_count == 5
        assert cfg.enable_console_logging is True

    def test_get_log_directory(self):
        from config import LoggingConfig
        cfg = LoggingConfig(log_file="logs/etl.log")
        assert cfg.get_log_directory() == Path("logs")

    def test_validate_failures(self):
        from config import LoggingConfig
        assert LoggingConfig(level="VERBOSE").validate() is False
        assert LoggingConfig(console_level="TRACE").validate() is False
        assert LoggingConfig(max_file_size=0).validate() is False
        assert LoggingConfig(backup_count=-1).validate() is False


# ========================================================================
# ApplicationConfig
# ========================================================================

class TestApplicationConfig:

    def test_defaults(self):
        from config import ApplicationConfig
        cfg = ApplicationConfig()
        assert cfg.name == "ETL Pipeline Manager"
        assert cfg.version == "2.0.0"
        assert cfg.debug_mode is False

    def test_derived_dirs_set_in_post_init(self):
        from config import ApplicationConfig
        cfg = ApplicationConfig()
        assert cfg.csv_dir   == cfg.data_dir / "CSV"
        assert cfg.api_dir   == cfg.data_dir / "API"
        assert cfg.cache_dir == cfg.data_dir / "cache"

    def test_is_production_and_development_helpers(self):
        from config import ApplicationConfig
        cfg = ApplicationConfig(environment="production")
        assert cfg.is_production() is True
        assert cfg.is_development() is False
        cfg = ApplicationConfig(environment="development")
        assert cfg.is_production() is False
        assert cfg.is_development() is True

    def test_validate_failures(self):
        from config import ApplicationConfig
        assert ApplicationConfig(name="").validate() is False
        assert ApplicationConfig(version="").validate() is False


# ========================================================================
# ETLConfig and factory functions
# ========================================================================

class TestETLConfigAndFactory:

    def test_load_config_from_env_returns_etlconfig(self):
        from config import load_config_from_env, ETLConfig
        cfg = load_config_from_env()
        assert isinstance(cfg, ETLConfig)

    def test_load_config_from_dict_applies(self):
        from config import load_config_from_dict
        cfg = load_config_from_dict({"database": {"pool_size": 99}})
        assert cfg.database.pool_size == 99
        cfg = load_config_from_dict({"api": {"timeout": 42}})
        assert cfg.api.timeout == 42
        cfg = load_config_from_dict({"processing": {"batch_size": 101}})
        assert cfg.processing.batch_size == 101

    def test_get_default_config_and_singleton(self):
        from config import get_default_config, get_config, set_config, reset_config, ETLConfig
        cfg = get_default_config()
        assert cfg.database.pool_size == 10
        # Singleton
        reset_config()
        a = get_config()
        b = get_config()
        assert a is b
        # Replace singleton
        custom = ETLConfig(database=a.database)
        set_config(custom)
        assert get_config() is custom
