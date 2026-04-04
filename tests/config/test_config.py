"""
Tests for the config package.

Run with:  pytest test_config.py -v
"""

import pytest
from unittest.mock import patch
from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

class DictBackend:
    """Simple in-memory EnvBackend for test isolation."""
    def __init__(self, vars: dict):
        self._vars = vars

    def get(self, key: str):
        return self._vars.get(key)


# ---------------------------------------------------------------------------
# etl_config.py — DatabaseConfig
# ---------------------------------------------------------------------------

class TestDatabaseConfig:
    def _make(self, **kw):
        from config.etl_config import DatabaseConfig
        return DatabaseConfig(**kw)

    def test_defaults(self):
        cfg = self._make()
        assert cfg.host == "127.0.0.1"
        assert cfg.port == 3306
        assert cfg.pool_size == 5
        assert cfg.autocommit is False

    def test_valid_custom(self):
        cfg = self._make(host="db.example.com", user="admin", port=5432)
        assert cfg.port == 5432

    def test_invalid_port_zero(self):
        with pytest.raises(ValueError, match="port"):
            self._make(port=0)

    def test_invalid_port_too_high(self):
        with pytest.raises(ValueError, match="port"):
            self._make(port=99999)

    def test_invalid_pool_size(self):
        with pytest.raises(ValueError, match="pool_size"):
            self._make(pool_size=0)

    def test_empty_host_raises(self):
        with pytest.raises(ValueError, match="host"):
            self._make(host="")

    def test_empty_user_raises(self):
        with pytest.raises(ValueError, match="user"):
            self._make(user="")

    def test_to_dict_excludes_pooling(self):
        cfg = self._make()
        d = cfg.to_dict()
        # to_dict() should expose connection keys but NOT internal pool settings
        assert "host" in d
        assert "user" in d
        assert "password" in d

    def test_to_dict_includes_autocommit(self):
        cfg = self._make(autocommit=True)
        assert cfg.to_dict()["autocommit"] is True

    def test_frozen(self):
        cfg = self._make()
        with pytest.raises((AttributeError, TypeError)):
            cfg.host = "other"


# ---------------------------------------------------------------------------
# etl_config.py — APIConfig
# ---------------------------------------------------------------------------

class TestAPIConfig:
    def _make(self, **kw):
        from config.etl_config import APIConfig
        return APIConfig(**kw)

    def test_defaults(self):
        cfg = self._make()
        assert cfg.timeout == 30
        assert cfg.retries == 3

    def test_empty_base_url_raises(self):
        with pytest.raises(ValueError, match="base_url"):
            self._make(base_url="")

    def test_zero_timeout_raises(self):
        with pytest.raises(ValueError, match="timeout"):
            self._make(timeout=0)

    def test_negative_retries_raises(self):
        with pytest.raises(ValueError, match="retries"):
            self._make(retries=-1)

    def test_zero_concurrent_requests_raises(self):
        with pytest.raises(ValueError, match="max_concurrent_requests"):
            self._make(max_concurrent_requests=0)

    def test_get_headers_default(self):
        cfg = self._make()
        h = cfg.get_headers()
        assert h["Accept"] == "application/json"
        assert "X-API-Key" not in h
        assert "Authorization" not in h

    def test_get_headers_api_key(self):
        cfg = self._make(api_key="secret")
        assert cfg.get_headers()["X-API-Key"] == "secret"

    def test_get_headers_bearer(self):
        cfg = self._make(bearer_token="tok123")
        assert cfg.get_headers()["Authorization"] == "Bearer tok123"


# ---------------------------------------------------------------------------
# etl_config.py — ProcessingConfig
# ---------------------------------------------------------------------------

class TestProcessingConfig:
    def _make(self, **kw):
        from config.etl_config import ProcessingConfig
        return ProcessingConfig(**kw)

    def test_defaults(self):
        cfg = self._make()
        assert cfg.batch_size == 1000
        assert cfg.max_workers == 4

    def test_batch_size_exceeds_max(self):
        with pytest.raises(ValueError, match="batch_size"):
            self._make(batch_size=20000, max_batch_size=10000)

    def test_zero_batch_size_raises(self):
        with pytest.raises(ValueError, match="batch_size"):
            self._make(batch_size=0)

    def test_zero_chunk_size_raises(self):
        with pytest.raises(ValueError, match="chunk_size"):
            self._make(chunk_size=0)

    def test_zero_max_workers_raises(self):
        with pytest.raises(ValueError, match="max_workers"):
            self._make(max_workers=0)


# ---------------------------------------------------------------------------
# etl_config.py — LoggingConfig
# ---------------------------------------------------------------------------

class TestLoggingConfig:
    def _make(self, **kw):
        from config.etl_config import LoggingConfig
        return LoggingConfig(**kw)

    def test_defaults(self):
        cfg = self._make()
        assert cfg.level == "INFO"
        assert cfg.backup_count == 5

    def test_invalid_level_raises(self):
        with pytest.raises(ValueError, match="logging level"):
            self._make(level="VERBOSE")

    def test_invalid_console_level_raises(self):
        with pytest.raises(ValueError, match="console level"):
            self._make(console_level="TRACE")

    def test_zero_max_file_size_raises(self):
        with pytest.raises(ValueError, match="log file"):
            self._make(max_file_size=0)

    def test_negative_backup_count_raises(self):
        with pytest.raises(ValueError, match="log file"):
            self._make(backup_count=-1)

    def test_get_log_directory(self):
        cfg = self._make(log_file="logs/etl.log")
        assert cfg.get_log_directory() == Path("logs")

    def test_case_insensitive_level(self):
        # Level is stored as-is; validation uppercases before checking
        cfg = self._make(level="debug")
        assert cfg.level == "debug"


# ---------------------------------------------------------------------------
# etl_config.py — ApplicationConfig
# ---------------------------------------------------------------------------

class TestApplicationConfig:
    def _make(self, **kw):
        from config.etl_config import ApplicationConfig
        return ApplicationConfig(**kw)

    def test_defaults(self):
        cfg = self._make()
        assert cfg.name == "ETL Pipeline Manager"
        assert cfg.environment == "development"

    def test_empty_name_raises(self):
        with pytest.raises(ValueError, match="name"):
            self._make(name="")

    def test_empty_version_raises(self):
        with pytest.raises(ValueError, match="version"):
            self._make(version="")

    def test_empty_environment_raises(self):
        with pytest.raises(ValueError, match="environment"):
            self._make(environment="")

    def test_is_production(self):
        cfg = self._make(environment="production")
        assert cfg.is_production() is True
        assert cfg.is_development() is False

    def test_is_development(self):
        cfg = self._make(environment="development")
        assert cfg.is_development() is True
        assert cfg.is_production() is False

    def test_subdirs_derived_from_data_dir(self):
        cfg = self._make(data_dir=Path("/tmp/data"))
        assert cfg.csv_dir == Path("/tmp/data/CSV")
        assert cfg.api_dir == Path("/tmp/data/API")
        assert cfg.cache_dir == Path("/tmp/data/cache")

    def test_subdirs_overridable(self):
        cfg = self._make(
            data_dir=Path("/tmp/data"),
            csv_dir=Path("/custom/csv"),
        )
        assert cfg.csv_dir == Path("/custom/csv")
        assert cfg.api_dir == Path("/tmp/data/API")  # default still used


# ---------------------------------------------------------------------------
# etl_config.py — ETLConfig (composite root)
# ---------------------------------------------------------------------------

class TestETLConfig:
    def test_default_construction(self):
        from config.etl_config import ETLConfig
        cfg = ETLConfig()
        assert cfg.database.port == 3306
        assert cfg.api.timeout == 30
        assert cfg.processing.batch_size == 1000

    def test_from_dict_roundtrip(self):
        from config.etl_config import ETLConfig
        data = {
            "database": {"host": "mydb", "user": "admin"},
            "api": {"base_url": "https://example.com"},
            "processing": {},
            "logging": {},
            "application": {},
        }
        cfg = ETLConfig.from_dict(data)
        assert cfg.database.host == "mydb"
        assert cfg.api.base_url == "https://example.com"

    def test_get_summary_keys(self):
        from config.etl_config import ETLConfig
        summary = ETLConfig().get_summary()
        assert "application" in summary
        assert "database" in summary
        assert "api" in summary
        assert "processing" in summary

    def test_get_summary_no_secrets(self):
        """Password must not appear in the summary."""
        from config.etl_config import ETLConfig
        cfg = ETLConfig()
        summary = cfg.get_summary()
        assert "password" not in str(summary)


# ---------------------------------------------------------------------------
# database.py — MySQLConfig
# ---------------------------------------------------------------------------

class TestMySQLConfig:
    def _make(self, **kw):
        from config.database import MySQLConfig
        return MySQLConfig.create(**kw)

    def test_defaults(self):
        cfg = self._make()
        assert cfg.max_connections == 151
        assert "utf8mb4" in cfg.collation

    def test_sql_mode_deduplication(self):
        cfg = self._make(sql_mode="STRICT_TRANS_TABLES,STRICT_TRANS_TABLES")
        parts = cfg.sql_mode.split(",")
        assert len(parts) == len(set(parts))

    def test_charset_fallback(self):
        cfg = self._make(charset=None)
        assert cfg.charset == "utf8mb4"

    def test_charset_empty_string_fallback(self):
        cfg = self._make(charset="")
        assert cfg.charset == "utf8mb4"

    def test_mysql5_key_rejected(self):
        with pytest.raises(ValueError, match="query_cache_size"):
            self._make(query_cache_size=0)

    def test_collation_empty_raises(self):
        with pytest.raises(ValueError):
            from config.database import MySQLConfig
            MySQLConfig(collation="")

    def test_max_connections_zero_raises(self):
        with pytest.raises(ValueError, match="max_connections"):
            from config.database import MySQLConfig
            MySQLConfig(max_connections=0)

    def test_charset_collation_mismatch_raises(self):
        """utf8mb4 charset must not be paired with a non-utf8mb4 collation."""
        with pytest.raises(ValueError, match="Collation"):
            from config.database import MySQLConfig
            MySQLConfig(charset="utf8mb4", collation="latin1_swedish_ci")

    def test_to_dict_has_init_command(self):
        cfg = self._make()
        d = cfg.to_dict()
        assert "init_command" in d
        assert "sql_mode" in d["init_command"]

    def test_frozen(self):
        cfg = self._make()
        with pytest.raises((AttributeError, TypeError)):
            cfg.max_connections = 999


# ---------------------------------------------------------------------------
# database.py — preset factories
# ---------------------------------------------------------------------------

class TestMySQLPresets:
    def test_development_config(self):
        from config.database import get_mysql_development_config
        cfg = get_mysql_development_config()
        assert cfg.database == "store_manager_dev"
        assert cfg.pool_size == 3

    def test_production_config(self):
        from config.database import get_mysql_production_config
        cfg = get_mysql_production_config()
        assert cfg.pool_size == 20
        assert cfg.max_connections == 500

    def test_testing_config(self):
        from config.database import get_mysql_testing_config
        cfg = get_mysql_testing_config()
        assert cfg.autocommit is True
        assert cfg.raise_on_warnings is False


# ---------------------------------------------------------------------------
# api.py — RESTAPIConfig
# ---------------------------------------------------------------------------

class TestRESTAPIConfig:
    def _make(self, **kw):
        from config.api import RESTAPIConfig
        return RESTAPIConfig(**kw)

    def test_default_endpoints_populated(self):
        cfg = self._make()
        assert cfg.endpoints is not None
        assert "customers" in cfg.endpoints

    def test_instances_do_not_share_endpoints(self):
        """Mutable default bug — each instance must have its own dict."""
        from config.api import RESTAPIConfig
        a = RESTAPIConfig()
        b = RESTAPIConfig()
        a.endpoints["new_key"] = "/new"
        assert "new_key" not in b.endpoints

    def test_get_endpoint_url_named(self):
        cfg = self._make(base_url="https://api.example.com")
        url = cfg.get_endpoint_url("customers")
        assert url == "https://api.example.com/api/customers"

    def test_get_endpoint_url_unknown(self):
        cfg = self._make(base_url="https://api.example.com")
        url = cfg.get_endpoint_url("widgets")
        assert url == "https://api.example.com/widgets"

    def test_get_endpoint_url_no_double_slash(self):
        cfg = self._make(base_url="https://api.example.com/")
        url = cfg.get_endpoint_url("customers")
        assert "//" not in url.replace("https://", "")


# ---------------------------------------------------------------------------
# api.py — AsyncAPIConfig
# ---------------------------------------------------------------------------

class TestAsyncAPIConfig:
    def _make(self, **kw):
        from config.api import AsyncAPIConfig
        return AsyncAPIConfig(**kw)

    def test_defaults(self):
        cfg = self._make()
        assert cfg.connector_limit == 100
        assert cfg.enable_circuit_breaker is True

    def test_per_host_exceeds_total_raises(self):
        with pytest.raises(ValueError, match="connector_limit_per_host"):
            self._make(connector_limit=10, connector_limit_per_host=20)

    def test_exception_types_not_shared(self):
        """Mutable default bug guard."""
        a = self._make()
        b = self._make()
        a.expected_exception_types.append("custom.Error")
        assert "custom.Error" not in b.expected_exception_types


# ---------------------------------------------------------------------------
# api.py — preset factories
# ---------------------------------------------------------------------------

class TestAPIPresets:
    def test_etl_server_config(self):
        from config.api import get_etl_server_config
        cfg = get_etl_server_config()
        assert "etl-server" in cfg.base_url
        assert cfg.pagination_limit == 500

    def test_jsonplaceholder_config(self):
        from config.api import get_jsonplaceholder_config
        cfg = get_jsonplaceholder_config()
        assert "jsonplaceholder" in cfg.base_url
        assert "posts" in cfg.endpoints

    def test_local_dev_config(self):
        from config.api import get_local_dev_config
        cfg = get_local_dev_config()
        assert "localhost" in cfg.base_url

    def test_async_production_config(self):
        from config.api import get_async_production_config
        cfg = get_async_production_config()
        assert cfg.connector_limit_per_host <= cfg.connector_limit


# ---------------------------------------------------------------------------
# env_config.py — EnvConfig
# ---------------------------------------------------------------------------

class TestEnvConfig:
    def setup_method(self):
        from config import env_config as ec
        self._ec_module = ec

    def test_db_host_default(self):
        self._inject_backend({})
        from config.env_config import EnvConfig
        cfg = EnvConfig()
        assert cfg.db_host == "localhost"

    def test_db_host_from_env(self):
        self._inject_backend({"DB_HOST": "myserver"})
        from config.env_config import EnvConfig
        cfg = EnvConfig()
        assert cfg.db_host == "myserver"

    def test_db_port_int_coercion(self):
        self._inject_backend({"DB_PORT": "5432"})
        from config.env_config import EnvConfig
        cfg = EnvConfig()
        assert cfg.db_port == 5432

    def test_db_port_invalid_falls_back(self):
        self._inject_backend({"DB_PORT": "not_a_number"})
        from config.env_config import EnvConfig
        cfg = EnvConfig()
        assert cfg.db_port == 3306  # default

    def test_debug_bool_true(self):
        self._inject_backend({"DEBUG": "true"})
        from config.env_config import EnvConfig
        cfg = EnvConfig()
        assert cfg.debug is True

    def test_debug_bool_false(self):
        self._inject_backend({"DEBUG": "false"})
        from config.env_config import EnvConfig
        cfg = EnvConfig()
        assert cfg.debug is False

    def test_api_key_none_when_missing(self):
        self._inject_backend({})
        from config.env_config import EnvConfig
        cfg = EnvConfig()
        assert cfg.api_key is None

    def test_api_key_returned_when_present(self):
        self._inject_backend({"API_KEY": "abc123"})
        from config.env_config import EnvConfig
        cfg = EnvConfig()
        assert cfg.api_key == "abc123"


# ---------------------------------------------------------------------------
# profiles.py — load_config_for_environment
# ---------------------------------------------------------------------------

class TestLoadConfigForEnvironment:
    def setup_method(self):
        from config.profiles import set_env
        self._set_backend = set_env_backend

    def teardown_method(self):
        from config.profiles import set_env, DotEnvBackend
        set_env_backend(DotEnvBackend())

    def test_development(self):
        from config.profiles import load_config_for_environment
        cfg = load_config_for_environment("development")
        assert cfg.application.environment == "development"
        assert cfg.application.debug_mode is True

    def test_dev_alias(self):
        from config.profiles import load_config_for_environment
        cfg = load_config_for_environment("dev")
        assert cfg.application.environment == "development"

    def test_production(self):
        from config.profiles import load_config_for_environment
        cfg = load_config_for_environment("production")
        assert cfg.application.is_production() is True
        assert cfg.application.debug_mode is False

    def test_testing(self):
        from config.profiles import load_config_for_environment
        cfg = load_config_for_environment("testing")
        assert cfg.database.autocommit is True

    def test_staging(self):
        from config.profiles import load_config_for_environment
        cfg = load_config_for_environment("staging")
        assert cfg.processing.strict_validation is True

    def test_unknown_environment_raises(self):
        from config.profiles import load_config_for_environment
        with pytest.raises(ValueError, match="Unknown environment"):
            load_config_for_environment("unknown_env")

    def test_env_var_used_when_no_arg(self):
        self._set_backend(DictBackend({"ENVIRONMENT": "testing"}))
        from config.profiles import load_config_for_environment
        cfg = load_config_for_environment()
        assert cfg.application.environment == "testing"

    def test_invalid_env_var_raises(self):
        self._set_backend(DictBackend({"ENVIRONMENT": "garbage"}))
        from config.profiles import load_config_for_environment
        with pytest.raises(ValueError, match="ENVIRONMENT"):
            load_config_for_environment()

    def test_defaults_to_development_when_no_env_var(self):
        self._set_backend(DictBackend({}))
        from config.profiles import load_config_for_environment
        cfg = load_config_for_environment()
        assert cfg.application.environment == "development"


# ---------------------------------------------------------------------------
# profiles.py — is_* helpers
# ---------------------------------------------------------------------------

class TestEnvironmentHelpers:
    def setup_method(self):
        from config.env_config import set_env_backend
        self._set_backend = set_env_backend

    def teardown_method(self):
        from config.env_config import set_env_backend, DotEnvBackend
        set_env_backend(DotEnvBackend())

    def test_is_production_true(self):
        self._set_backend(DictBackend({"ENVIRONMENT": "production"}))
        from config.profiles import is_production
        assert is_production() is True

    def test_is_production_false(self):
        self._set_backend(DictBackend({"ENVIRONMENT": "development"}))
        from config.profiles import is_production
        assert is_production() is False

    def test_is_development_true(self):
        self._set_backend(DictBackend({"ENVIRONMENT": "dev"}))
        from config.profiles import is_development
        assert is_development() is True

    def test_is_testing_true(self):
        self._set_backend(DictBackend({"ENVIRONMENT": "test"}))
        from config.profiles import is_testing
        assert is_testing() is True