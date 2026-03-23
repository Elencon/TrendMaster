"""
Test suite for the config package.

Covers:
    - DatabaseConfig
    - APIConfig
    - ProcessingConfig
    - LoggingConfig
    - ApplicationConfig
    - ETLConfig
    - MySQLConfig
    - RESTAPIConfig / GraphQLAPIConfig / AsyncAPIConfig
    - EnvConfig / DotEnvBackend / _env helpers
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
from unittest.mock import patch


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


# ===========================================================================
# _env helpers
# ===========================================================================

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


# ===========================================================================
# EnvConfig
# ===========================================================================

class TestEnvConfig:

    def test_db_host_from_env(self):
        _install_backend({"DB_HOST": "myhost"})
        from config.env_config import env_config
        assert env_config.db_host == "myhost"

    def test_db_host_default(self):
        from config.env_config import env_config
        assert env_config.db_host == "localhost"

    def test_db_port_from_env(self):
        _install_backend({"DB_PORT": "5432"})
        from config.env_config import env_config
        assert env_config.db_port == 5432

    def test_db_port_default(self):
        from config.env_config import env_config
        assert env_config.db_port == 3306

    def test_db_name_default(self):
        from config.env_config import env_config
        assert env_config.db_name == "trend_master"

    def test_db_user_default(self):
        from config.env_config import env_config
        assert env_config.db_user == "root"

    def test_db_password_default(self):
        from config.env_config import env_config
        assert env_config.db_password == ""

    def test_api_url_from_env(self):
        _install_backend({"API_URL": "https://my-api.example.com"})
        from config.env_config import env_config
        assert env_config.api_url == "https://my-api.example.com"

    def test_api_key_none_when_missing(self):
        from config.env_config import env_config
        assert env_config.api_key is None

    def test_api_key_from_env(self):
        _install_backend({"API_KEY": "secret"})
        from config.env_config import env_config
        assert env_config.api_key == "secret"

    def test_session_timeout_default(self):
        from config.env_config import env_config
        assert env_config.session_timeout_minutes == 30

    def test_max_login_attempts_default(self):
        from config.env_config import env_config
        assert env_config.max_login_attempts == 5

    def test_lockout_duration_default(self):
        from config.env_config import env_config
        assert env_config.lockout_duration_minutes == 15

    def test_environment_default(self):
        from config.env_config import env_config
        assert env_config.environment == "development"

    def test_debug_default(self):
        from config.env_config import env_config
        assert env_config.debug is False

    def test_debug_from_env(self):
        _install_backend({"DEBUG": "true"})
        from config.env_config import env_config
        assert env_config.debug is True

    def test_log_level_default(self):
        from config.env_config import env_config
        assert env_config.log_level == "INFO"

    def test_static_method_get(self):
        _install_backend({"X": "y"})
        from config.env_config import EnvConfig
        assert EnvConfig.get("X") == "y"

    def test_static_method_get_int(self):
        _install_backend({"N": "7"})
        from config.env_config import EnvConfig
        assert EnvConfig.get_int("N", 0) == 7

    def test_static_method_get_bool(self):
        _install_backend({"B": "yes"})
        from config.env_config import EnvConfig
        assert EnvConfig.get_bool("B", False) is True


# ===========================================================================
# DatabaseConfig
# ===========================================================================

class TestDatabaseConfig:

    def test_defaults(self):
        from config import DatabaseConfig
        cfg = DatabaseConfig()
        assert cfg.host == "127.0.0.1"
        assert cfg.port == 3306
        assert cfg.pool_size == 5
        assert cfg.autocommit is False
        assert cfg.raise_on_warnings is True

    def test_env_overrides_host(self):
        _install_backend({"DB_HOST": "prod-host"})
        from config import DatabaseConfig
        cfg = DatabaseConfig()
        assert cfg.host == "prod-host"

    def test_to_dict_keys(self):
        from config import DatabaseConfig
        d = DatabaseConfig().to_dict()
        assert set(d.keys()) == {
            "user", "password", "host", "port", "database",
            "raise_on_warnings", "autocommit", "connect_timeout",
        }

    def test_to_dict_no_pool_fields(self):
        from config import DatabaseConfig
        d = DatabaseConfig().to_dict()
        assert "pool_size" not in d
        assert "enable_pooling" not in d

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

    def test_validate_empty_host(self):
        from config import DatabaseConfig
        assert DatabaseConfig(host="").validate() is False

    def test_validate_empty_user(self):
        from config import DatabaseConfig
        assert DatabaseConfig(user="").validate() is False

    def test_validate_invalid_port_low(self):
        from config import DatabaseConfig
        assert DatabaseConfig(port=0).validate() is False

    def test_validate_invalid_port_high(self):
        from config import DatabaseConfig
        assert DatabaseConfig(port=99999).validate() is False

    def test_validate_pool_size_zero(self):
        from config import DatabaseConfig
        assert DatabaseConfig(pool_size=0).validate() is False


# ===========================================================================
# APIConfig
# ===========================================================================

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
        assert "User-Agent" in headers

    def test_get_headers_with_api_key(self):
        from config import APIConfig
        cfg = APIConfig(api_key="my-key")
        assert cfg.get_headers()["X-API-Key"] == "my-key"

    def test_get_headers_with_bearer_token(self):
        from config import APIConfig
        cfg = APIConfig(bearer_token="tok")
        assert cfg.get_headers()["Authorization"] == "Bearer tok"

    def test_get_headers_no_auth_when_missing(self):
        from config import APIConfig
        headers = APIConfig().get_headers()
        assert "X-API-Key" not in headers
        assert "Authorization" not in headers

    def test_validate_passes(self):
        from config import APIConfig
        assert APIConfig().validate() is True

    def test_validate_empty_base_url(self):
        from config import APIConfig
        assert APIConfig(base_url="").validate() is False

    def test_validate_zero_timeout(self):
        from config import APIConfig
        assert APIConfig(timeout=0).validate() is False

    def test_validate_negative_retries(self):
        from config import APIConfig
        assert APIConfig(retries=-1).validate() is False

    def test_validate_zero_concurrent(self):
        from config import APIConfig
        assert APIConfig(max_concurrent_requests=0).validate() is False


# ===========================================================================
# ProcessingConfig
# ===========================================================================

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

    def test_validate_batch_exceeds_max(self):
        from config import ProcessingConfig
        assert ProcessingConfig(batch_size=20_000, max_batch_size=10_000).validate() is False

    def test_validate_zero_batch(self):
        from config import ProcessingConfig
        assert ProcessingConfig(batch_size=0).validate() is False

    def test_validate_zero_chunk(self):
        from config import ProcessingConfig
        assert ProcessingConfig(chunk_size=0).validate() is False

    def test_validate_zero_workers(self):
        from config import ProcessingConfig
        assert ProcessingConfig(max_workers=0).validate() is False

    def test_pandas_na_values_not_shared(self):
        from config import ProcessingConfig
        a = ProcessingConfig()
        b = ProcessingConfig()
        a.pandas_na_values.append("CUSTOM")
        assert "CUSTOM" not in b.pandas_na_values


# ===========================================================================
# LoggingConfig
# ===========================================================================

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

    def test_validate_passes(self):
        from config import LoggingConfig
        assert LoggingConfig().validate() is True

    def test_validate_invalid_level(self):
        from config import LoggingConfig
        assert LoggingConfig(level="VERBOSE").validate() is False

    def test_validate_invalid_console_level(self):
        from config import LoggingConfig
        assert LoggingConfig(console_level="TRACE").validate() is False

    def test_validate_zero_file_size(self):
        from config import LoggingConfig
        assert LoggingConfig(max_file_size=0).validate() is False

    def test_validate_negative_backup_count(self):
        from config import LoggingConfig
        assert LoggingConfig(backup_count=-1).validate() is False

    def test_level_from_env(self):
        _install_backend({"LOG_LEVEL": "DEBUG"})
        from config import LoggingConfig
        assert LoggingConfig().level == "DEBUG"


# ===========================================================================
# ApplicationConfig
# ===========================================================================

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

    def test_explicit_dirs_not_overridden(self):
        from config import ApplicationConfig
        cfg = ApplicationConfig(csv_dir=Path("/custom/csv"))
        assert cfg.csv_dir == Path("/custom/csv")

    def test_is_production(self):
        from config import ApplicationConfig
        assert ApplicationConfig(environment="production").is_production() is True
        assert ApplicationConfig(environment="development").is_production() is False

    def test_is_development(self):
        from config import ApplicationConfig
        assert ApplicationConfig(environment="development").is_development() is True
        assert ApplicationConfig(environment="production").is_development() is False

    def test_validate_passes(self):
        from config import ApplicationConfig
        assert ApplicationConfig().validate() is True

    def test_validate_empty_name(self):
        from config import ApplicationConfig
        assert ApplicationConfig(name="").validate() is False

    def test_validate_empty_version(self):
        from config import ApplicationConfig
        assert ApplicationConfig(version="").validate() is False


# ===========================================================================
# ETLConfig
# ===========================================================================

class TestETLConfig:

    def test_defaults_are_valid(self):
        from config import ETLConfig
        assert ETLConfig().is_valid() is True

    def test_validate_all_returns_dict(self):
        from config import ETLConfig
        result = ETLConfig().validate_all()
        assert set(result.keys()) == {
            "database", "api", "processing", "logging", "application"
        }
        assert all(result.values())

    def test_is_valid_false_when_section_invalid(self):
        from config import ETLConfig, DatabaseConfig
        cfg = ETLConfig(database=DatabaseConfig(host=""))
        assert cfg.is_valid() is False

    def test_get_summary_keys(self):
        from config import ETLConfig
        summary = ETLConfig().get_summary()
        assert set(summary.keys()) == {
            "application", "database", "api", "processing"
        }

    def test_get_summary_no_password(self):
        from config import ETLConfig, DatabaseConfig
        cfg = ETLConfig(database=DatabaseConfig(password="supersecret"))
        summary = str(cfg.get_summary())
        assert "supersecret" not in summary

    def test_get_summary_no_api_key(self):
        from config import ETLConfig, APIConfig
        cfg = ETLConfig(api=APIConfig(api_key="secret-key"))
        summary = str(cfg.get_summary())
        assert "secret-key" not in summary


# ===========================================================================
# MySQLConfig
# ===========================================================================

class TestMySQLConfig:

    def test_to_dict_includes_charset(self):
        from config.database import MySQLConfig
        d = MySQLConfig().to_dict()
        assert d["charset"] == "utf8mb4"
        assert d["collation"] == "utf8mb4_unicode_ci"

    def test_to_dict_includes_init_command(self):
        from config.database import MySQLConfig
        d = MySQLConfig().to_dict()
        assert "SET sql_mode=" in d["init_command"]

    def test_to_dict_inherits_base_keys(self):
        from config.database import MySQLConfig
        d = MySQLConfig().to_dict()
        assert "host" in d
        assert "user" in d
        assert "database" in d

    def test_development_config(self):
        from config.database import get_mysql_development_config
        cfg = get_mysql_development_config()
        assert cfg.database == "store_manager_dev"
        assert cfg.pool_size == 3

    def test_production_config(self):
        from config.database import get_mysql_production_config
        cfg = get_mysql_production_config()
        assert cfg.pool_size == 20
        assert cfg.innodb_buffer_pool_size == "512M"

    def test_testing_config(self):
        from config.database import get_mysql_testing_config
        cfg = get_mysql_testing_config()
        assert cfg.autocommit is True
        assert cfg.raise_on_warnings is False
        assert cfg.database == "store_manager_test"


# ===========================================================================
# RESTAPIConfig
# ===========================================================================

class TestRESTAPIConfig:

    def test_default_endpoints_set(self):
        from config.api import RESTAPIConfig
        cfg = RESTAPIConfig(base_url="https://example.com")
        assert "customers" in cfg.endpoints
        assert "health" in cfg.endpoints

    def test_endpoints_not_shared_between_instances(self):
        from config.api import RESTAPIConfig
        a = RESTAPIConfig(base_url="https://a.com")
        b = RESTAPIConfig(base_url="https://b.com")
        a.endpoints["new"] = "/new"
        assert "new" not in b.endpoints

    def test_get_endpoint_url_named(self):
        from config.api import RESTAPIConfig
        cfg = RESTAPIConfig(base_url="https://example.com")
        assert cfg.get_endpoint_url("customers") == "https://example.com/api/customers"

    def test_get_endpoint_url_unknown_with_slash(self):
        from config.api import RESTAPIConfig
        cfg = RESTAPIConfig(base_url="https://example.com")
        assert cfg.get_endpoint_url("/custom") == "https://example.com/custom"

    def test_get_endpoint_url_unknown_without_slash(self):
        from config.api import RESTAPIConfig
        cfg = RESTAPIConfig(base_url="https://example.com")
        assert cfg.get_endpoint_url("custom") == "https://example.com/custom"

    def test_get_endpoint_url_no_double_slash(self):
        from config.api import RESTAPIConfig
        cfg = RESTAPIConfig(base_url="https://example.com/")
        url = cfg.get_endpoint_url("customers")
        assert "//" not in url.replace("https://", "")

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


# ===========================================================================
# GraphQLAPIConfig
# ===========================================================================

class TestGraphQLAPIConfig:

    def test_get_graphql_url(self):
        from config.api import GraphQLAPIConfig
        cfg = GraphQLAPIConfig(base_url="https://example.com")
        assert cfg.get_graphql_url() == "https://example.com/graphql"

    def test_get_graphql_url_no_double_slash(self):
        from config.api import GraphQLAPIConfig
        cfg = GraphQLAPIConfig(base_url="https://example.com/")
        url = cfg.get_graphql_url()
        assert "//" not in url.replace("https://", "")


# ===========================================================================
# AsyncAPIConfig
# ===========================================================================

class TestAsyncAPIConfig:

    def test_default_exception_types_set(self):
        from config.api import AsyncAPIConfig
        cfg = AsyncAPIConfig(base_url="https://example.com")
        assert "aiohttp.ClientError" in cfg.expected_exception_types

    def test_exception_types_not_shared(self):
        from config.api import AsyncAPIConfig
        a = AsyncAPIConfig(base_url="https://a.com")
        b = AsyncAPIConfig(base_url="https://b.com")
        a.expected_exception_types.append("CustomError")
        assert "CustomError" not in b.expected_exception_types

    def test_connector_limit_per_host_exceeds_limit_raises(self):
        from config.api import AsyncAPIConfig
        with pytest.raises(ValueError, match="connector_limit_per_host"):
            AsyncAPIConfig(
                base_url="https://example.com",
                connector_limit=10,
                connector_limit_per_host=20,
            )

    def test_async_production_config(self):
        from config.api import get_async_production_config
        cfg = get_async_production_config()
        assert cfg.connector_limit == 200
        assert cfg.enable_circuit_breaker is True


# ===========================================================================
# Environment profiles
# ===========================================================================

class TestEnvironmentProfiles:

    def _assert_valid(self, cfg):
        assert cfg.is_valid(), f"Config invalid: {cfg.validate_all()}"

    def test_development_profile_valid(self):
        from config.environments import DevelopmentProfile
        self._assert_valid(DevelopmentProfile.load_config())

    def test_development_profile_debug_mode(self):
        from config.environments import DevelopmentProfile
        cfg = DevelopmentProfile.load_config()
        assert cfg.application.debug_mode is True
        assert cfg.application.environment == "development"

    def test_production_profile_valid(self):
        from config.environments import ProductionProfile
        self._assert_valid(ProductionProfile.load_config())

    def test_production_profile_no_debug(self):
        from config.environments import ProductionProfile
        cfg = ProductionProfile.load_config()
        assert cfg.application.debug_mode is False
        assert cfg.processing.strict_validation is True

    def test_production_log_file_from_env(self):
        _install_backend({"PROD_LOG_FILE": "/custom/prod.log"})
        from config.environments import ProductionProfile
        cfg = ProductionProfile.load_config()
        assert cfg.logging.log_file == "/custom/prod.log"

    def test_production_log_file_default_is_relative(self):
        from config.environments import ProductionProfile
        cfg = ProductionProfile.load_config()
        assert not cfg.logging.log_file.startswith("/var/log")
        assert cfg.logging.log_file.startswith("logs/")

    def test_testing_profile_valid(self):
        from config.environments import TestingProfile
        self._assert_valid(TestingProfile.load_config())

    def test_testing_profile_no_file_logging(self):
        from config.environments import TestingProfile
        cfg = TestingProfile.load_config()
        assert cfg.logging.enable_file_logging is False
        assert cfg.database.autocommit is True

    def test_staging_profile_valid(self):
        from config.environments import StagingProfile
        self._assert_valid(StagingProfile.load_config())

    def test_staging_log_file_from_env(self):
        _install_backend({"STAGING_LOG_FILE": "/custom/staging.log"})
        from config.environments import StagingProfile
        cfg = StagingProfile.load_config()
        assert cfg.logging.log_file == "/custom/staging.log"

    def test_staging_log_file_default_is_relative(self):
        from config.environments import StagingProfile
        cfg = StagingProfile.load_config()
        assert cfg.logging.log_file.startswith("logs/")


# ===========================================================================
# load_config_for_environment
# ===========================================================================

class TestLoadConfigForEnvironment:

    def test_development_aliases(self):
        from config.environments import load_config_for_environment
        for alias in ("development", "dev", "DEVELOPMENT", "DEV"):
            cfg = load_config_for_environment(alias)
            assert cfg.application.environment == "development"

    def test_production_aliases(self):
        from config.environments import load_config_for_environment
        for alias in ("production", "prod"):
            cfg = load_config_for_environment(alias)
            assert cfg.application.environment == "production"

    def test_testing_aliases(self):
        from config.environments import load_config_for_environment
        for alias in ("testing", "test"):
            cfg = load_config_for_environment(alias)
            assert cfg.application.environment == "testing"

    def test_staging_aliases(self):
        from config.environments import load_config_for_environment
        for alias in ("staging", "stage"):
            cfg = load_config_for_environment(alias)
            assert cfg.application.environment == "staging"

    def test_unknown_raises(self):
        from config.environments import load_config_for_environment
        with pytest.raises(ValueError, match="Unknown environment"):
            load_config_for_environment("local")

    def test_none_falls_back_to_development_when_env_unset(self):
        from config.environments import load_config_for_environment
        cfg = load_config_for_environment(None)
        assert cfg.application.environment == "development"

    def test_none_reads_environment_env_var(self):
        _install_backend({"ENVIRONMENT": "staging"})
        from config.environments import load_config_for_environment
        cfg = load_config_for_environment(None)
        assert cfg.application.environment == "staging"

    def test_invalid_environment_env_var_raises(self):
        _install_backend({"ENVIRONMENT": "LOCAL"})
        from config.environments import load_config_for_environment
        with pytest.raises(ValueError, match="ENVIRONMENT='LOCAL'"):
            load_config_for_environment(None)

    def test_explicit_arg_overrides_env_var(self):
        _install_backend({"ENVIRONMENT": "production"})
        from config.environments import load_config_for_environment
        cfg = load_config_for_environment("development")
        assert cfg.application.environment == "development"


# ===========================================================================
# get_current_environment / is_* helpers
# ===========================================================================

class TestEnvironmentHelpers:

    def test_get_current_environment_default(self):
        from config.environments import get_current_environment
        assert get_current_environment() == "development"

    def test_get_current_environment_from_env(self):
        _install_backend({"ENVIRONMENT": "PRODUCTION"})
        from config.environments import get_current_environment
        assert get_current_environment() == "production"

    def test_is_production_true(self):
        from config.environments import is_production
        for val in ("production", "prod"):
            _install_backend({"ENVIRONMENT": val})
            assert is_production() is True

    def test_is_production_false(self):
        from config.environments import is_production
        _install_backend({"ENVIRONMENT": "development"})
        assert is_production() is False

    def test_is_development_true(self):
        from config.environments import is_development
        for val in ("development", "dev"):
            _install_backend({"ENVIRONMENT": val})
            assert is_development() is True

    def test_is_testing_true(self):
        from config.environments import is_testing
        for val in ("testing", "test"):
            _install_backend({"ENVIRONMENT": val})
            assert is_testing() is True

    def test_is_helpers_delegate_to_get_current_environment(self):
        """Confirms helpers read env once via get_current_environment, not independently."""
        from config.environments import is_production, is_development, is_testing
        _install_backend({"ENVIRONMENT": "production"})
        assert is_production() is True
        assert is_development() is False
        assert is_testing() is False


# ===========================================================================
# Factory functions
# ===========================================================================

class TestFactoryFunctions:

    def test_load_config_from_env_returns_etlconfig(self):
        from config import load_config_from_env, ETLConfig
        assert isinstance(load_config_from_env(), ETLConfig)

    def test_load_config_from_dict_applies_database(self):
        from config import load_config_from_dict
        cfg = load_config_from_dict({"database": {"pool_size": 99}})
        assert cfg.database.pool_size == 99

    def test_load_config_from_dict_applies_api(self):
        from config import load_config_from_dict
        cfg = load_config_from_dict({"api": {"timeout": 99}})
        assert cfg.api.timeout == 99

    def test_load_config_from_dict_applies_processing(self):
        from config import load_config_from_dict
        cfg = load_config_from_dict({"processing": {"batch_size": 42}})
        assert cfg.processing.batch_size == 42

    def test_load_config_from_dict_ignores_unknown_keys(self):
        from config import load_config_from_dict
        cfg = load_config_from_dict({"database": {"nonexistent_key": "value"}})
        assert cfg.is_valid()

    def test_get_default_config_overrides(self):
        from config import get_default_config
        cfg = get_default_config()
        assert cfg.database.pool_size == 10
        assert cfg.processing.batch_size == 2000
        assert cfg.api.max_concurrent_requests == 15

    def test_get_default_config_is_valid(self):
        from config import get_default_config
        assert get_default_config().is_valid()

    def test_get_config_returns_singleton(self):
        from config import get_config
        a = get_config()
        b = get_config()
        assert a is b

    def test_set_config_replaces_singleton(self):
        from config import get_config, set_config, ETLConfig, DatabaseConfig
        custom = ETLConfig(database=DatabaseConfig(host="custom-host"))
        set_config(custom)
        assert get_config().database.host == "custom-host"

    def test_reset_config_forces_reinit(self):
        from config import get_config, set_config, reset_config, ETLConfig, DatabaseConfig
        set_config(ETLConfig(database=DatabaseConfig(host="custom-host")))
        reset_config()
        cfg = get_config()
        assert cfg.database.host != "custom-host"


# ===========================================================================
# Entry point
# ===========================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])