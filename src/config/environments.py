r"""
C:\Economy\Invest\TrendMaster\src\config\environments.py
Environment-specific configuration profiles.
"""

from pathlib import Path
from typing import Dict

from . import (
    APIConfig,
    ApplicationConfig,
    DatabaseConfig,
    ETLConfig,
    LoggingConfig,
    ProcessingConfig,
)
from .env_config import _backend, _env, _env_int

# ---------------------------------------------------------------------------
# Base profile
# ---------------------------------------------------------------------------

class ConfigProfile:
    """Base class for environment-specific configuration profiles."""

    @staticmethod
    def load_config() -> ETLConfig:
        """Return an ETLConfig for this environment."""
        raise NotImplementedError("Subclasses must implement load_config()")


# ---------------------------------------------------------------------------
# Environment profiles
# ---------------------------------------------------------------------------

class DevelopmentProfile(ConfigProfile):
    """Development environment — permissive settings, verbose logging."""

    @staticmethod
    def load_config() -> ETLConfig:
        return ETLConfig(
            database=DatabaseConfig(
                host="localhost",
                port=3306,
                database="store_manager_dev",
                user="root",
                password="",
                pool_size=3,
                connect_timeout=10,
                max_retry_attempts=2,
            ),
            api=APIConfig(
                base_url="http://localhost:8000",
                timeout=10,
                retries=2,
                max_concurrent_requests=5,
                rate_limit_calls=1000,
            ),
            processing=ProcessingConfig(
                batch_size=500,
                chunk_size=2000,
                use_multiprocessing=False,
                max_workers=2,
                strict_validation=False,
            ),
            logging=LoggingConfig(
                level="DEBUG",
                enable_console_logging=True,
                enable_file_logging=True,
                log_file="logs/dev_etl_pipeline.log",
                log_sql_queries=True,
                log_performance_metrics=True,
            ),
            application=ApplicationConfig(
                environment="development",
                debug_mode=True,
                enable_caching=False,
                allow_data_export=True,
            ),
        )


class ProductionProfile(ConfigProfile):
    """Production environment — env-var driven, strict validation, JSON logging."""

    @staticmethod
    def load_config() -> ETLConfig:
        _log_file = _env("PROD_LOG_FILE", "logs/etl_pipeline.log")
        return ETLConfig(
            database=DatabaseConfig(
                host=_env("PROD_DB_HOST", "localhost"),
                port=_env_int("PROD_DB_PORT", 3306),
                database=_env("PROD_DB_NAME", "store_manager"),
                user=_env("PROD_DB_USER", "etl_user"),
                password=_env("PROD_DB_PASSWORD", ""),
                pool_size=20,
                connect_timeout=30,
                max_retry_attempts=5,
                retry_delay=5,
            ),
            api=APIConfig(
                base_url=_env("PROD_API_URL", "https://etl-server.fly.dev"),
                timeout=60,
                retries=5,
                retry_delay=2.0,
                max_concurrent_requests=25,
                rate_limit_calls=1000,
                rate_limit_period=60,
            ),
            processing=ProcessingConfig(
                batch_size=5000,
                max_batch_size=20_000,
                chunk_size=10_000,
                max_memory_usage_mb=2048,
                use_multiprocessing=True,
                max_workers=8,
                strict_validation=True,
                validate_schema=True,
            ),
            logging=LoggingConfig(
                level="INFO",
                enable_console_logging=True,
                enable_file_logging=True,
                log_file=_log_file,
                max_file_size=50_000_000,
                backup_count=10,
                use_json_format=True,
                log_sql_queries=False,
                log_performance_metrics=True,
            ),
            application=ApplicationConfig(
                environment="production",
                debug_mode=False,
                enable_caching=True,
                enable_monitoring=True,
                allow_data_export=False,
            ),
        )


class TestingProfile(ConfigProfile):
    """Testing environment — isolated DB, no file logging, fast timeouts."""

    @staticmethod
    def load_config() -> ETLConfig:
        return ETLConfig(
            database=DatabaseConfig(
                host="localhost",
                port=3306,
                database="store_manager_test",
                user="test_user",
                password="test_password",
                pool_size=2,
                connect_timeout=5,
                autocommit=True,
                raise_on_warnings=False,
            ),
            api=APIConfig(
                base_url="http://localhost:8888",
                timeout=5,
                retries=1,
                max_concurrent_requests=3,
                rate_limit_calls=10_000,
            ),
            processing=ProcessingConfig(
                batch_size=100,
                chunk_size=500,
                use_multiprocessing=False,
                max_workers=1,
                strict_validation=True,
                validate_schema=True,
            ),
            logging=LoggingConfig(
                level="DEBUG",
                enable_console_logging=True,
                enable_file_logging=False,
                log_sql_queries=True,
                log_performance_metrics=False,
            ),
            application=ApplicationConfig(
                environment="testing",
                debug_mode=True,
                enable_caching=False,
                enable_monitoring=False,
                data_dir=Path("/tmp/etl_test_data"),
            ),
        )


class StagingProfile(ConfigProfile):
    """Staging environment — env-var driven, production-like but less capacity."""

    @staticmethod
    def load_config() -> ETLConfig:
        _log_file = _env("STAGING_LOG_FILE", "logs/staging_etl_pipeline.log")
        return ETLConfig(
            database=DatabaseConfig(
                host=_env("STAGING_DB_HOST", "staging-db.example.com"),
                port=_env_int("STAGING_DB_PORT", 3306),
                database=_env("STAGING_DB_NAME", "store_manager_staging"),
                user=_env("STAGING_DB_USER", "staging_user"),
                password=_env("STAGING_DB_PASSWORD", ""),
                pool_size=10,
                connect_timeout=20,
                max_retry_attempts=3,
            ),
            api=APIConfig(
                base_url=_env("STAGING_API_URL", "https://staging-api.example.com"),
                timeout=30,
                retries=3,
                max_concurrent_requests=15,
                rate_limit_calls=500,
            ),
            processing=ProcessingConfig(
                batch_size=2000,
                chunk_size=5000,
                use_multiprocessing=True,
                max_workers=4,
                strict_validation=True,
            ),
            logging=LoggingConfig(
                level="INFO",
                enable_console_logging=True,
                enable_file_logging=True,
                log_file=_log_file,
                log_performance_metrics=True,
            ),
            application=ApplicationConfig(
                environment="staging",
                debug_mode=False,
                enable_caching=True,
                enable_monitoring=True,
            ),
        )


# ---------------------------------------------------------------------------
# Profile registry  (private — access via load_config_for_environment)
# ---------------------------------------------------------------------------

_PROFILES: Dict[str, type] = {
    "development": DevelopmentProfile,
    "dev":         DevelopmentProfile,
    "production":  ProductionProfile,
    "prod":        ProductionProfile,
    "testing":     TestingProfile,
    "test":        TestingProfile,
    "staging":     StagingProfile,
    "stage":       StagingProfile,
}

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_config_for_environment(environment: str = None) -> ETLConfig:
    """
    Return an ETLConfig for the specified environment.

    Args:
        environment: One of development/dev, production/prod, testing/test,
                     staging/stage. Falls back to the ENVIRONMENT env var.
                     If neither is set, defaults to 'development'.

    Raises:
        ValueError: If the resolved environment name is not recognised.
    """
    if environment is not None:
        env = environment.lower().strip()
    else:
        # Read directly from _backend so test backends injected via
        # set_env_backend() are respected — _env() helper captures
        # _backend at import time and would return stale values.
        raw = _backend.get("ENVIRONMENT")
        if raw is not None:
            env = raw.lower().strip()
            if env not in _PROFILES:
                raise ValueError(
                    f"ENVIRONMENT='{raw}' is not recognised. "
                    f"Available: {sorted(_PROFILES)}"
                )
        else:
            env = "development"  # only true default — no env var was set at all

    profile = _PROFILES.get(env)
    if profile is None:
        raise ValueError(
            f"Unknown environment '{env}'. Available: {sorted(_PROFILES)}"
        )
    return profile.load_config()


def get_current_environment() -> str:
    """Return the current environment name (lower-cased)."""
    return (_backend.get("ENVIRONMENT") or "development").lower()


def is_production() -> bool:
    """Return True if the current environment is production."""
    return get_current_environment() in ("production", "prod")


def is_development() -> bool:
    """Return True if the current environment is development."""
    return get_current_environment() in ("development", "dev")


def is_testing() -> bool:
    """Return True if the current environment is testing."""
    return get_current_environment() in ("testing", "test")


# ---------------------------------------------------------------------------
# CLI smoke-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    for _env_name in ("development", "production", "testing", "staging"):
        print(f"\n=== {_env_name.upper()} PROFILE ===")
        _cfg = load_config_for_environment(_env_name)
        print(f"Valid:      {_cfg.is_valid()}")
        print(f"Database:   {_cfg.database.get_connection_string()}")
        print(f"API:        {_cfg.api.base_url}")
        print(f"Batch size: {_cfg.processing.batch_size}")
        print(f"Log level:  {_cfg.logging.level}")