r"""
C:\Economy\Invest\TrendMaster\src\config\environments.py
Environment-specific configuration profiles.
this code is not used
"""

from typing import Dict, Type
from pathlib import Path
from .env_config import env_config

from .etl_config import (
    ETLConfig,
    APIConfig,
    ProcessingConfig,
    LoggingConfig,
    ApplicationConfig,
)

# NEW: import MySQL presets (auto-loads .env inside database.py)
from .database import (
    mysql_development,
    mysql_production,
    mysql_testing,
)


# ---------------------------------------------------------------------------
# Base Profile
# ---------------------------------------------------------------------------

class ConfigProfile:
    """Base class for environment-specific configuration profiles."""

    @staticmethod
    def load_config() -> ETLConfig:
        raise NotImplementedError("Subclasses must implement load_config()")

# ---------------------------------------------------------------------------
# DEVELOPMENT
# ---------------------------------------------------------------------------

class DevelopmentProfile(ConfigProfile):

    @staticmethod
    def load_config() -> ETLConfig:
        return ETLConfig(
            database=mysql_development(),
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


# ---------------------------------------------------------------------------
# PRODUCTION
# ---------------------------------------------------------------------------

class ProductionProfile(ConfigProfile):

    @staticmethod
    def load_config() -> ETLConfig:
        return ETLConfig(
            database=mysql_production(),
            api=APIConfig(
                base_url=field(default_factory=lambda: env_config.get("PROD_API_URL", "https://etl-server.fly.dev")),
                timeout=60,
                retries=5,
                retry_delay=2.0,
                max_concurrent_requests=25,
                rate_limit_calls=1000,
                rate_limit_period=60,
            ),
            processing=ProcessingConfig(
                batch_size=5000,
                max_batch_size=20000,
                chunk_size=10000,
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
                log_file="/var/log/etl/etl_pipeline.log",
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


# ---------------------------------------------------------------------------
# TESTING
# ---------------------------------------------------------------------------

class TestingProfile(ConfigProfile):

    @staticmethod
    def load_config() -> ETLConfig:
        return ETLConfig(
            database=mysql_testing(),
            api=APIConfig(
                base_url="http://localhost:8888",
                timeout=5,
                retries=1,
                max_concurrent_requests=3,
                rate_limit_calls=10000,
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


# ---------------------------------------------------------------------------
# STAGING
# ---------------------------------------------------------------------------

class StagingProfile(ConfigProfile):

    @staticmethod
    def load_config() -> ETLConfig:
        return ETLConfig(
            database=mysql_production(),  # staging uses prod-like DB defaults
            api=APIConfig(
                base_url=field(default_factory=lambda: env_config.get("STAGING_API_URL", "https://staging-api.example.com")),
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
                log_file="/var/log/etl/staging_etl_pipeline.log",
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
# PROFILE REGISTRY
# ---------------------------------------------------------------------------

PROFILES: Dict[str, Type[ConfigProfile]] = {
    "development": DevelopmentProfile,
    "dev": DevelopmentProfile,
    "production": ProductionProfile,
    "prod": ProductionProfile,
    "testing": TestingProfile,
    "test": TestingProfile,
    "staging": StagingProfile,
    "stage": StagingProfile,
}


# ---------------------------------------------------------------------------
# ENVIRONMENT HELPERS
# ---------------------------------------------------------------------------
# In environments.py

def load_config_for_environment(environment: str = None) -> ETLConfig:
    if environment is None:
        environment = env_config.environment # Uses the property you already built

    environment = environment.lower().strip()

    if environment in PROFILES:
        return PROFILES[environment].load_config()

    raise ValueError(f"Unknown environment '{environment}'. Available: {list(PROFILES.keys())}")


def get_current_environment() -> str:
    return env_config.get("ENVIRONMENT", "development").lower()


def is_production() -> bool:
    return get_current_environment() in ["production", "prod"]


def is_development() -> bool:
    return get_current_environment() in ["development", "dev"]


def is_testing() -> bool:
    return get_current_environment() in ["testing", "test"]
