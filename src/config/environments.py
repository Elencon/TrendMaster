"""
Environment-specific configuration profiles.
"""

from typing import Dict, Any
from pathlib import Path
import os
from . import (
    ETLConfig, 
    DatabaseConfig, 
    APIConfig, 
    ProcessingConfig, 
    LoggingConfig, 
    ApplicationConfig
)


class ConfigProfile:
    """Base class for environment-specific configuration profiles."""
    
    @staticmethod
    def load_config() -> ETLConfig:
        """Load configuration for this environment."""
        raise NotImplementedError("Subclasses must implement load_config()")


class DevelopmentProfile(ConfigProfile):
    """Development environment configuration."""
    
    @staticmethod
    def load_config() -> ETLConfig:
        """Load development configuration."""
        return ETLConfig(
            database=DatabaseConfig(
                host='localhost',
                port=3306,
                database='store_manager_dev',
                user='root',
                password='',
                pool_size=3,
                connect_timeout=10,
                max_retry_attempts=2,
                retry_delay=1.0
            ),
            api=APIConfig(
                base_url='http://localhost:8000',
                timeout=10,
                retries=2,
                max_concurrent_requests=5,
                rate_limit_calls=1000
            ),
            processing=ProcessingConfig(
                batch_size=500,
                chunk_size=2000,
                use_multiprocessing=False,
                max_workers=2,
                strict_validation=False
            ),
            logging=LoggingConfig(
                level='DEBUG',
                enable_console_logging=True,
                enable_file_logging=True,
                log_file='logs/dev_etl_pipeline.log',
                log_sql_queries=True,
                log_performance_metrics=True
            ),
            application=ApplicationConfig(
                environment='development',
                debug_mode=True,
                enable_caching=False,
                allow_data_export=True
            )
        )


class ProductionProfile(ConfigProfile):
    """Production environment configuration."""
    
    @staticmethod
    def load_config() -> ETLConfig:
        """Load production configuration."""
        return ETLConfig(
            database=DatabaseConfig(
                host=os.getenv('PROD_DB_HOST', 'localhost'),
                port=int(os.getenv('PROD_DB_PORT', '3306')),
                database=os.getenv('PROD_DB_NAME', 'store_manager'),
                user=os.getenv('PROD_DB_USER', 'etl_user'),
                password=os.getenv('PROD_DB_PASSWORD', ''),
                pool_size=20,
                connect_timeout=30,
                max_retry_attempts=5,
                retry_delay=5.0
            ),
            api=APIConfig(
                base_url=os.getenv('PROD_API_URL', 'https://etl-server.fly.dev'),
                timeout=60,
                retries=5,
                retry_delay=2.0,
                max_concurrent_requests=25,
                rate_limit_calls=1000,
                rate_limit_period=60
            ),
            processing=ProcessingConfig(
                batch_size=5000,
                max_batch_size=20000,
                chunk_size=10000,
                max_memory_usage_mb=2048,
                use_multiprocessing=True,
                max_workers=8,
                strict_validation=True,
                validate_schema=True
            ),
            logging=LoggingConfig(
                level='INFO',
                enable_console_logging=True,
                enable_file_logging=True,
                log_file='/var/log/etl/etl_pipeline.log',
                max_file_size=50_000_000,  # 50MB
                backup_count=10,
                use_json_format=True,
                log_sql_queries=False,
                log_performance_metrics=True
            ),
            application=ApplicationConfig(
                environment='production',
                debug_mode=False,
                enable_caching=True,
                enable_monitoring=True,
                allow_data_export=False
            )
        )


class TestingProfile(ConfigProfile):
    """Testing environment configuration."""
    
    @staticmethod
    def load_config() -> ETLConfig:
        """Load testing configuration."""
        return ETLConfig(
            database=DatabaseConfig(
                host='localhost',
                port=3306,
                database='store_manager_test',
                user='test_user',
                password='test_password',
                pool_size=2,
                connect_timeout=5,
                autocommit=True,
                raise_on_warnings=False
            ),
            api=APIConfig(
                base_url='http://localhost:8888',  # Test server
                timeout=5,
                retries=1,
                max_concurrent_requests=3,
                rate_limit_calls=10000  # No limits for testing
            ),
            processing=ProcessingConfig(
                batch_size=100,
                chunk_size=500,
                use_multiprocessing=False,
                max_workers=1,
                strict_validation=True,
                validate_schema=True
            ),
            logging=LoggingConfig(
                level='DEBUG',
                enable_console_logging=True,
                enable_file_logging=False,
                log_sql_queries=True,
                log_performance_metrics=False
            ),
            application=ApplicationConfig(
                environment='testing',
                debug_mode=True,
                enable_caching=False,
                enable_monitoring=False,
                data_dir=Path('/tmp/etl_test_data')
            )
        )


class StagingProfile(ConfigProfile):
    """Staging environment configuration."""
    
    @staticmethod
    def load_config() -> ETLConfig:
        """Load staging configuration."""
        return ETLConfig(
            database=DatabaseConfig(
                host=os.getenv('STAGING_DB_HOST', 'staging-db.example.com'),
                port=int(os.getenv('STAGING_DB_PORT', '3306')),
                database=os.getenv('STAGING_DB_NAME', 'store_manager_staging'),
                user=os.getenv('STAGING_DB_USER', 'staging_user'),
                password=os.getenv('STAGING_DB_PASSWORD', ''),
                pool_size=10,
                connect_timeout=20,
                max_retry_attempts=3,
                retry_delay=2.0
            ),
            api=APIConfig(
                base_url=os.getenv('STAGING_API_URL', 'https://staging-api.example.com'),
                timeout=30,
                retries=3,
                max_concurrent_requests=15,
                rate_limit_calls=500
            ),
            processing=ProcessingConfig(
                batch_size=2000,
                chunk_size=5000,
                use_multiprocessing=True,
                max_workers=4,
                strict_validation=True
            ),
            logging=LoggingConfig(
                level='INFO',
                enable_console_logging=True,
                enable_file_logging=True,
                log_file='/var/log/etl/staging_etl_pipeline.log',
                log_performance_metrics=True
            ),
            application=ApplicationConfig(
                environment='staging',
                debug_mode=False,
                enable_caching=True,
                enable_monitoring=True
            )
        )


# Profile registry
PROFILES: Dict[str, type] = {
    'development': DevelopmentProfile,
    'dev': DevelopmentProfile,  # Alias
    'production': ProductionProfile,
    'prod': ProductionProfile,  # Alias
    'testing': TestingProfile,
    'test': TestingProfile,  # Alias
    'staging': StagingProfile,
    'stage': StagingProfile  # Alias
}


def load_config_for_environment(environment: str = None) -> ETLConfig:
    """
    Load configuration for specified environment.
    
    Args:
        environment (str): Environment name (development, production, testing, staging)
                          If None, uses ENVIRONMENT env var or defaults to development
    
    Returns:
        ETLConfig: Configuration for the specified environment
    """
    if environment is None:
        environment = os.getenv('ENVIRONMENT', 'development')
    
    environment = environment.lower().strip()
    
    if environment in PROFILES:
        profile_class = PROFILES[environment]
        return profile_class.load_config()
    else:
        available_profiles = list(PROFILES.keys())
        raise ValueError(f"Unknown environment '{environment}'. Available: {available_profiles}")


def get_current_environment() -> str:
    """Get current environment from env var or default."""
    return os.getenv('ENVIRONMENT', 'development').lower()


def is_production() -> bool:
    """Check if current environment is production."""
    return get_current_environment() in ['production', 'prod']


def is_development() -> bool:
    """Check if current environment is development."""
    return get_current_environment() in ['development', 'dev']


def is_testing() -> bool:
    """Check if current environment is testing."""
    return get_current_environment() in ['testing', 'test']


if __name__ == "__main__":
    # Test environment profiles
    for env_name in ['development', 'production', 'testing', 'staging']:
        print(f"\n=== {env_name.upper()} PROFILE ===")
        try:
            config = load_config_for_environment(env_name)
            print(f"Valid: {config.is_valid()}")
            print(f"Database: {config.database.get_connection_string()}")
            print(f"API: {config.api.base_url}")
            print(f"Batch size: {config.processing.batch_size}")
            print(f"Log level: {config.logging.level}")
        except Exception as e:
            print(f"Error loading {env_name}: {e}")