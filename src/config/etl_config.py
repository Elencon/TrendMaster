"""
Configuration Management Module
Provides structured configuration classes using dataclasses for type safety and validation.
"""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from pathlib import Path
from yarl import URL

from .env_config import env_config
# Import your centralized path definitions
from .path_config import (
    DATA_PATH,
    CSV_PATH,
    API_PATH,
    CACHE_PATH,
)


@dataclass
class DatabaseConfig:
    """Database connection configuration with validation."""

    user: str = field(default_factory=lambda: env_config.db_user)
    password: str = field(default_factory=lambda: env_config.db_password)
    host: str = field(default_factory=lambda: env_config.db_host)
    port: int = field(default_factory=lambda: env_config.db_port)
    database: str = field(default_factory=lambda: env_config.db_name)

    # Connection behavior
    raise_on_warnings: bool = field(default=True)
    autocommit: bool = field(default=False)
    connect_timeout: int = field(default=30)

    # Retry settings
    max_retry_attempts: int = field(default=3)
    retry_delay: int = field(default=2)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for mysql connector."""
        return {
            'user': self.user,
            'password': self.password,
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'raise_on_warnings': self.raise_on_warnings,
            'autocommit': self.autocommit,
            'connect_timeout': self.connect_timeout
        }

    def get_connection_string(self) -> str:
        raise NotImplementedError


    def validate(self) -> bool:
        """Validate configuration parameters."""
        if not self.host or not self.user:
            return False
        if not (1 <= self.port <= 65535):
            return False
        return True

@dataclass
class APIConfig:
    """API configuration for external data sources."""

    base_url: str = field(default_factory=lambda: env_config.api_url)
    timeout: int = field(default=30)
    retries: int = field(default=3)
    retry_delay: float = field(default=1.0)

    # Rate limiting
    rate_limit_calls: int = field(default=100)
    rate_limit_period: int = field(default=60)  # seconds

    # Authentication
    api_key: Optional[str] = field(default_factory=lambda: env_config.api_key)
    bearer_token: Optional[str] = field(default_factory=lambda: env_config.api_bearer_token)

    # Headers
    user_agent: str = field(default="ETL-Pipeline/1.0")
    accept: str = field(default="application/json")

    # Async settings
    max_concurrent_requests: int = field(default=10)
    semaphore_limit: int = field(default=5)

    def get_headers(self) -> Dict[str, str]:
        """Get HTTP headers for API requests."""
        headers = {
            'User-Agent': self.user_agent,
            'Accept': self.accept,
            'Content-Type': 'application/json'
        }

        if self.api_key:
            headers['X-API-Key'] = self.api_key

        if self.bearer_token:
            headers['Authorization'] = f'Bearer {self.bearer_token}'

        return headers


    def _is_valid_url(self, url: str) -> bool:
        """Validate HTTP/HTTPS URL using yarl."""

        if not url:
            logger.error("APIConfig validation failed: base_url is missing.")
            return False

        try:
            parsed = URL(url.strip())

            if parsed.scheme not in {"http", "https"}:
                logger.error(
                    f"APIConfig validation failed: Unsupported URL scheme '{parsed.scheme}'."
                )
                return False

            if parsed.host is None:
                logger.error(
                    f"APIConfig validation failed: URL '{url}' has no host component."
                )
                return False

            return True

        except Exception:
            logger.error(f"APIConfig validation failed: Invalid URL '{url}'.")
            return False


    def validate(self) -> bool:
        valid = True

        if not self._is_valid_url(self.base_url):
            valid = False

        if self.timeout <= 0:
            logger.error("APIConfig validation failed: timeout must be > 0.")
            valid = False

        if self.retries < 0:
            logger.error("APIConfig validation failed: retries cannot be negative.")
            valid = False

        if self.rate_limit_calls <= 0 or self.rate_limit_period <= 0:
            logger.error("APIConfig validation failed: rate limit values must be > 0.")
            valid = False

        if self.max_concurrent_requests <= 0:
            logger.error("APIConfig validation failed: max_concurrent_requests must be > 0.")
            valid = False

        if self.semaphore_limit > self.max_concurrent_requests:
            logger.error(
                "APIConfig validation failed: semaphore_limit cannot exceed max_concurrent_requests."
            )
            valid = False

        if not (self.api_key or self.bearer_token):
            logger.warning(
                "APIConfig: No authentication provided (api_key or bearer_token). "
                "Some APIs may reject requests."
            )

        return valid

@dataclass
class ProcessingConfig:
    """Data processing configuration."""

    # Batch processing
    batch_size: int = field(default=1000)
    max_batch_size: int = field(default=10000)

    # Memory management
    chunk_size: int = field(default=5000)
    max_memory_usage_mb: int = field(default=512)

    # CSV processing
    csv_encoding: str = field(default='utf-8')
    csv_delimiter: str = field(default=',')
    csv_quotechar: str = field(default='"')

    # Pandas options
    pandas_low_memory: bool = field(default=False)
    pandas_na_values: List[str] = field(default_factory=lambda: ['', 'NULL', 'null', 'NaN', 'nan'])

    # Data validation
    validate_schema: bool = field(default=True)
    strict_validation: bool = field(default=False)

    # Performance
    use_multiprocessing: bool = field(default=False)
    max_workers: int = field(default=4)

    def validate(self) -> bool:
        """Validate processing configuration."""
        if self.batch_size <= 0 or self.batch_size > self.max_batch_size:
            return False
        if self.chunk_size <= 0:
            return False
        if self.max_workers <= 0:
            return False
        return True

@dataclass
class LoggingConfig:
    """Logging configuration."""

    level: str = field(default_factory=lambda: env_config.log_level)
    format: str = field(default='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    date_format: str = field(default='%Y-%m-%d %H:%M:%S')

    # File logging
    enable_file_logging: bool = field(default=True)
    log_file: str = field(default='logs/etl_pipeline.log')
    max_file_size: int = field(default=10_000_000)  # 10MB
    backup_count: int = field(default=5)

    # Console logging
    enable_console_logging: bool = field(default=True)
    console_level: str = field(default='INFO')

    # Structured logging
    use_json_format: bool = field(default=False)
    include_extra_fields: bool = field(default=True)

    # Performance logging
    log_sql_queries: bool = field(default=False)
    log_performance_metrics: bool = field(default=True)

    def get_log_directory(self) -> Path:
        """Get log directory path."""
        return Path(self.log_file).parent

    def validate(self) -> bool:
        """Validate logging configuration."""
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if self.level.upper() not in valid_levels:
            return False
        if self.console_level.upper() not in valid_levels:
            return False
        if self.max_file_size <= 0 or self.backup_count < 0:
            return False
        return True


@dataclass
class ApplicationConfig:
    """Main application configuration."""

    # Application metadata
    name: str = "ETL Pipeline Manager"
    version: str = "2.0.0"
    environment: str = field(default_factory=lambda: env_config.environment)

    # Data directories (sourced from path_config)
    data_dir: Path = DATA_PATH
    csv_dir: Path = CSV_PATH
    api_dir: Path = API_PATH
    cache_dir: Path = CACHE_PATH

    # Feature flags
    enable_caching: bool = True
    enable_monitoring: bool = True
    enable_api_mode: bool = True

    # Security
    debug_mode: bool = field(default_factory=lambda: env_config.debug)
    allow_data_export: bool = True

    def __post_init__(self) -> None:
        """
        Automatically called after initialization.
        Ensures all required application directories exist.
        """
        self.create_directories()

    def create_directories(self) -> None:
        """Create necessary directories if they don't exist."""
        directories = [
            self.data_dir,
            self.csv_dir,
            self.api_dir,
            self.cache_dir
        ]
        for directory in directories:
            # parents=True creates missing intermediate parents
            # exist_ok=True prevents errors if the folder already exists
            directory.mkdir(parents=True, exist_ok=True)

    def is_production(self) -> bool:
        return self.environment.lower() == "production"

    def is_development(self) -> bool:
        return self.environment.lower() == "development"

    def validate(self) -> bool:
        return bool(self.name and self.version and self.data_dir)


@dataclass
class ETLConfig:
    """Complete ETL configuration combining all components."""

    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    api: APIConfig = field(default_factory=APIConfig)
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    application: ApplicationConfig = field(default_factory=ApplicationConfig)

    def validate_all(self) -> Dict[str, bool]:
        """Validate all configuration sections."""
        return {
            'database': self.database.validate(),
            'api': self.api.validate(),
            'processing': self.processing.validate(),
            'logging': self.logging.validate(),
            'application': self.application.validate()
        }

    def is_valid(self) -> bool:
        """Check if all configurations are valid."""
        return all(self.validate_all().values())

    def get_summary(self) -> Dict[str, Any]:
        """Get configuration summary for logging."""
        return {
            'application': {
                'name': self.application.name,
                'version': self.application.version,
                'environment': self.application.environment
            },
            'database': {
                'host': self.database.host,
                'port': self.database.port,
                'database': self.database.database,
            },
            'api': {
                'base_url': self.api.base_url,
                'timeout': self.api.timeout,
                'max_concurrent': self.api.max_concurrent_requests
            },
            'processing': {
                'batch_size': self.processing.batch_size,
                'chunk_size': self.processing.chunk_size,
                'multiprocessing': self.processing.use_multiprocessing
            }
        }


# Configuration factory functions

def load_config_from_env() -> ETLConfig:
    """Load configuration from environment variables."""
    return ETLConfig()


def load_config_from_dict(config_dict: Dict[str, Any]) -> ETLConfig:
    """Load configuration from dictionary."""
    config = ETLConfig()

    if 'database' in config_dict:
        db_config = config_dict['database']
        for key, value in db_config.items():
            if hasattr(config.database, key):
                setattr(config.database, key, value)

    if 'api' in config_dict:
        api_config = config_dict['api']
        for key, value in api_config.items():
            if hasattr(config.api, key):
                setattr(config.api, key, value)

    if 'processing' in config_dict:
        proc_config = config_dict['processing']
        for key, value in proc_config.items():
            if hasattr(config.processing, key):
                setattr(config.processing, key, value)

    return config



def get_default_config() -> ETLConfig:
    """Get default configuration with sensible defaults."""
    config = ETLConfig()

    # Override some defaults for production readiness
    config.database.pool_size = 10
    config.processing.batch_size = 2000
    config.api.max_concurrent_requests = 15

    return config

_global_config: Optional[ETLConfig] = None

def get_config() -> ETLConfig:
    """Get global configuration instance."""
    global _global_config
    if _global_config is None:
        _global_config = load_config_from_env()
    return _global_config

def set_config(config: ETLConfig) -> None:
    """Set global configuration instance."""
    global _global_config
    _global_config = config

def reset_config() -> None:
    """Reset global configuration to None."""
    global _global_config
    _global_config = None


if __name__ == "__main__":
    # Test configuration
    config = get_default_config()
    print("Configuration loaded successfully:")
    print(f"Valid: {config.is_valid()}")
    print(f"Validation results: {config.validate_all()}")
    print(f"Summary: {config.get_summary()}")
