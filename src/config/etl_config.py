"""
Configuration Management Module
Provides structured configuration classes using dataclasses for type safety and validation.
"""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from pathlib import Path
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


@dataclass
class DatabaseConfig:
    """Database connection configuration with validation."""
    
    user: str = field(default_factory=lambda: os.getenv('DB_USER', 'root'))
    password: str = field(default_factory=lambda: os.getenv('DB_PASSWORD', ''))
    host: str = field(default_factory=lambda: os.getenv('DB_HOST', '127.0.0.1'))
    port: int = field(default_factory=lambda: int(os.getenv('DB_PORT', '3306')))
    database: str = field(default_factory=lambda: os.getenv('DB_NAME', 'store_manager'))
    
    # Connection pool settings
    pool_size: int = field(default=5)
    enable_pooling: bool = field(default=True)
    pool_reset_session: bool = field(default=True)
    
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
        """Get connection string for logging (without password)."""
        return f"mysql://{self.user}@{self.host}:{self.port}/{self.database}"
    
    def validate(self) -> bool:
        """Validate configuration parameters."""
        if not self.host or not self.user:
            return False
        if not (1 <= self.port <= 65535):
            return False
        if self.pool_size < 1:
            return False
        return True


@dataclass
class APIConfig:
    """API configuration for external data sources."""
    
    base_url: str = field(default_factory=lambda: os.getenv('API_BASE_URL', 'https://etl-server.fly.dev'))
    timeout: int = field(default=30)
    retries: int = field(default=3)
    retry_delay: float = field(default=1.0)
    
    # Rate limiting
    rate_limit_calls: int = field(default=100)
    rate_limit_period: int = field(default=60)  # seconds
    
    # Authentication
    api_key: Optional[str] = field(default_factory=lambda: os.getenv('API_KEY'))
    bearer_token: Optional[str] = field(default_factory=lambda: os.getenv('API_BEARER_TOKEN'))
    
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
    
    def validate(self) -> bool:
        """Validate API configuration."""
        if not self.base_url:
            return False
        if self.timeout <= 0 or self.retries < 0:
            return False
        if self.max_concurrent_requests <= 0:
            return False
        return True


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
    
    level: str = field(default_factory=lambda: os.getenv('LOG_LEVEL', 'INFO'))
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
    name: str = field(default="ETL Pipeline Manager")
    version: str = field(default="2.0.0")
    environment: str = field(default_factory=lambda: os.getenv('ENVIRONMENT', 'development'))
    
    # Data directories
    data_dir: Path = field(default_factory=lambda: Path(__file__).parent.parent.parent / 'data')
    csv_dir: Optional[Path] = field(default=None)
    api_dir: Optional[Path] = field(default=None)
    cache_dir: Optional[Path] = field(default=None)
    
    # Feature flags
    enable_caching: bool = field(default=True)
    enable_monitoring: bool = field(default=True)
    enable_api_mode: bool = field(default=True)
    
    # Security
    debug_mode: bool = field(default_factory=lambda: os.getenv('DEBUG', 'False').lower() == 'true')
    allow_data_export: bool = field(default=True)
    
    def __post_init__(self):
        """Initialize derived paths after creation."""
        if self.csv_dir is None:
            self.csv_dir = self.data_dir / 'CSV'
        if self.api_dir is None:
            self.api_dir = self.data_dir / 'API'
        if self.cache_dir is None:
            self.cache_dir = self.data_dir / 'cache'
    
    def create_directories(self) -> None:
        """Create necessary directories if they don't exist."""
        directories = [self.data_dir, self.csv_dir, self.api_dir, self.cache_dir]
        for directory in directories:
            if directory:
                directory.mkdir(parents=True, exist_ok=True)
    
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment.lower() == 'production'
    
    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.environment.lower() == 'development'
    
    def validate(self) -> bool:
        """Validate application configuration."""
        if not self.name or not self.version:
            return False
        if not self.data_dir:
            return False
        return True


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
                'pooling': self.database.enable_pooling,
                'pool_size': self.database.pool_size
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


# Global configuration instance
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