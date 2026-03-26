r"""
C:\Economy\Invest\TrendMaster\src\config\__init__.py
ETL Configuration Module
------------------------
Provides structured configuration classes using dataclasses.
Automatically validates fields via __post_init__.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _env(key: str, default: Any = None) -> Any:
    return os.environ.get(key, default)

def _env_int(key: str, default: int = 0) -> int:
    val = os.environ.get(key)
    return int(val) if val is not None else default

def _env_bool(key: str, default: bool = False) -> bool:
    val = os.environ.get(key)
    if val is None:
        return default
    return val.lower() in ("1", "true", "yes")


# ---------------------------------------------------------------------------
# DatabaseConfig
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DatabaseConfig:
    """Database connection configuration."""

    user: str = field(default_factory=lambda: _env("DB_USER", "root"))
    password: str = field(default_factory=lambda: _env("DB_PASSWORD", ""))
    host: str = field(default_factory=lambda: _env("DB_HOST", "127.0.0.1"))
    port: int = field(default_factory=lambda: _env_int("DB_PORT", 3306))
    database: str = field(default_factory=lambda: _env("DB_NAME", "trend_master"))

    # Added charset field
    charset: str = field(default_factory=lambda: _env("DB_CHARSET", "utf8mb4"))

    pool_size: int = 5
    enable_pooling: bool = True
    pool_reset_session: bool = True

    raise_on_warnings: bool = True
    autocommit: bool = False
    connect_timeout: int = 30

    max_retry_attempts: int = 3
    retry_delay: int = 2

    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.host or not self.user:
            raise ValueError("Database host and user cannot be empty")

        if not (1 <= self.port <= 65535):
            raise ValueError(f"Invalid port: {self.port}")

        if self.pool_size < 1:
            raise ValueError("pool_size must be >= 1")

        if not self.charset:
            raise ValueError("Database charset cannot be empty")

    def to_dict(self) -> Dict[str, Any]:
        """Return parameters suitable for PyMySQL driver."""
        return {
            "user": self.user,
            "password": self.password,
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "charset": self.charset,  # Now included automatically
            "raise_on_warnings": self.raise_on_warnings,
            "autocommit": self.autocommit,
            "connect_timeout": self.connect_timeout,
        }

    def get_connection_string(self) -> str:
        """Return a safe connection string for logging."""
        return f"mysql://{self.user}@{self.host}:{self.port}/{self.database}?charset={self.charset}"

# ---------------------------------------------------------------------------
# APIConfig
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class APIConfig:
    base_url: str = field(default_factory=lambda: _env("API_BASE_URL", "https://etl-server.fly.dev"))
    timeout: int = 30
    retries: int = 3
    retry_delay: float = 1.0
    rate_limit_calls: int = 100
    rate_limit_period: int = 60
    api_key: Optional[str] = field(default_factory=lambda: _env("API_KEY") or None)
    bearer_token: Optional[str] = field(default_factory=lambda: _env("API_BEARER_TOKEN") or None)
    user_agent: str = "ETL-Pipeline/1.0"
    accept: str = "application/json"
    max_concurrent_requests: int = 10
    semaphore_limit: int = 5

    def __post_init__(self):
        if not self.base_url:
            raise ValueError("API base_url cannot be empty")
        if self.timeout <= 0 or self.retries < 0:
            raise ValueError("Invalid timeout or retries")
        if self.max_concurrent_requests <= 0:
            raise ValueError("max_concurrent_requests must be > 0")

    def get_headers(self) -> Dict[str, str]:
        headers = {
            "User-Agent": self.user_agent,
            "Accept": self.accept,
            "Content-Type": "application/json",
        }
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        if self.bearer_token:
            headers["Authorization"] = f"Bearer {self.bearer_token}"
        return headers


# ---------------------------------------------------------------------------
# ProcessingConfig
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ProcessingConfig:
    batch_size: int = 1000
    max_batch_size: int = 10_000
    chunk_size: int = 5000
    max_memory_usage_mb: int = 512
    csv_encoding: str = "utf-8"
    csv_delimiter: str = ","
    csv_quotechar: str = '"'
    pandas_low_memory: bool = False
    pandas_na_values: List[str] = field(default_factory=lambda: ["", "NULL", "null", "NaN", "nan"])
    validate_schema: bool = True
    strict_validation: bool = False
    use_multiprocessing: bool = False
    max_workers: int = 4

    def __post_init__(self):
        if self.batch_size <= 0 or self.batch_size > self.max_batch_size:
            raise ValueError("batch_size invalid")
        if self.chunk_size <= 0:
            raise ValueError("chunk_size must be > 0")
        if self.max_workers <= 0:
            raise ValueError("max_workers must be > 0")


# ---------------------------------------------------------------------------
# LoggingConfig
# ---------------------------------------------------------------------------

_VALID_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})

@dataclass(frozen=True)
class LoggingConfig:
    level: str = field(default_factory=lambda: _env("LOG_LEVEL", "INFO"))
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    date_format: str = "%Y-%m-%d %H:%M:%S"
    enable_file_logging: bool = True
    log_file: str = "logs/etl_pipeline.log"
    max_file_size: int = 10_000_000
    backup_count: int = 5
    enable_console_logging: bool = True
    console_level: str = "INFO"
    use_json_format: bool = False
    include_extra_fields: bool = True
    log_sql_queries: bool = False
    log_performance_metrics: bool = True

    def __post_init__(self):
        if self.level.upper() not in _VALID_LOG_LEVELS:
            raise ValueError(f"Invalid logging level: {self.level}")
        if self.console_level.upper() not in _VALID_LOG_LEVELS:
            raise ValueError(f"Invalid console level: {self.console_level}")
        if self.max_file_size <= 0 or self.backup_count < 0:
            raise ValueError("Invalid log file settings")


# ---------------------------------------------------------------------------
# ApplicationConfig
# ---------------------------------------------------------------------------

_DATA_ROOT = Path(__file__).parent.parent.parent / "data"

@dataclass(frozen=True)
class ApplicationConfig:
    name: str = "ETL Pipeline Manager"
    version: str = "2.0.0"
    environment: str = field(default_factory=lambda: _env("ENVIRONMENT", "development"))
    data_dir: Path = field(default_factory=lambda: _DATA_ROOT)
    csv_dir: Optional[Path] = None
    api_dir: Optional[Path] = None
    cache_dir: Optional[Path] = None
    enable_caching: bool = True
    enable_monitoring: bool = True
    enable_api_mode: bool = True
    debug_mode: bool = field(default_factory=lambda: _env_bool("DEBUG", False))
    allow_data_export: bool = True

    def __post_init__(self):
        object.__setattr__(self, "csv_dir", self.csv_dir or self.data_dir / "CSV")
        object.__setattr__(self, "api_dir", self.api_dir or self.data_dir / "API")
        object.__setattr__(self, "cache_dir", self.cache_dir or self.data_dir / "cache")


# ---------------------------------------------------------------------------
# ETLConfig composite root
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ETLConfig:
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    api: APIConfig = field(default_factory=APIConfig)
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    application: ApplicationConfig = field(default_factory=ApplicationConfig)

    def __post_init__(self):
        # Ensure all components validate automatically
        self.database.__post_init__()
        self.api.__post_init__()
        self.processing.__post_init__()
        self.logging.__post_init__()
        self.application.__post_init__()

    def get_summary(self) -> Dict[str, Any]:
        return {
            "application": {
                "name": self.application.name,
                "version": self.application.version,
                "environment": self.application.environment,
            },
            "database": {
                "host": self.database.host,
                "port": self.database.port,
                "database": self.database.database,
                "pooling": self.database.enable_pooling,
                "pool_size": self.database.pool_size,
            },
            "api": {
                "base_url": self.api.base_url,
                "timeout": self.api.timeout,
                "max_concurrent": self.api.max_concurrent_requests,
            },
            "processing": {
                "batch_size": self.processing.batch_size,
                "chunk_size": self.processing.chunk_size,
                "multiprocessing": self.processing.use_multiprocessing,
            },
        }


# ---------------------------------------------------------------------------
# Global singleton
# ---------------------------------------------------------------------------

_global_config: Optional[ETLConfig] = None

def get_config() -> ETLConfig:
    global _global_config
    if _global_config is None:
        _global_config = ETLConfig()
    return _global_config

def set_config(config: ETLConfig) -> None:
    global _global_config
    _global_config = config

def reset_config() -> None:
    global _global_config
    _global_config = None
