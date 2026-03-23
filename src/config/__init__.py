"""
C:\Economy\Invest\TrendMaster\src\config\__init__.py
Configuration Management Module.
Provides structured configuration classes using dataclasses for type safety and validation.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

# env_config is the single module in this package that imports `os`.
# All env-var access goes through these three helpers.
from .env_config import _env, _env_bool, _env_int

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_VALID_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})
_DATA_ROOT        = Path(__file__).parent.parent.parent / "data"

# ---------------------------------------------------------------------------
# DatabaseConfig
# ---------------------------------------------------------------------------

@dataclass
class DatabaseConfig:
    """Database connection configuration with validation."""

    user:     str = field(default_factory=lambda: _env("DB_USER", "root"))
    password: str = field(default_factory=lambda: _env("DB_PASSWORD", ""))
    host:     str = field(default_factory=lambda: _env("DB_HOST", "127.0.0.1"))
    port:     int = field(default_factory=lambda: _env_int("DB_PORT", 3306))
    database: str = field(default_factory=lambda: _env("DB_NAME", "trend_master"))

    # Connection pool
    pool_size:          int  = field(default=5)
    enable_pooling:     bool = field(default=True)
    pool_reset_session: bool = field(default=True)

    # Connection behaviour
    raise_on_warnings: bool = field(default=True)
    autocommit:        bool = field(default=False)
    connect_timeout:   int  = field(default=30)

    # Retry
    max_retry_attempts: int = field(default=3)
    retry_delay:        int = field(default=2)

    def to_dict(self) -> Dict[str, Any]:
        """Return a dict suitable for passing to mysql.connector.connect()."""
        return {
            "user":              self.user,
            "password":          self.password,
            "host":              self.host,
            "port":              self.port,
            "database":          self.database,
            "raise_on_warnings": self.raise_on_warnings,
            "autocommit":        self.autocommit,
            "connect_timeout":   self.connect_timeout,
        }

    def get_connection_string(self) -> str:
        """Return a loggable connection string (password omitted)."""
        return f"mysql://{self.user}@{self.host}:{self.port}/{self.database}"

    def validate(self) -> bool:
        """Return True if all required fields are within acceptable bounds."""
        if not self.host or not self.user:
            return False
        if not (1 <= self.port <= 65535):
            return False
        if self.pool_size < 1:
            return False
        return True


# ---------------------------------------------------------------------------
# APIConfig
# ---------------------------------------------------------------------------

@dataclass
class APIConfig:
    """API configuration for external data sources."""

    base_url: str = field(
        default_factory=lambda: _env("API_BASE_URL", "https://etl-server.fly.dev")
    )
    timeout:     int   = field(default=30)
    retries:     int   = field(default=3)
    retry_delay: float = field(default=1.0)

    # Rate limiting
    rate_limit_calls:  int = field(default=100)
    rate_limit_period: int = field(default=60)  # seconds

    # Authentication
    api_key:      Optional[str] = field(default_factory=lambda: _env("API_KEY") or None)
    bearer_token: Optional[str] = field(
        default_factory=lambda: _env("API_BEARER_TOKEN") or None
    )

    # Headers
    user_agent: str = field(default="ETL-Pipeline/1.0")
    accept:     str = field(default="application/json")

    # Concurrency
    max_concurrent_requests: int = field(default=10)
    semaphore_limit:         int = field(default=5)

    def get_headers(self) -> Dict[str, str]:
        """Return HTTP headers for API requests."""
        headers: Dict[str, str] = {
            "User-Agent":   self.user_agent,
            "Accept":       self.accept,
            "Content-Type": "application/json",
        }
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        if self.bearer_token:
            headers["Authorization"] = f"Bearer {self.bearer_token}"
        return headers

    def validate(self) -> bool:
        if not self.base_url:
            return False
        if self.timeout <= 0 or self.retries < 0:
            return False
        if self.max_concurrent_requests <= 0:
            return False
        return True


# ---------------------------------------------------------------------------
# ProcessingConfig
# ---------------------------------------------------------------------------

@dataclass
class ProcessingConfig:
    """Data processing configuration."""

    # Batch processing
    batch_size:     int = field(default=1000)
    max_batch_size: int = field(default=10_000)

    # Memory
    chunk_size:          int = field(default=5000)
    max_memory_usage_mb: int = field(default=512)

    # CSV
    csv_encoding:  str = field(default="utf-8")
    csv_delimiter: str = field(default=",")
    csv_quotechar: str = field(default='"')

    # Pandas
    pandas_low_memory: bool      = field(default=False)
    pandas_na_values:  List[str] = field(
        default_factory=lambda: ["", "NULL", "null", "NaN", "nan"]
    )

    # Validation
    validate_schema:   bool = field(default=True)
    strict_validation: bool = field(default=False)

    # Performance
    use_multiprocessing: bool = field(default=False)
    max_workers:         int  = field(default=4)

    def validate(self) -> bool:
        if self.batch_size <= 0 or self.batch_size > self.max_batch_size:
            return False
        if self.chunk_size <= 0:
            return False
        if self.max_workers <= 0:
            return False
        return True


# ---------------------------------------------------------------------------
# LoggingConfig
# ---------------------------------------------------------------------------

@dataclass
class LoggingConfig:
    """Logging configuration."""

    level:       str = field(default_factory=lambda: _env("LOG_LEVEL", "INFO"))
    format:      str = field(default="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    date_format: str = field(default="%Y-%m-%d %H:%M:%S")

    # File logging
    enable_file_logging: bool = field(default=True)
    log_file:            str  = field(default="logs/etl_pipeline.log")
    max_file_size:       int  = field(default=10_000_000)  # 10 MB
    backup_count:        int  = field(default=5)

    # Console logging
    enable_console_logging: bool = field(default=True)
    console_level:          str  = field(default="INFO")

    # Structured logging
    use_json_format:      bool = field(default=False)
    include_extra_fields: bool = field(default=True)

    # Performance logging
    log_sql_queries:         bool = field(default=False)
    log_performance_metrics: bool = field(default=True)

    def get_log_directory(self) -> Path:
        """Return the directory that contains the log file."""
        return Path(self.log_file).parent

    def validate(self) -> bool:
        if self.level.upper() not in _VALID_LOG_LEVELS:
            return False
        if self.console_level.upper() not in _VALID_LOG_LEVELS:
            return False
        if self.max_file_size <= 0 or self.backup_count < 0:
            return False
        return True


# ---------------------------------------------------------------------------
# ApplicationConfig
# ---------------------------------------------------------------------------

@dataclass
class ApplicationConfig:
    """Main application configuration."""

    # Metadata
    name:        str = field(default="ETL Pipeline Manager")
    version:     str = field(default="2.0.0")
    environment: str = field(default_factory=lambda: _env("ENVIRONMENT", "development"))

    # Directories
    data_dir:  Path           = field(default_factory=lambda: _DATA_ROOT)
    csv_dir:   Optional[Path] = field(default=None)
    api_dir:   Optional[Path] = field(default=None)
    cache_dir: Optional[Path] = field(default=None)

    # Feature flags
    enable_caching:    bool = field(default=True)
    enable_monitoring: bool = field(default=True)
    enable_api_mode:   bool = field(default=True)

    # Security
    debug_mode:        bool = field(default_factory=lambda: _env_bool("DEBUG", False))
    allow_data_export: bool = field(default=True)

    def __post_init__(self) -> None:
        """Derive sub-directory paths from data_dir if not explicitly set."""
        if self.csv_dir is None:
            self.csv_dir = self.data_dir / "CSV"
        if self.api_dir is None:
            self.api_dir = self.data_dir / "API"
        if self.cache_dir is None:
            self.cache_dir = self.data_dir / "cache"

    def create_directories(self) -> None:
        """Create all configured directories, including parents."""
        for directory in (self.data_dir, self.csv_dir, self.api_dir, self.cache_dir):
            if directory:
                directory.mkdir(parents=True, exist_ok=True)

    def is_production(self) -> bool:
        return self.environment.lower() == "production"

    def is_development(self) -> bool:
        return self.environment.lower() == "development"

    def validate(self) -> bool:
        if not self.name or not self.version:
            return False
        if not self.data_dir:
            return False
        return True


# ---------------------------------------------------------------------------
# ETLConfig  (composite root)
# ---------------------------------------------------------------------------

@dataclass
class ETLConfig:
    """Complete ETL configuration combining all components."""

    database:    DatabaseConfig    = field(default_factory=DatabaseConfig)
    api:         APIConfig         = field(default_factory=APIConfig)
    processing:  ProcessingConfig  = field(default_factory=ProcessingConfig)
    logging:     LoggingConfig     = field(default_factory=LoggingConfig)
    application: ApplicationConfig = field(default_factory=ApplicationConfig)

    def validate_all(self) -> Dict[str, bool]:
        """Return a per-section validation report."""
        return {
            "database":    self.database.validate(),
            "api":         self.api.validate(),
            "processing":  self.processing.validate(),
            "logging":     self.logging.validate(),
            "application": self.application.validate(),
        }

    def is_valid(self) -> bool:
        """Return True only if every section passes validation."""
        return all(self.validate_all().values())

    def get_summary(self) -> Dict[str, Any]:
        """Return a loggable summary (no secrets)."""
        return {
            "application": {
                "name":        self.application.name,
                "version":     self.application.version,
                "environment": self.application.environment,
            },
            "database": {
                "host":      self.database.host,
                "port":      self.database.port,
                "database":  self.database.database,
                "pooling":   self.database.enable_pooling,
                "pool_size": self.database.pool_size,
            },
            "api": {
                "base_url":       self.api.base_url,
                "timeout":        self.api.timeout,
                "max_concurrent": self.api.max_concurrent_requests,
            },
            "processing": {
                "batch_size":      self.processing.batch_size,
                "chunk_size":      self.processing.chunk_size,
                "multiprocessing": self.processing.use_multiprocessing,
            },
        }


# ---------------------------------------------------------------------------
# Factory functions
# ---------------------------------------------------------------------------

def load_config_from_env() -> ETLConfig:
    """Load configuration from environment variables (uses field defaults)."""
    return ETLConfig()


def load_config_from_dict(config_dict: Dict[str, Any]) -> ETLConfig:
    """
    Load configuration from a nested dictionary.

    Only keys that correspond to existing fields are applied;
    unknown keys are silently ignored.
    """
    config = ETLConfig()
    _apply_dict(config.database,   config_dict.get("database",   {}))
    _apply_dict(config.api,        config_dict.get("api",        {}))
    _apply_dict(config.processing, config_dict.get("processing", {}))
    return config


def get_default_config() -> ETLConfig:
    """Return a configuration with sensible production-ready defaults."""
    config = ETLConfig()
    config.database.pool_size          = 10
    config.processing.batch_size       = 2000
    config.api.max_concurrent_requests = 15
    return config


# ---------------------------------------------------------------------------
# Global singleton
# ---------------------------------------------------------------------------

_global_config: Optional[ETLConfig] = None


def get_config() -> ETLConfig:
    """Return the global ETLConfig, initialising from env vars on first call."""
    global _global_config
    if _global_config is None:
        _global_config = load_config_from_env()
    return _global_config


def set_config(config: ETLConfig) -> None:
    """Replace the global ETLConfig."""
    global _global_config
    _global_config = config


def reset_config() -> None:
    """Reset the global ETLConfig to None (forces re-initialisation on next get_config())."""
    global _global_config
    _global_config = None


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _apply_dict(target: object, updates: Dict[str, Any]) -> None:
    """Apply key/value pairs from *updates* to *target*, skipping unknown keys."""
    for key, value in updates.items():
        if hasattr(target, key):
            setattr(target, key, value)


# ---------------------------------------------------------------------------
# CLI smoke-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    config = get_default_config()
    print("Configuration loaded successfully:")
    print(f"Valid: {config.is_valid()}")
    print(f"Validation results: {config.validate_all()}")
    print(f"Summary: {config.get_summary()}")