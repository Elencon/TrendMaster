r"""
Configuration package initializer.

Ensures path configuration is loaded first so that:
- sys.path is patched correctly
- project paths are available to all config modules
"""

# ---------------------------------------------------------
# Load path configuration FIRST (this modifies sys.path)
# ---------------------------------------------------------
from .path_config import (
    PROJECT_ROOT,
    SRC_PATH,
    GUI_PATH,
    DATA_PATH,
    CSV_PATH,
    API_PATH,
    ENV_PATH,
)

# ---------------------------------------------------------
# Environment configuration
# ---------------------------------------------------------
from .env_config import env_config, EnvConfig

# ---------------------------------------------------------
# Base configuration classes and core functions
# ---------------------------------------------------------
from .etl_config import (
    DatabaseConfig,
    APIConfig,
    ProcessingConfig,
    LoggingConfig,
    ApplicationConfig,
    ETLConfig,
    load_config_from_env,
    load_config_from_dict,
    get_default_config,
    get_config,
    set_config,
    reset_config,
)

# ---------------------------------------------------------
# Specialized Database configurations
# ---------------------------------------------------------
from .database import (
    MySQLConfig,
    get_mysql_development_config,
    get_mysql_production_config,
    get_mysql_testing_config,
)

# ---------------------------------------------------------
# Specialized API configurations
# ---------------------------------------------------------
from .api import (
    RESTAPIConfig,
    GraphQLAPIConfig,
    AsyncAPIConfig,
    get_etl_server_config,
    get_jsonplaceholder_config,
    get_local_dev_config,
    get_async_production_config,
)

# ---------------------------------------------------------
# Environment profiles and loaders
# ---------------------------------------------------------
from .environments import (
    ConfigProfile,
    DevelopmentProfile,
    ProductionProfile,
    TestingProfile,
    StagingProfile,
    PROFILES,
    load_config_for_environment,
    get_current_environment,
    is_production,
    is_development,
    is_testing,
)

# ---------------------------------------------------------
# Public API
# ---------------------------------------------------------
__all__ = [
    # Paths
    "PROJECT_ROOT",
    "SRC_PATH",
    "GUI_PATH",
    "DATA_PATH",
    "CSV_PATH",
    "API_PATH",
    "ENV_PATH",

    # Environment helper
    "env_config",
    "EnvConfig",

    # Core Classes
    "DatabaseConfig",
    "APIConfig",
    "ProcessingConfig",
    "LoggingConfig",
    "ApplicationConfig",
    "ETLConfig",

    # Specialized Subclasses
    "MySQLConfig",
    "RESTAPIConfig",
    "GraphQLAPIConfig",
    "AsyncAPIConfig",

    # Core Functions
    "load_config_from_env",
    "load_config_from_dict",
    "get_default_config",
    "get_config",
    "set_config",
    "reset_config",

    # Database Presets
    "get_mysql_development_config",
    "get_mysql_production_config",
    "get_mysql_testing_config",

    # API Presets
    "get_etl_server_config",
    "get_jsonplaceholder_config",
    "get_local_dev_config",
    "get_async_production_config",

    # Environment Management
    "ConfigProfile",
    "DevelopmentProfile",
    "ProductionProfile",
    "TestingProfile",
    "StagingProfile",
    "PROFILES",
    "load_config_for_environment",
    "get_current_environment",
    "is_production",
    "is_development",
    "is_testing",
]