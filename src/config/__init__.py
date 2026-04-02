r"""
C:\Economy\Invest\TrendMaster\src\config\__init__.py
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
    CACHE_PATH,
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
    get_default_config,
    get_config,
    set_config,
    reset_config,
)

# ---------------------------------------------------------
# Specialized Database configurations
# ---------------------------------------------------------
from .database import MySQLConfig

# ---------------------------------------------------------
# Specialized API configurations
# ---------------------------------------------------------
from .api import (
    RESTAPIConfig,
    GraphQLAPIConfig,
    AsyncAPIConfig,
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
    "CACHE_PATH",
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
    "get_default_config",
    "get_config",
    "set_config",
    "reset_config",

    # Environment Management  (functionality in environmets)
    "ConfigProfile",
    "DevelopmentProfile",
    "ProductionProfile",
    "TestingProfile",
    "StagingProfile",
    "PROFILES",
]
