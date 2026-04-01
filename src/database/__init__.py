r"""
C:\Economy\Invest\TrendMaster\src\database\__init__.py
Database package - modular components for ETL operations
"""

# Core database management
from .db_manager import DatabaseManager
from .schema_manager import SchemaManager, SCHEMA_DEFINITIONS, TABLE_COLUMNS

# Modular utilities
from src.database.utilities import (
    ConfigUtils,
    DataUtils,
    DatabaseUtils,
    OperationStats,
    safe_operation,
)

# Specialized processors
from .batch_operations import BatchProcessor
from .csv_operations import CSVImporter

# Always-present components
from .connection_manager import DatabaseConnection, ConnectionPool
from .pandas_optimizer import PandasOptimizer, DataFrameChunker
from .data_validator import (
    DataValidator,
    ValidationRule,
    ValidationResult,
    ValidationSeverity,
)
from .data_from_api import APIDataFetcher

# Connection helpers
from .connect import (
    connect_sync,
    connect_async,
    mysql_cursor_sync,
    mysql_cursor_async,
    connect_to_mysql,
    config,
    logger,
)


__all__ = [
    # Core
    "DatabaseManager",

    # Utilities
    "ConfigUtils",
    "DataUtils",
    "DatabaseUtils",
    "OperationStats",
    "safe_operation",

    # Processors
    "BatchProcessor",
    "CSVImporter",

    # Schema
    "SchemaManager",
    "SCHEMA_DEFINITIONS",
    "TABLE_COLUMNS",

    # Always-present components
    "DatabaseConnection",
    "ConnectionPool",
    "PandasOptimizer",
    "DataFrameChunker",

    # Validation
    "DataValidator",
    "ValidationRule",
    "ValidationResult",
    "ValidationSeverity",

    # API data
    "APIDataFetcher",

    # Connection helpers
    "connect_sync",
    "connect_async",
    "mysql_cursor_sync",
    "mysql_cursor_async",
    "connect_to_mysql",
    "config",
    "logger",
]
