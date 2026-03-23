"""
ETL exception handling package.

Provides a comprehensive exception hierarchy for ETL operations with
structured context, recovery suggestions, and typed error handling.
"""

from .base_exceptions import ErrorCategory, ErrorContext, ErrorSeverity, ETLException
from .database_exceptions import ConnectionError, DatabaseError, QueryError
from .validation_exceptions import DataQualityError, SchemaValidationError, ValidationError
from .api_exceptions import APIError
from .processing_exceptions import ProcessingError
from .system_exceptions import ConfigurationError, FileSystemError, MemoryError
from .exception_factories import create_api_error, create_database_error, create_validation_error
from .decorators import handle_etl_exceptions

__all__ = [
    # Core
    "ETLException",
    "ErrorSeverity",
    "ErrorCategory",
    "ErrorContext",
    # Database
    "DatabaseError",
    "ConnectionError",
    "QueryError",
    # Validation
    "ValidationError",
    "SchemaValidationError",
    "DataQualityError",
    # API
    "APIError",
    # Processing
    "ProcessingError",
    # System
    "ConfigurationError",
    "FileSystemError",
    "MemoryError",
    # Factories
    "create_database_error",
    "create_validation_error",
    "create_api_error",
    # Decorators
    "handle_etl_exceptions",
]
