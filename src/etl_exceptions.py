"""
Central namespace for all ETL exceptions.
Provides a convenient shortcut to the exceptions package.
"""

from exceptions import (
    ETLException, ErrorSeverity, ErrorCategory, ErrorContext,
    DatabaseError, ConnectionError, QueryError,
    ValidationError, SchemaValidationError, DataQualityError,
    ProcessingError,
    ConfigurationError, FileSystemError, MemoryError,
    APIError,
    handle_etl_exceptions,
    create_database_error, create_validation_error, create_api_error
)

# Explicitly control what's public
__all__ = [
    "ETLException", "ErrorSeverity", "ErrorCategory", "ErrorContext",
    "DatabaseError", "ConnectionError", "QueryError",
    "ValidationError", "SchemaValidationError", "DataQualityError",
    "ProcessingError",
    "ConfigurationError", "FileSystemError", "MemoryError",
    "APIError",
    "handle_etl_exceptions",
    "create_database_error", "create_validation_error", "create_api_error",
]