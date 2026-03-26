"""
TrendMaster Common Utilities Package.
Path: src/common/__init__.py
"""

from .retry import RetryHandler, RetryConfig
from .exceptions import (
    TrendMasterError,
    APIError,
    APIConnectionError,
    APITimeoutError,
    APIResponseError,
    DatabaseError,
    DatabaseConnectionError,
    DatabaseQueryError,
    ProcessingError,
    ConfigurationError,
)

# Explicitly defining available classes for clean 'from common import *' support
__all__ = [
    # Resilience & Logic
    "RetryHandler",
    "RetryConfig",

    # Base Exception
    "TrendMasterError",

    # API Exceptions
    "APIError",
    "APIConnectionError",
    "APITimeoutError",
    "APIResponseError",

    # Database Exceptions
    "DatabaseError",
    "DatabaseConnectionError",
    "DatabaseQueryError",

    # System Exceptions
    "ProcessingError",
    "ConfigurationError",
]
