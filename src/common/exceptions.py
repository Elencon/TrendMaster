"""
TrendMaster Common Exceptions.
Centralized error hierarchy to avoid circular imports and ensure consistent error handling.
Path: src/common/exceptions.py
"""

from typing import Optional, Any

class TrendMasterError(Exception):
    """Base exception for all TrendMaster errors."""
    def __init__(self, message: str, details: Optional[Any] = None):
        super().__init__(message)
        self.message = message
        self.details = details

# ────────────────────────────────────────────────
# API & Network Exceptions
# ────────────────────────────────────────────────

class APIError(TrendMasterError):
    """Base class for API-related failures."""
    pass

class APIConnectionError(APIError):
    """Raised when the remote server is unreachable."""
    pass

class APITimeoutError(APIError):
    """Raised when an API request exceeds the allocated time."""
    pass

class APIResponseError(APIError):
    """Raised when the API returns a non-successful status code (4xx, 5xx)."""
    def __init__(self, message: str, status_code: int, data: Optional[Any] = None):
        super().__init__(message, details={"status_code": status_code, "data": data})
        self.status_code = status_code

# ────────────────────────────────────────────────
# Database Exceptions
# ────────────────────────────────────────────────

class DatabaseError(TrendMasterError):
    """Base class for Database-related failures."""
    pass

class DatabaseConnectionError(DatabaseError):
    """Raised when failing to connect to the MySQL instance."""
    pass

class DatabaseQueryError(DatabaseError):
    """Raised when a SQL execution fails."""
    pass

# ────────────────────────────────────────────────
# ETL & Processing Exceptions
# ────────────────────────────────────────────────

class ProcessingError(TrendMasterError):
    """Raised when data transformation or msgspec conversion fails."""
    pass

class ConfigurationError(TrendMasterError):
    """Raised when environment variables or config settings are missing/invalid."""
    pass
