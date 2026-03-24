r"""
C:\Economy\Invest\TrendMaster\src\exceptions\database_exceptions.py
Database-related exception classes.
Handles database connection, query execution, and transaction errors.
"""

from typing import Any, Dict, List, Optional

from .base_exceptions import ETLException, ErrorCategory, ErrorContext, ErrorSeverity

# ---------------------------------------------------------------------------
# Default recovery suggestions
# ---------------------------------------------------------------------------

_DB_SUGGESTIONS: List[str] = [
    "Check database connectivity and credentials",
    "Verify SQL syntax and table/column names",
    "Check database permissions",
    "Ensure database server is running and accessible",
    "Review connection pool settings",
]

_CONNECTION_SUGGESTIONS: List[str] = [
    "Verify database server is running",
    "Check network connectivity",
    "Validate connection credentials",
    "Review firewall settings",
    "Check connection timeout settings",
]

_QUERY_SUGGESTIONS: List[str] = [
    "Review SQL syntax for errors",
    "Verify table and column names exist",
    "Check data types in WHERE clauses",
    "Ensure proper JOIN conditions",
    "Validate user permissions for the operation",
]

# ---------------------------------------------------------------------------
# Exception classes
# ---------------------------------------------------------------------------

class DatabaseError(ETLException):
    """Database-related errors."""

    def __init__(
        self,
        message: str,
        error_code:      str                       = "DB_ERROR",
        connection_info: Optional[Dict[str, Any]]  = None,
        sql_query:       Optional[str]             = None,
        **kwargs,
    ) -> None:
        """
        Args:
            message:         Error message.
            error_code:      Database-specific error code.
            connection_info: Database connection information.
            sql_query:       SQL query that caused the error.
            **kwargs:        Forwarded to ETLException.
        """
        kwargs.setdefault("category", ErrorCategory.DATABASE)
        kwargs.setdefault("severity", ErrorSeverity.HIGH)
        kwargs.setdefault("recovery_suggestions", _DB_SUGGESTIONS)

        context = kwargs.get("context", ErrorContext())
        if connection_info:
            context.additional_data["connection_info"] = connection_info
        if sql_query:
            context.additional_data["sql_query"] = sql_query
        kwargs["context"] = context

        super().__init__(message, error_code, **kwargs)


class ConnectionError(DatabaseError):
    """Database connection-specific errors."""

    def __init__(self, message: str, **kwargs) -> None:
        kwargs.setdefault("error_code", "DB_CONNECTION_ERROR")
        kwargs.setdefault("severity", ErrorSeverity.CRITICAL)
        kwargs.setdefault("recovery_suggestions", _CONNECTION_SUGGESTIONS)
        super().__init__(message, **kwargs)


class QueryError(DatabaseError):
    """SQL query execution errors."""

    def __init__(self, message: str, **kwargs) -> None:
        kwargs.setdefault("error_code", "DB_QUERY_ERROR")
        kwargs.setdefault("recovery_suggestions", _QUERY_SUGGESTIONS)
        super().__init__(message, **kwargs)
