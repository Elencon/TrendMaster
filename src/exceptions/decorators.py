"""
Decorators for automatic exception handling and conversion.
Wraps functions to catch generic exceptions and re-raise as typed ETL exceptions.
"""

import functools
from typing import Callable

from .api_exceptions import APIError
from .base_exceptions import ETLException, ErrorContext
from .exception_factories import create_database_error
from .processing_exceptions import ProcessingError
from .system_exceptions import FileSystemError, MemoryError

# ---------------------------------------------------------------------------
# Keyword sets used to classify unknown exceptions by message content
# ---------------------------------------------------------------------------

_DB_KEYWORDS   = frozenset({"mysql", "database", "connection", "sql"})
_API_KEYWORDS  = frozenset({"http", "api", "request", "response"})
_FILE_KEYWORDS = frozenset({"file", "directory", "path"})

# ---------------------------------------------------------------------------
# Decorator
# ---------------------------------------------------------------------------

def handle_etl_exceptions(operation_name: str, component: str = "unknown") -> Callable:
    """
    Decorator that catches non-ETL exceptions and re-raises them as typed
    ETL exceptions with context.

    ETLException subclasses are re-raised unchanged.
    All other exceptions are classified by inspecting the error message and
    converted to the most appropriate ETL exception type.

    Args:
        operation_name: Label for the operation, stored in ErrorContext.
        component:      Component name, stored in ErrorContext.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except ETLException:
                raise
            except Exception as exc:
                context = ErrorContext(
                    operation=operation_name,
                    component=component,
                )
                msg = str(exc)
                lower = msg.lower()

                if _DB_KEYWORDS & set(lower.split()):
                    raise create_database_error(
                        f"Database error in {operation_name}: {msg}",
                        original_exception=exc,
                        context=context,
                    ) from exc

                if _API_KEYWORDS & set(lower.split()):
                    raise APIError(
                        f"API error in {operation_name}: {msg}",
                        original_exception=exc,
                        context=context,
                    ) from exc

                if _FILE_KEYWORDS & set(lower.split()):
                    raise FileSystemError(
                        f"File system error in {operation_name}: {msg}",
                        original_exception=exc,
                        context=context,
                    ) from exc

                if "memory" in lower:
                    raise MemoryError(
                        f"Memory error in {operation_name}: {msg}",
                        original_exception=exc,
                        context=context,
                    ) from exc

                raise ProcessingError(
                    f"Processing error in {operation_name}: {msg}",
                    original_exception=exc,
                    context=context,
                ) from exc

        return wrapper
    return decorator
