r"""
C:\Economy\Invest\TrendMaster\src\exceptions\exception_factories.py
Factory functions for creating exception instances.
Provides convenient construction of appropriate exceptions based on context.
"""

from typing import Optional

from .api_exceptions import APIError
from .database_exceptions import ConnectionError, DatabaseError, QueryError
from .validation_exceptions import DataQualityError, SchemaValidationError, ValidationError

# ---------------------------------------------------------------------------
# Error-code map for APIError factory
# ---------------------------------------------------------------------------

_API_ERROR_CODES = {
    401: "API_UNAUTHORIZED",
    403: "API_FORBIDDEN",
    404: "API_NOT_FOUND",
    429: "API_RATE_LIMITED",
}

# ---------------------------------------------------------------------------
# Factory functions
# ---------------------------------------------------------------------------

def create_database_error(
    message: str,
    original_exception: Optional[Exception] = None,
    **kwargs,
) -> DatabaseError:
    """
    Return the most specific DatabaseError subclass for the given exception.

    Inspects *original_exception* to choose between ConnectionError,
    QueryError, and the generic DatabaseError.
    """
    if original_exception:
        kwargs["original_exception"] = original_exception
        error_str = str(original_exception).lower()
        if "connection" in error_str or "connect" in error_str:
            return ConnectionError(message, **kwargs)
        if "syntax" in error_str or "query" in error_str:
            return QueryError(message, **kwargs)

    return DatabaseError(message, **kwargs)


def create_validation_error(
    message: str,
    validation_type: str = "general",
    **kwargs,
) -> ValidationError:
    """
    Return the most specific ValidationError subclass for the given type.

    Args:
        validation_type: One of "schema", "data_quality", or "general".
    """
    if validation_type == "schema":
        return SchemaValidationError(message, **kwargs)
    if validation_type == "data_quality":
        return DataQualityError(message, **kwargs)
    return ValidationError(message, **kwargs)


def create_api_error(
    message: str,
    status_code: Optional[int] = None,
    **kwargs,
) -> APIError:
    """
    Return an APIError with an error code derived from *status_code*.

    Known status codes (401, 403, 404, 429) get named codes;
    5xx responses get "API_SERVER_ERROR"; others get "API_HTTP_<code>".
    """
    if status_code:
        if status_code >= 500:
            default_code = "API_SERVER_ERROR"
        else:
            default_code = _API_ERROR_CODES.get(status_code, f"API_HTTP_{status_code}")
        kwargs.setdefault("error_code", default_code)

    return APIError(message, status_code=status_code, **kwargs)
