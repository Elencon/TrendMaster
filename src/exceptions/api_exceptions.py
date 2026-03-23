"""
API-related exception classes.
Handles HTTP requests, API responses, and external service errors.
"""

from typing import Dict, List, Optional

from .base_exceptions import ETLException, ErrorCategory, ErrorContext, ErrorSeverity

# ---------------------------------------------------------------------------
# Default recovery suggestions
# ---------------------------------------------------------------------------

_API_BASE_SUGGESTIONS: List[str] = [
    "Check API endpoint URL and method",
    "Verify API authentication credentials",
    "Review request parameters and headers",
    "Check network connectivity",
    "Implement retry logic with exponential backoff",
]

_STATUS_SUGGESTIONS: Dict[int, str] = {
    401: "Update or refresh authentication token",
    403: "Verify API permissions and access rights",
    404: "Confirm API endpoint exists and is accessible",
    429: "Implement rate limiting and backoff strategy",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _severity_for_status(status_code: int) -> ErrorSeverity:
    if status_code >= 500:
        return ErrorSeverity.HIGH
    if status_code >= 400:
        return ErrorSeverity.MEDIUM
    return ErrorSeverity.LOW


def _suggestions_for_status(status_code: int) -> List[str]:
    """Return a copy of the base suggestions with a status-specific hint prepended."""
    suggestions = list(_API_BASE_SUGGESTIONS)
    hint = _STATUS_SUGGESTIONS.get(status_code)
    if hint is None and status_code >= 500:
        hint = "Server error - retry after waiting period"
    if hint:
        suggestions.insert(0, hint)
    return suggestions


# ---------------------------------------------------------------------------
# Exception class
# ---------------------------------------------------------------------------

class APIError(ETLException):
    """API-related errors."""

    def __init__(
        self,
        message: str,
        error_code:    str                  = "API_ERROR",
        status_code:   Optional[int]        = None,
        endpoint:      Optional[str]        = None,
        response_data: Optional[Dict]       = None,
        **kwargs,
    ) -> None:
        """
        Args:
            message:       Error message.
            error_code:    API-specific error code.
            status_code:   HTTP status code.
            endpoint:      API endpoint that failed.
            response_data: API response payload.
            **kwargs:      Forwarded to ETLException.
        """
        kwargs.setdefault("category", ErrorCategory.API)
        kwargs.setdefault(
            "severity",
            _severity_for_status(status_code) if status_code else ErrorSeverity.MEDIUM,
        )
        kwargs.setdefault(
            "recovery_suggestions",
            _suggestions_for_status(status_code) if status_code else list(_API_BASE_SUGGESTIONS),
        )

        context = kwargs.get("context", ErrorContext())
        if status_code:
            context.additional_data["status_code"] = status_code
        if endpoint:
            context.additional_data["endpoint"] = endpoint
        if response_data:
            context.additional_data["response_data"] = response_data
        kwargs["context"] = context

        super().__init__(message, error_code, **kwargs)
