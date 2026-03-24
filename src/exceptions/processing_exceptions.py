r"""
C:\Economy\Invest\TrendMaster\src\exceptions\processing_exceptions.py
Data processing exception classes.
Handles data transformation, ETL pipeline, and processing stage errors.
"""

from typing import List, Optional

from .base_exceptions import ETLException, ErrorCategory, ErrorContext, ErrorSeverity

# ---------------------------------------------------------------------------
# Default recovery suggestions
# ---------------------------------------------------------------------------

_PROCESSING_SUGGESTIONS: List[str] = [
    "Review data transformation logic",
    "Check for memory or resource constraints",
    "Validate input data format and structure",
    "Implement error handling for edge cases",
    "Consider batch processing for large datasets",
]

# ---------------------------------------------------------------------------
# Exception class
# ---------------------------------------------------------------------------

class ProcessingError(ETLException):
    """Data processing errors."""

    def __init__(
        self,
        message: str,
        error_code:       str           = "PROCESSING_ERROR",
        processing_stage: Optional[str] = None,
        **kwargs,
    ) -> None:
        """
        Args:
            message:          Error message.
            error_code:       Processing-specific error code.
            processing_stage: Pipeline stage that failed (e.g. "transform", "load").
            **kwargs:         Forwarded to ETLException.
        """
        kwargs.setdefault("category", ErrorCategory.PROCESSING)
        kwargs.setdefault("severity", ErrorSeverity.MEDIUM)
        kwargs.setdefault("recovery_suggestions", _PROCESSING_SUGGESTIONS)

        context = kwargs.get("context", ErrorContext())
        if processing_stage:
            context.additional_data["processing_stage"] = processing_stage
        kwargs["context"] = context

        super().__init__(message, error_code, **kwargs)
