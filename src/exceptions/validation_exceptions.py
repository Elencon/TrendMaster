"""
Data validation exception classes.
Handles schema validation, data quality, and validation rule errors.
"""

from typing import Dict, List, Optional

from .base_exceptions import ETLException, ErrorCategory, ErrorContext, ErrorSeverity

# ---------------------------------------------------------------------------
# Default recovery suggestions
# ---------------------------------------------------------------------------

_VALIDATION_SUGGESTIONS: List[str] = [
    "Review data quality and format requirements",
    "Check for null values in required fields",
    "Validate data types and ranges",
    "Apply data cleansing rules",
    "Update validation rules if needed",
]

_SCHEMA_SUGGESTIONS: List[str] = [
    "Verify column names match expected schema",
    "Check data types are compatible",
    "Ensure required columns are present",
    "Review schema documentation",
    "Update data mapping configuration",
]

_DATA_QUALITY_SUGGESTIONS: List[str] = [
    "Apply data cleansing transformations",
    "Remove or fix invalid records",
    "Update validation thresholds",
    "Implement data normalisation",
    "Review data source quality",
]

# ---------------------------------------------------------------------------
# Exception classes
# ---------------------------------------------------------------------------

class ValidationError(ETLException):
    """Data validation errors."""

    def __init__(
        self,
        message: str,
        error_code:       str                      = "VALIDATION_ERROR",
        failed_records:   Optional[List[Dict]]     = None,
        validation_rules: Optional[List[str]]      = None,
        **kwargs,
    ) -> None:
        """
        Args:
            message:          Error message.
            error_code:       Validation-specific error code.
            failed_records:   Records that failed validation.
            validation_rules: Names of the rules that were violated.
            **kwargs:         Forwarded to ETLException.
        """
        kwargs.setdefault("category", ErrorCategory.VALIDATION)
        kwargs.setdefault("severity", ErrorSeverity.MEDIUM)
        kwargs.setdefault("recovery_suggestions", _VALIDATION_SUGGESTIONS)

        context = kwargs.get("context", ErrorContext())
        if failed_records:
            context.additional_data["failed_records"] = failed_records
        if validation_rules:
            context.additional_data["validation_rules"] = validation_rules
        kwargs["context"] = context

        super().__init__(message, error_code, **kwargs)


class SchemaValidationError(ValidationError):
    """Schema validation specific errors."""

    def __init__(self, message: str, **kwargs) -> None:
        kwargs.setdefault("error_code", "SCHEMA_VALIDATION_ERROR")
        kwargs.setdefault("recovery_suggestions", _SCHEMA_SUGGESTIONS)
        super().__init__(message, **kwargs)


class DataQualityError(ValidationError):
    """Data quality specific errors."""

    def __init__(self, message: str, **kwargs) -> None:
        kwargs.setdefault("error_code", "DATA_QUALITY_ERROR")
        kwargs.setdefault("recovery_suggestions", _DATA_QUALITY_SUGGESTIONS)
        super().__init__(message, **kwargs)
