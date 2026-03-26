r"""
C:\Economy\Invest\TrendMaster\src\exceptions\system_exceptions.py
System-related exception classes.
Handles configuration, file system, memory, and infrastructure errors.
"""

from typing import List, Optional

from .base_exceptions import ETLException, ErrorCategory, ErrorContext, ErrorSeverity

# ---------------------------------------------------------------------------
# Default recovery suggestions
# ---------------------------------------------------------------------------

_CONFIG_SUGGESTIONS: List[str] = [
    "Review configuration file syntax",
    "Verify all required configuration keys are present",
    "Check configuration value types and formats",
    "Validate environment-specific settings",
    "Consult configuration documentation",
]

_FILE_SYSTEM_SUGGESTIONS: List[str] = [
    "Verify file path exists and is accessible",
    "Check file permissions (read/write access)",
    "Ensure sufficient disk space",
    "Validate file format and encoding",
    "Check for file locks or concurrent access",
]

_MEMORY_SUGGESTIONS: List[str] = [
    "Implement chunked processing for large datasets",
    "Optimise data types to reduce memory usage",
    "Clear unnecessary variables and force garbage collection",
    "Increase available memory or use memory-efficient algorithms",
    "Consider streaming processing instead of loading all data",
]

# ---------------------------------------------------------------------------
# Exception classes
# ---------------------------------------------------------------------------

class ConfigurationError(ETLException):
    """Configuration-related errors."""

    def __init__(
        self,
        message: str,
        error_code:     str                  = "CONFIG_ERROR",
        config_section: Optional[str]        = None,
        invalid_keys:   Optional[List[str]]  = None,
        **kwargs,
    ) -> None:
        """
        Args:
            message:        Error message.
            error_code:     Configuration-specific error code.
            config_section: Configuration section that failed.
            invalid_keys:   List of invalid configuration keys.
            **kwargs:       Forwarded to ETLException.
        """
        kwargs.setdefault("category", ErrorCategory.CONFIGURATION)
        kwargs.setdefault("severity", ErrorSeverity.HIGH)
        kwargs.setdefault("recovery_suggestions", _CONFIG_SUGGESTIONS)

        context = kwargs.get("context", ErrorContext())
        if config_section:
            context.additional_data["config_section"] = config_section
        if invalid_keys:
            context.additional_data["invalid_keys"] = invalid_keys
        kwargs["context"] = context

        super().__init__(message, error_code, **kwargs)


class FileSystemError(ETLException):
    """File system related errors."""

    def __init__(
        self,
        message: str,
        error_code: str           = "FILE_SYSTEM_ERROR",
        file_path:  Optional[str] = None,
        **kwargs,
    ) -> None:
        """
        Args:
            message:    Error message.
            error_code: File system-specific error code.
            file_path:  Path to the file that caused the error.
            **kwargs:   Forwarded to ETLException.
        """
        kwargs.setdefault("category", ErrorCategory.FILE_SYSTEM)
        kwargs.setdefault("severity", ErrorSeverity.MEDIUM)
        kwargs.setdefault("recovery_suggestions", _FILE_SYSTEM_SUGGESTIONS)

        context = kwargs.get("context", ErrorContext())
        if file_path:
            context.file_path = file_path
        kwargs["context"] = context

        super().__init__(message, error_code, **kwargs)


class MemoryError(ETLException):
    """Memory-related errors."""

    def __init__(
        self,
        message: str,
        error_code:      str            = "MEMORY_ERROR",
        memory_usage_mb: Optional[float] = None,
        **kwargs,
    ) -> None:
        """
        Args:
            message:         Error message.
            error_code:      Memory-specific error code.
            memory_usage_mb: Current memory usage in MB at the time of the error.
            **kwargs:        Forwarded to ETLException.
        """
        kwargs.setdefault("category", ErrorCategory.MEMORY)
        kwargs.setdefault("severity", ErrorSeverity.HIGH)
        kwargs.setdefault("recovery_suggestions", _MEMORY_SUGGESTIONS)

        context = kwargs.get("context", ErrorContext())
        if memory_usage_mb is not None:
            context.additional_data["memory_usage_mb"] = memory_usage_mb
        kwargs["context"] = context

        super().__init__(message, error_code, **kwargs)
