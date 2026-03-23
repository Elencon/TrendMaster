"""
Base exception classes and core enums for ETL operations.
Provides the foundation for all ETL-specific exceptions.
"""

import traceback
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class ErrorSeverity(Enum):
    """Error severity levels."""
    LOW      = "low"
    MEDIUM   = "medium"
    HIGH     = "high"
    CRITICAL = "critical"


class ErrorCategory(Enum):
    """Error category types."""
    DATABASE       = "database"
    VALIDATION     = "validation"
    CONFIGURATION  = "configuration"
    API            = "api"
    PROCESSING     = "processing"
    AUTHENTICATION = "authentication"
    NETWORK        = "network"
    FILE_SYSTEM    = "file_system"
    MEMORY         = "memory"


@dataclass
class ErrorContext:
    """Context information attached to an ETL exception."""

    operation:       str                = ""
    component:       str                = ""
    table_name:      Optional[str]      = None
    file_path:       Optional[str]      = None
    record_count:    Optional[int]      = None
    additional_data: Dict[str, Any]     = field(default_factory=dict)
    timestamp:       datetime           = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-serialisable representation."""
        return {
            "operation":       self.operation,
            "component":       self.component,
            "table_name":      self.table_name,
            "file_path":       self.file_path,
            "record_count":    self.record_count,
            "additional_data": self.additional_data,
            "timestamp":       self.timestamp.isoformat(),
        }


class ETLException(Exception):
    """Base exception for all ETL operations."""

    def __init__(
        self,
        message: str,
        error_code: str                        = "ETL_UNKNOWN",
        severity:   ErrorSeverity              = ErrorSeverity.MEDIUM,
        category:   ErrorCategory             = ErrorCategory.PROCESSING,
        context:    Optional[ErrorContext]     = None,
        recovery_suggestions: Optional[List[str]] = None,
        original_exception:   Optional[Exception] = None,
    ) -> None:
        """
        Initialise an ETL exception.

        Args:
            message:              Human-readable error description.
            error_code:           Machine-readable unique code (e.g. "DB_CONNECTION_ERROR").
            severity:             How severe the error is.
            category:             Which subsystem the error belongs to.
            context:              Structured context (operation, component, etc.).
            recovery_suggestions: Ordered list of remediation hints.
            original_exception:   Underlying exception that caused this one.
        """
        super().__init__(message)
        self.message              = message
        self.error_code           = error_code
        self.severity             = severity
        self.category             = category
        self.context              = context or ErrorContext()
        self.recovery_suggestions = recovery_suggestions or []
        self.original_exception   = original_exception
        self.traceback_info       = (
            traceback.format_exc() if original_exception else None
        )

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-serialisable representation for logging / monitoring."""
        return {
            "error_type":           self.__class__.__name__,
            "message":              self.message,
            "error_code":           self.error_code,
            "severity":             self.severity.value,
            "category":             self.category.value,
            "context":              self.context.to_dict(),
            "recovery_suggestions": self.recovery_suggestions,
            "original_exception":   str(self.original_exception) if self.original_exception else None,
            "traceback":            self.traceback_info,
        }

    def __str__(self) -> str:
        parts = [f"[{self.error_code}] {self.message}"]
        if self.context.operation:
            parts.append(f"(Operation: {self.context.operation})")
        if self.context.component:
            parts.append(f"(Component: {self.context.component})")
        if self.severity in (ErrorSeverity.HIGH, ErrorSeverity.CRITICAL):
            parts.append(f"[SEVERITY: {self.severity.value.upper()}]")
        return " ".join(parts)
