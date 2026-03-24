"""
C:\Economy\Invest\TrendMaster\src\exceptions\base_exceptions.py
Base exception classes and core enums for ETL operations.
Provides the foundation for all ETL-specific exceptions.
"""

import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
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
    timestamp:       datetime           = field(default_factory=lambda: datetime.now(timezone.utc))

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
        category:   ErrorCategory              = ErrorCategory.PROCESSING,
        context:    Optional[ErrorContext]     = None,
        recovery_suggestions: Optional[List[str]] = None,
        original_exception:   Optional[Exception] = None,
    ) -> None:

        super().__init__(message)
        self.message              = message
        self.error_code           = error_code
        self.severity             = severity
        self.category             = category
        self.context              = context or ErrorContext()
        self.recovery_suggestions = recovery_suggestions or []
        self.original_exception   = original_exception

        # Capture detailed traceback if an original exception exists
        if original_exception:
            self.traceback_info = "".join(
                traceback.format_exception(original_exception)
            )
        else:
            self.traceback_info = None

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
        """Formatted string for console output and basic logging."""
        parts = [f"[{self.error_code}] {self.message}"]
        
        # Add context flags
        if self.context.operation:
            parts.append(f"(Op: {self.context.operation})")
        if self.context.table_name:
            parts.append(f"(Table: {self.context.table_name})")
            
        # Highlight critical issues
        if self.severity in (ErrorSeverity.HIGH, ErrorSeverity.CRITICAL):
            parts.append(f"!!{self.severity.value.upper()}!!")
            
        # Include original cause if present
        if self.original_exception:
            parts.append(f"| Cause: {type(self.original_exception).__name__}: {self.original_exception}")
            
        return " ".join(parts)