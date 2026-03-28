"""
Base worker class for background operations
"""

from PySide6.QtCore import QThread, Signal
from typing import Callable, Dict, Any

class BaseWorker(QThread):
    """Base worker thread with common functionality."""

    # Common signals
    finished = Signal(str)
    error = Signal(str)
    progress = Signal(str)

    def __init__(self, operation: str, *args, **kwargs):
        super().__init__()

        # Protected internal state
        self._operation = operation
        self._args = args
        self._kwargs = kwargs
        self._is_cancelled = False

        # Operation registry: name → callable
        self._operations: Dict[str, Callable[..., Any]] = {}

    # ---------------------------------------------------------
    # Public API
    # ---------------------------------------------------------
    def cancel(self):
        """Request cancellation of the operation."""
        self._is_cancelled = True

    # ---------------------------------------------------------
    # Protected helpers
    # ---------------------------------------------------------
    def _check_cancelled(self) -> bool:
        """Return True if the worker has been cancelled."""
        return self._is_cancelled

    def _emit_error(self, message: str):
        """Emit an error signal."""
        self.error.emit(message)

    def _execute_operation(self):
        """Execute the registered operation with args/kwargs."""
        func = self._operations.get(self._operation)

        if not func:
            self._emit_error(f"Unknown operation: {self._operation}")
            return

        return func(*self._args, **self._kwargs)

    # ---------------------------------------------------------
    # Thread entry point
    # ---------------------------------------------------------
    def run(self):
        """Main execution method with operation routing."""
        try:
            result = self._execute_operation()

            if not self._is_cancelled:
                self.finished.emit(str(result) if result is not None else "")
        except Exception as e:
            self._emit_error(f"Error in {self._operation}: {e}")