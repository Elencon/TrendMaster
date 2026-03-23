"""Base worker class for background operations"""

from PySide6.QtCore import QThread, Signal
from typing import Callable, Dict


class BaseWorker(QThread):
    """Base worker thread with common functionality"""
    
    # Common signals
    finished = Signal(str)
    error = Signal(str)
    progress = Signal(str)
    
    def __init__(self, operation: str, *args, **kwargs):
        super().__init__()
        self.operation = operation
        self.args = args
        self.kwargs = kwargs
        self._is_cancelled = False
        self._operations: Dict[str, Callable] = {}
    
    def cancel(self):
        """Request cancellation of the operation"""
        self._is_cancelled = True
    
    def run(self):
        """Main execution method with operation routing"""
        try:
            if self.operation in self._operations:
                self._operations[self.operation]()
            else:
                self.error.emit(f"Unknown operation: {self.operation}")
        except Exception as e:
            self.error.emit(f"Error in {self.operation}: {str(e)}")
