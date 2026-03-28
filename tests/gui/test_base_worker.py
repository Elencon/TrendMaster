import pytest
from PySide6.QtWidgets import QApplication
import sys
import time

from src.gui.base_worker import BaseWorker

# Create a QApplication instance if one doesn't exist
app = QApplication.instance()
if app is None:
    app = QApplication(sys.argv)

def wait_for_worker(worker, timeout=2.0):
    start = time.time()
    while worker.isRunning() and time.time() - start < timeout:
        app.processEvents()
        time.sleep(0.01)
    app.processEvents()  # final pump for signals

class DummyWorker(BaseWorker):
    def __init__(self, operation: str, *args, **kwargs):
        super().__init__(operation, *args, **kwargs)
        self._operations["test_op"] = self.do_test_op
        self._operations["error_op"] = self.do_error_op

    def do_test_op(self, value):
        return f"result: {value}"

    def do_error_op(self):
        raise ValueError("Simulated error")

def test_base_worker_success():
    """Test successful operation execution."""
    worker = DummyWorker("test_op", "hello")
    result = []
    
    def on_finished(val):
        result.append(val)
        
    worker.finished.connect(on_finished)
    worker.start()
    
    wait_for_worker(worker)
    assert result == ["result: hello"]

def test_base_worker_unknown_operation():
    """Test behavior with an unknown operation."""
    worker = DummyWorker("unknown_op")
    result = []
    
    def on_error(val):
        result.append(val)
        
    worker.error.connect(on_error)
    worker.start()
    
    wait_for_worker(worker)
    assert len(result) > 0
    assert "Unknown operation" in result[0]
    
def test_base_worker_exception():
    """Test exception handling during operation."""
    worker = DummyWorker("error_op")
    result = []
    
    def on_error(val):
        result.append(val)
        
    worker.error.connect(on_error)
    worker.start()
    
    wait_for_worker(worker)
    assert len(result) > 0
    assert "Error in error_op" in result[0]
    assert "Simulated error" in result[0]
    
def test_base_worker_cancellation():
    """Test cancellation of worker."""
    worker = DummyWorker("test_op", "hello")
    worker.cancel()
    
    assert worker._check_cancelled() is True
    
    worker.start()
    wait_for_worker(worker)
