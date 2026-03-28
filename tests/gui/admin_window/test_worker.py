import pytest
from unittest.mock import MagicMock, patch
from PySide6.QtCore import QCoreApplication
import sys
import time

# Create a QCoreApplication instance if one doesn't exist
app = QCoreApplication.instance()
if app is None:
    app = QCoreApplication(sys.argv)

from src.gui.admin_window.worker import ETLWorker

def wait_for_worker(worker, timeout=2.0):
    start = time.time()
    while worker.isRunning() and time.time() - start < timeout:
        app.processEvents()
        time.sleep(0.01)
    app.processEvents()  # final pump for signals

@pytest.fixture
def mock_db_manager():
    with patch('src.gui.admin_window.worker.DatabaseManager') as MockDB:
        mock_instance = MagicMock()
        MockDB.return_value = mock_instance
        yield mock_instance

@pytest.fixture
def mock_api_client():
    with patch('src.gui.admin_window.worker.APIClient') as MockAPI:
        mock_instance = MagicMock()
        MockAPI.return_value = mock_instance
        yield mock_instance

def run_worker(worker):
    result = {"finished": [], "error": []}
    worker.finished.connect(lambda msg: result["finished"].append(msg))
    worker.error.connect(lambda msg: result["error"].append(msg))
    worker.start()
    wait_for_worker(worker)
    return result

def test_worker_unknown_operation():
    """Test worker handles unknown operations."""
    worker = ETLWorker("unknown_op")
    result = run_worker(worker)
    assert len(result["error"]) > 0
    assert "Unknown operation" in result["error"][0]
    
def test_worker_test_connection_success(mock_db_manager):
    """Test database connection success."""
    mock_db_manager.test_connection.return_value = True
    
    worker = ETLWorker("test_connection")
    result = run_worker(worker)
    assert len(result["finished"]) > 0
    assert "Database connection successful!" in result["finished"][0]

def test_worker_test_connection_failure(mock_db_manager):
    """Test database connection failure."""
    mock_db_manager.test_connection.return_value = False
    
    worker = ETLWorker("test_connection")
    result = run_worker(worker)
    assert len(result["error"]) > 0
    assert "Failed to connect to database" in result["error"][0]

def test_worker_test_api_success(mock_api_client):
    """Test API connection success."""
    mock_api_client.fetch_data.return_value = [{"id": 1}]
    
    worker = ETLWorker("test_api", "http://test.com/api")
    result = run_worker(worker)
    assert len(result["finished"]) > 0
    assert "API connection successful" in result["finished"][0]

def test_worker_test_api_failure(mock_api_client):
    """Test API connection failure."""
    mock_api_client.fetch_data.return_value = None
    
    worker = ETLWorker("test_api", "http://test.com/api")
    result = run_worker(worker)
    assert len(result["error"]) > 0
    assert "API connection failed - no data received" in result["error"][0]

def test_worker_create_tables(mock_db_manager):
    """Test table creation."""
    mock_db_manager.create_database_if_not_exists.return_value = True
    mock_db_manager.create_all_tables_from_csv.return_value = True
    mock_db_manager.get_row_count.return_value = 0
    
    worker = ETLWorker("create_tables")
    result = run_worker(worker)
    assert len(result["finished"]) > 0
    assert "All 9 database tables created successfully!" in result["finished"][0]
    
def test_worker_test_csv_access(mock_db_manager):
    """Test CSV file access testing."""
    mock_db_manager.csv_files = {"table1": "file1.csv"}
    mock_df = MagicMock()
    mock_df.__len__.return_value = 10
    mock_df.columns = ["a", "b"]
    mock_db_manager.read_csv_file.return_value = mock_df
    
    worker = ETLWorker("test_csv_access")
    result = run_worker(worker)
    assert len(result["finished"]) > 0
    assert "CSV access test completed!" in result["finished"][0]

@patch('src.gui.admin_window.worker.shutil.copy2')
def test_worker_select_csv_files(mock_copy):
    """Test copying CSV files."""
    worker = ETLWorker("select_csv_files", ["file1.csv", "file2.csv"])
    result = run_worker(worker)
        
    assert mock_copy.call_count == 2
    assert len(result["finished"]) > 0
    assert "Successfully copied 2 files" in result["finished"][0]
