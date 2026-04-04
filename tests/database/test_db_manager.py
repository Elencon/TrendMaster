import pytest
from unittest.mock import patch, MagicMock

import sys
from pathlib import Path

# Add src to python path for testing
src_path = Path("c:/Economy/Invest/TrendMaster/src")
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from database.db_manager import DatabaseManager

@pytest.fixture
def manager_mocks():
    """Mock internal dependencies of DatabaseManager."""
    with patch("database.db_manager.DatabaseManager") as mock_cm, \
         patch("database.schema_manager.SchemaManager") as mock_sm:

        # Setup basic mock engine and connection
        instance_cm = mock_cm.return_value
        instance_cm.engine = MagicMock()
        instance_cm.get_connection.return_value.__enter__.return_value = MagicMock()

        yield mock_cm, mock_sm

def test_database_manager_init(manager_mocks, mock_config):
    """Test initialization parameter resolution."""
    mock_cm, mock_sm = manager_mocks

    dm = DatabaseManager(config=mock_config, pool_size=15)

    assert dm.config == mock_config
    # Check that DatabaseManager was instantiated properly
    mock_cm.assert_called_once_with(mock_config, pool_size=15, echo=False)
    # Check SchemaManager instantiation
    mock_sm.assert_called_once()

def test_test_connection(manager_mocks, mock_config):
    """Test test_connection wrapper method."""
    mock_cm, _ = manager_mocks
    dm = DatabaseManager(config=mock_config)

    # Success scenario is mocked by default
    assert dm.test_connection() is True

    # Exception scenario
    dm.engine.connect.side_effect = Exception("DB Down")
    assert dm.test_connection() is False

def test_close_connections(manager_mocks, mock_config):
    """Test connection disposal proxy."""
    mock_cm, _ = manager_mocks
    cm_instance = mock_cm.return_value

    dm = DatabaseManager(config=mock_config)
    dm.close_connections()

    cm_instance.dispose.assert_called_once()
