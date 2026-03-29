import pytest
from unittest.mock import MagicMock

import sys
from pathlib import Path

# Add src to python path for testing
src_path = Path("c:/Economy/Invest/TrendMaster/src")
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from database.schema_manager import SchemaManager

@pytest.fixture
def mock_db_connection():
    """Mock database connection for SchemaManager tests."""
    conn_mock = MagicMock()
    # Support connection as context manager
    conn_mock.get_connection.return_value.__enter__.return_value = conn_mock
    # Provide an engine for inspection
    conn_mock.engine = MagicMock()
    return conn_mock

def test_schema_manager_init(mock_db_connection):
    """Test initialization with explicit schema definitions."""
    schemas = {"users": "CREATE TABLE users (id INT)"}
    sm = SchemaManager(mock_db_connection, schema_definitions=schemas)

    assert sm.schema_definitions == schemas
    assert "users" in sm.schema_definitions

def test_create_table(mock_db_connection):
    """Test DDL execution when schema exists."""
    schemas = {"users": "CREATE TABLE users (id INT)"}
    sm = SchemaManager(mock_db_connection, schema_definitions=schemas)

    # Needs to return True for success
    result = sm.create_table("users")
    assert result is True

    # Assert get_connection and execute were called
    mock_db_connection.get_connection.assert_called()
    mock_db_connection.execute.assert_called()

    # Assert commit was called
    mock_db_connection.commit.assert_called()

def test_create_table_not_found(mock_db_connection):
    """Test behavior when schema doesn't exist."""
    sm = SchemaManager(mock_db_connection, schema_definitions={})

    result = sm.create_table("unknown_table")
    assert result is False
    mock_db_connection.execute.assert_not_called()

def test_create_all_tables(mock_db_connection):
    """Test bulk table creation respecting order."""
    schemas = {
        "b_table": "CREATE TABLE b_table (id INT)",
        "a_table": "CREATE TABLE a_table (id INT)"
    }

    # Pass explicit order
    order = ["a_table", "b_table"]
    sm = SchemaManager(mock_db_connection, schema_definitions=schemas, import_order=order)

    result = sm.create_all_tables()
    assert result is True
    assert mock_db_connection.execute.call_count == 2
