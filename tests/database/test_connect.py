import pytest
from unittest.mock import patch, MagicMock

# Mock get_config before importing connect to avoid hanging during module load
mock_db_cfg = MagicMock()
mock_db_cfg.max_retry_attempts = 3
mock_db_cfg.retry_delay = 1
mock_db_cfg.to_dict.return_value = {
    "host": "localhost",
    "user": "test_user",
    "password": "test_password",
    "database": "test_db",
    "port": 3306
}

mock_cfg = MagicMock()
mock_cfg.database = mock_db_cfg

with patch("src.config.get_config", return_value=mock_cfg):
    from src.database.connect import (
        connect_sync,
        connect_async,
        mysql_cursor_sync,
        mysql_cursor_async
    )

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def detect_driver(conn):
    """Return driver name based on connection object."""
    mod = type(conn).__module__.lower()
    if "pymysql" in mod:
        return "PyMySQL"
    return f"Unknown ({mod})"

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_pymysql():
    with patch("src.database.connect.pymysql") as mock:
        yield mock

@pytest.fixture
def mock_retry_handler():
    with patch("src.database.connect.RetryHandler") as mock:
        handler_instance = mock.return_value
        # For sync execute, just call the function
        handler_instance.execute_sync.side_effect = lambda f, *args, **kwargs: f(*args, **kwargs)
        # For async execute, make it an awaitable that calls the function
        async def mock_execute_async(f, *args, **kwargs):
            return f(*args, **kwargs)
        handler_instance.execute_async.side_effect = mock_execute_async
        yield handler_instance

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_detect_driver():
    mock_conn = MagicMock()
    mock_conn.__class__.__module__ = "pymysql.connections"
    assert detect_driver(mock_conn) == "PyMySQL"

    mock_conn.__class__.__module__ = "other.driver"
    assert "Unknown" in detect_driver(mock_conn)


def test_connect_sync(mock_pymysql, mock_retry_handler):
    mock_conn = MagicMock()
    mock_pymysql.connect.return_value = mock_conn

    conn = connect_sync()

    assert conn is mock_conn
    mock_pymysql.connect.assert_called_once()


@pytest.mark.asyncio
async def test_connect_async(mock_pymysql, mock_retry_handler):
    mock_conn = MagicMock()
    mock_pymysql.connect.return_value = mock_conn

    conn = await connect_async()

    assert conn is mock_conn
    mock_pymysql.connect.assert_called_once()


def test_mysql_cursor_sync(mock_pymysql, mock_retry_handler):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_pymysql.connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    with mysql_cursor_sync() as cur:
        assert cur is mock_cursor
        cur.execute("SELECT 1")

    cur.execute.assert_called_with("SELECT 1")
    cur.close.assert_called_once()


@pytest.mark.asyncio
async def test_mysql_cursor_async(mock_pymysql, mock_retry_handler):
    """Ensures that async cursor wrapper behaves like a proper async context manager."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_pymysql.connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    async with mysql_cursor_async() as cur:
        assert cur is mock_cursor
        cur.execute("SELECT 1")

    cur.execute.assert_called_with("SELECT 1")
    cur.close.assert_called_once()
