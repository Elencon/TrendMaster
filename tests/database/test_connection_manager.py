import pytest
from unittest.mock import patch, MagicMock
from src.database.connection_manager import DatabaseConnection, create_connection_manager


# -----------------------------
# Fixtures
# -----------------------------

@pytest.fixture
def mock_config():
    """Mock config dictionary for DatabaseConnection."""
    return {
        "user": "test_user",
        "password": "test_password",
        "host": "localhost",
        "port": 3306,
        "database": "test_db",
    }


# -----------------------------
# Direct connection tests
# -----------------------------

def test_direct_connection(mock_config):
    """Test get_connection without pooling."""
    db = DatabaseConnection(mock_config, enable_pooling=False)

    with patch("pymysql.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        with db.get_connection() as conn:
            assert conn is mock_conn

        mock_connect.assert_called_once()


# -----------------------------
# Connection pooling tests
# -----------------------------

def test_pool_connection_acquire_release(mock_config):
    """Test acquiring and releasing from a pool."""
    with patch("pymysql.connect") as mock_connect:
        mock_conn1 = MagicMock()
        mock_conn2 = MagicMock()
        mock_connect.side_effect = [mock_conn1, mock_conn2]

        db = DatabaseConnection(mock_config, enable_pooling=True, pool_size=2)
        pool = db._pool

        conn = pool._acquire()
        assert conn in [mock_conn1, mock_conn2]

        pool._release(conn)
        assert conn in pool._available

def test_pool_replaces_stale_connection(mock_config):
    """Test pool replaces dead connection with new one."""
    with patch("pymysql.connect") as mock_connect:
        alive_conn = MagicMock()
        stale_conn = MagicMock()
        stale_conn.ping.side_effect = Exception("stale")
        new_conn = MagicMock()

        mock_connect.side_effect = [stale_conn, new_conn]

        db = DatabaseConnection(mock_config, enable_pooling=True, pool_size=1)
        pool = db._pool

        conn = pool._acquire()
        assert conn is new_conn


# -----------------------------
# get_schema tests
# -----------------------------

def test_get_schema_returns_columns(mock_config):
    """get_schema returns list of column names."""
    db = DatabaseConnection(mock_config, enable_pooling=False)

    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [{"COLUMN_NAME": "id"}, {"COLUMN_NAME": "name"}]

    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    with patch("pymysql.connect", return_value=mock_conn):
        cols = db.get_schema("users")
        assert cols == ["id", "name"]


def test_get_schema_handles_invalid_name(mock_config):
    """get_schema returns empty list for invalid table name."""
    db = DatabaseConnection(mock_config, enable_pooling=False)
    cols = db.get_schema("123invalid")
    assert cols == []


# -----------------------------
# create_database_if_not_exists tests
# -----------------------------

def test_create_database_if_not_exists_calls_execute(mock_config):
    """CREATE DATABASE is executed and committed."""
    db = DatabaseConnection(mock_config, enable_pooling=False)

    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    with patch("pymysql.connect", return_value=mock_conn):
        result = db.create_database_if_not_exists()
        assert result is True
        mock_cursor.execute.assert_any_call("CREATE DATABASE IF NOT EXISTS `test_db`")
        mock_conn.commit.assert_called_once()


def test_create_database_if_not_exists_failure(mock_config):
    """Handles exception during database creation."""
    db = DatabaseConnection(mock_config, enable_pooling=False)

    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = Exception("DB failure")
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    with patch("pymysql.connect", return_value=mock_conn):
        result = db.create_database_if_not_exists()
        assert result is False


# -----------------------------
# Diagnostics
# -----------------------------

def test_test_connection_returns_true(mock_config):
    db = DatabaseConnection(mock_config, enable_pooling=False)

    with patch("pymysql.connect", return_value=MagicMock()):
        assert db.test_connection() is True


def test_get_connection_stats_counts_attempts(mock_config):
    db = DatabaseConnection(mock_config, enable_pooling=False)

    with patch("pymysql.connect", return_value=MagicMock()):
        with db.get_connection():
            pass

    stats = db.get_connection_stats()
    assert stats["attempts"] == 1
    assert stats["pooling_enabled"] is False


def test_get_config_summary_masks_password(mock_config):
    db = DatabaseConnection(mock_config, enable_pooling=False)
    summary = db.get_config_summary()
    assert summary["password"] == "*" * len(mock_config["password"])


# -----------------------------
# Pool close
# -----------------------------

def test_close_pool_closes_connections(mock_config):
    with patch("pymysql.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        db = DatabaseConnection(mock_config, enable_pooling=True)
        db.close_pool()

        assert db._pool is None
        mock_conn.close.assert_called()


# -----------------------------
# Factory
# -----------------------------

def test_create_connection_manager_returns_instance(mock_config):
    db = create_connection_manager(mock_config)
    assert isinstance(db, DatabaseConnection)