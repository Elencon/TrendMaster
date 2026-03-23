import pytest
from unittest.mock import patch, MagicMock
from sqlalchemy.engine import Engine
from database.connection_manager import (
    ConnectionManager,
    get_connection_manager
)


# -----------------------------
# Fixtures
# -----------------------------

@pytest.fixture
def mock_config():
    """Mock config object for ConnectionManager."""
    config = MagicMock()
    config.db_user = "test_user"
    config.db_password = "test_password"
    config.db_host = "localhost"
    config.db_port = 3306
    config.db_name = "test_db"
    return config


@pytest.fixture(autouse=True)
def reset_connection_manager():
    """Reset singleton between tests to avoid cross-test contamination."""
    import database.connection_manager as cm_module
    cm_module._connection_manager_instance = None
    yield
    cm_module._connection_manager_instance = None


@pytest.fixture
def mock_engine():
    """A reusable mock engine for tests."""
    return MagicMock(spec=Engine)


# -----------------------------
# ConnectionManager initialization
# -----------------------------

def test_connection_manager_init(mock_config, mock_engine):
    """Test default parameters and create_engine call."""
    with patch("database.connection_manager.create_engine", return_value=mock_engine) as mock_create_engine:

        cm = ConnectionManager(mock_config)

        # Default pool parameters
        assert cm.pool_size == 10
        assert cm.max_overflow == 20
        assert cm.pool_recycle == 1800

        mock_create_engine.assert_called_once()
        args, kwargs = mock_create_engine.call_args
        url = str(args[0])

        # Check DB URL contains credentials and host
        assert "mysql+pymysql://" in url
        assert "test_user" in url
        assert "test_password" in url
        assert "localhost:3306" in url
        assert "test_db" in url

        # Pooling kwargs
        assert kwargs["pool_size"] == 10
        assert kwargs["max_overflow"] == 20
        assert kwargs["pool_timeout"] == 30
        assert kwargs["pool_recycle"] == 1800
        assert kwargs["pool_pre_ping"] is True


# -----------------------------
# get_connection
# -----------------------------

def test_get_connection_context_manager(mock_config, mock_engine):
    """Test that get_connection yields the correct connection."""
    mock_conn = MagicMock()
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_conn
    mock_engine.connect.return_value = mock_context

    with patch("database.connection_manager.create_engine", return_value=mock_engine):
        cm = ConnectionManager(mock_config)

        with cm.get_connection() as conn:
            assert conn is mock_conn

        mock_engine.connect.assert_called_once()


# -----------------------------
# get_session commit path
# -----------------------------

def test_get_session_commit(mock_config, mock_engine):
    """Test that session commits on success and closes."""
    mock_session = MagicMock()
    mock_session_class = MagicMock(return_value=mock_session)

    with patch("database.connection_manager.create_engine", return_value=mock_engine), \
         patch("database.connection_manager.sessionmaker", return_value=mock_session_class):

        cm = ConnectionManager(mock_config)

        with cm.get_session() as session:
            assert session is mock_session

        mock_session.commit.assert_called_once()
        mock_session.rollback.assert_not_called()
        mock_session.close.assert_called_once()


# -----------------------------
# get_session rollback path
# -----------------------------

def test_get_session_rollback_on_exception(mock_config, mock_engine):
    """Test that session rolls back and closes on exception."""
    mock_session = MagicMock()
    mock_session_class = MagicMock(return_value=mock_session)

    with patch("database.connection_manager.create_engine", return_value=mock_engine), \
         patch("database.connection_manager.sessionmaker", return_value=mock_session_class):

        cm = ConnectionManager(mock_config)

        with pytest.raises(ValueError):
            with cm.get_session():
                raise ValueError("test failure")

        mock_session.rollback.assert_called_once()
        mock_session.commit.assert_not_called()
        mock_session.close.assert_called_once()


# -----------------------------
# create_database_if_not_exists
# -----------------------------

def test_create_database_if_not_exists_success(mock_config, mock_engine):
    """Test that CREATE DATABASE is executed with AUTOCOMMIT."""
    mock_conn = MagicMock()
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_conn
    mock_engine.connect.return_value = mock_context

    with patch("database.connection_manager.create_engine", return_value=mock_engine) as mock_create_engine:

        cm = ConnectionManager(mock_config)
        mock_create_engine.reset_mock()  # Reset init call

        result = cm.create_database_if_not_exists()
        assert result is True

        mock_create_engine.assert_called_once()
        _, kwargs = mock_create_engine.call_args
        assert kwargs.get("isolation_level") == "AUTOCOMMIT"

        executed_sql = str(mock_conn.execute.call_args[0][0])
        assert "CREATE DATABASE IF NOT EXISTS" in executed_sql
        assert mock_config.db_name in executed_sql


def test_create_database_if_not_exists_failure(mock_config, mock_engine):
    """Test handling when database creation fails."""
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = Exception("DB failure")

    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_conn
    mock_engine.connect.return_value = mock_context

    with patch("database.connection_manager.create_engine", return_value=mock_engine):
        cm = ConnectionManager(mock_config)
        result = cm.create_database_if_not_exists()
        assert result is False


# -----------------------------
# Engine reuse
# -----------------------------

def test_engine_is_stored(mock_config, mock_engine):
    """Ensure engine instance is stored in ConnectionManager."""
    with patch("database.connection_manager.create_engine", return_value=mock_engine):
        cm = ConnectionManager(mock_config)
        assert cm.engine is mock_engine


# -----------------------------
# Sessionmaker configuration
# -----------------------------

def test_sessionmaker_configuration(mock_config, mock_engine):
    """Verify that sessionmaker is configured with correct parameters."""
    with patch("database.connection_manager.create_engine", return_value=mock_engine), \
         patch("database.connection_manager.sessionmaker") as mock_sessionmaker:

        ConnectionManager(mock_config)
        mock_sessionmaker.assert_called_once_with(bind=mock_engine, expire_on_commit=False)


# -----------------------------
# Singleton factory
# -----------------------------

def test_connection_manager_singleton(mock_config):
    """Test get_connection_manager returns the same instance."""
    with patch("database.connection_manager.create_engine"):
        cm1 = get_connection_manager(mock_config)
        cm2 = get_connection_manager()
        assert isinstance(cm1, ConnectionManager)
        assert cm1 is cm2