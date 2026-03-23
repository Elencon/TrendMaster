import pytest
from sqlalchemy import create_engine
import pandas as pd
from unittest.mock import MagicMock

@pytest.fixture
def mock_config():
    """Returns a standard mock configuration dictionary."""
    return {
        "user": "test_user",
        "password": "test_password",
        "host": "localhost",
        "port": 3306,
        "database": "test_db"
    }

@pytest.fixture
def temp_db_engine():
    """Returns an in-memory SQLite SQLAlchemy engine for integration testing."""
    engine = create_engine("sqlite:///:memory:", future=True)
    yield engine
    engine.dispose()

@pytest.fixture
def mock_engine():
    """Returns a fully mocked SQLAlchemy engine."""
    mock_eng = MagicMock()
    # Basic mock setup
    mock_eng.url = "mock://url"
    return mock_eng

@pytest.fixture
def sample_df():
    """Returns a standard pandas DataFrame for testing."""
    return pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", None],
        "age": [25, 30, 45],
        "email": ["alice@example.com", "bob@bad-email", "charlie@example.com"]
    })
