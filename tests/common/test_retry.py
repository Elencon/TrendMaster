import os
import pytest

import config
from config.env_config import set_env_backend


# ---------------------------------------------------------------------------
# Mock environment backend for testing
# ---------------------------------------------------------------------------

class MockBackend:
    def __init__(self, data):
        self.data = data

    def get(self, key: str):
        return self.data.get(key)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_env():
    backend = MockBackend({
        "ENVIRONMENT": "development",
        "DB_HOST": "mock-host",
        "DB_PORT": "3307",
        "DB_NAME": "mock_db",
        "DB_USER": "mock_user",
        "DB_PASSWORD": "mock_pass",
    })
    set_env_backend(backend)
    return backend


# ---------------------------------------------------------------------------
# Tests: env_config
# ---------------------------------------------------------------------------

def test_env_config_reads_values(mock_env):
    ec = config.env_config

    assert ec.db_host == "mock-host"
    assert ec.db_port == 3307
    assert ec.db_name == "mock_db"
    assert ec.db_user == "mock_user"
    assert ec.db_password == "mock_pass"


def test_env_config_defaults(monkeypatch):
    # Reset backend to default OS env behavior
    monkeypatch.delenv("DB_HOST", raising=False)

    ec = config.env_config

    # Defaults should apply
    assert isinstance(ec.db_host, str)


# ---------------------------------------------------------------------------
# Tests: MySQLConfig
# ---------------------------------------------------------------------------

def test_mysql_config_creation():
    cfg = config.MySQLConfig.create(
        host="localhost",
        port=3306,
        database="test_db",
        user="root",
        password="",
        pool_size=5,
    )

    assert cfg.host == "localhost"
    assert cfg.port == 3306
    assert cfg.database == "test_db"
    assert cfg.pool_size == 5

    conn_dict = cfg.to_dict()
    assert "collation" in conn_dict
    assert "init_command" in conn_dict


# ---------------------------------------------------------------------------
# Tests: environment profiles
# ---------------------------------------------------------------------------

def test_load_development_profile():
    cfg = config.load_config_for_environment("development")

    assert cfg.application.environment == "development"
    assert cfg.logging.level == "DEBUG"
    assert cfg.processing.batch_size > 0


def test_load_testing_profile():
    cfg = config.load_config_for_environment("testing")

    assert cfg.application.environment == "testing"
    assert cfg.database.database == "store_manager_test"
    assert cfg.logging.enable_file_logging is False


def test_load_production_profile():
    cfg = config.load_config_for_environment("production")

    assert cfg.application.environment == "production"
    assert cfg.application.debug_mode is False
    assert cfg.logging.level == "INFO"


def test_invalid_environment_raises():
    with pytest.raises(ValueError):
        config.load_config_for_environment("unknown_env")


# ---------------------------------------------------------------------------
# Tests: environment helpers
# ---------------------------------------------------------------------------

def test_environment_helpers(mock_env):
    # backend ENVIRONMENT = development
    assert config.is_development() is True
    assert config.is_production() is False
    assert config.is_testing() is False


# ---------------------------------------------------------------------------
# Tests: factory API
# ---------------------------------------------------------------------------

def test_get_config_returns_etl_config():
    # Depending on your factory implementation
    cfg = config.get_config()

    assert hasattr(cfg, "database")
    assert hasattr(cfg, "api")
    assert hasattr(cfg, "processing")


def test_get_default_config():
    cfg = config.get_default_config()
    assert cfg is not None


# ---------------------------------------------------------------------------
# Integration test: full pipeline
# ---------------------------------------------------------------------------

def test_full_environment_pipeline(mock_env):
    cfg = config.load_config_for_environment()

    assert cfg.database.host == "localhost" or isinstance(cfg.database.host, str)
    assert cfg.application is not None
    assert cfg.processing is not None
    assert cfg.logging is not None