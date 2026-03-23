import pytest
from unittest.mock import MagicMock, patch

from src.auth.password_handler import (
    PasswordHandler,
    default_hash_password,
    default_verify_password,
    SQL_GET_PASSWORD,
    SQL_UPDATE_PASSWORD,
)


# ---------------------------------------------------------------------------
# Hashing tests
# ---------------------------------------------------------------------------

def test_default_hash_password_and_verify():
    password = "secret123"
    hashed = default_hash_password(password)

    assert hashed != password
    assert default_verify_password(password, hashed) is True
    assert default_verify_password("wrong", hashed) is False


# ---------------------------------------------------------------------------
# PasswordHandler tests
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_db():
    """Return a mock DB connection with a mock cursor."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn


def test_change_password_success(mock_db):
    handler = PasswordHandler(mock_db)

    # Mock DB row
    mock_db.cursor.return_value.fetchone.return_value = {
        "password_hash": default_hash_password("oldpass")
    }

    result = handler.change_password(
        user_id=1,
        old_password="oldpass",
        new_password="newpass"
    )

    assert result is True

    cursor = mock_db.cursor.return_value

    # Check SELECT executed
    cursor.execute.assert_any_call(SQL_GET_PASSWORD, (1,))

    # Check UPDATE executed
    assert cursor.execute.call_args_list[-1][0][0] == SQL_UPDATE_PASSWORD

    # Check commit called
    mock_db.commit.assert_called_once()


def test_change_password_user_not_found(mock_db):
    handler = PasswordHandler(mock_db)

    # No user returned
    mock_db.cursor.return_value.fetchone.return_value = None

    result = handler.change_password(1, "oldpass", "newpass")

    assert result is False
    mock_db.commit.assert_not_called()


def test_change_password_wrong_old_password(mock_db):
    handler = PasswordHandler(mock_db)

    # Return a hash that does NOT match
    mock_db.cursor.return_value.fetchone.return_value = {
        "password_hash": default_hash_password("something_else")
    }

    result = handler.change_password(1, "oldpass", "newpass")

    assert result is False
    mock_db.commit.assert_not_called()


def test_change_password_db_error_triggers_rollback(mock_db):
    handler = PasswordHandler(mock_db)

    # Simulate DB error on SELECT
    mock_db.cursor.return_value.execute.side_effect = Exception("DB error")

    result = handler.change_password(1, "oldpass", "newpass")

    assert result is False
    mock_db.rollback.assert_called_once()


def test_cursor_is_closed(mock_db):
    handler = PasswordHandler(mock_db)

    # Return valid user
    mock_db.cursor.return_value.fetchone.return_value = {
        "password_hash": default_hash_password("oldpass")
    }

    handler.change_password(1, "oldpass", "newpass")

    cursor = mock_db.cursor.return_value
    cursor.close.assert_called_once()