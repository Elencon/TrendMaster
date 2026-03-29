import pytest
from unittest.mock import MagicMock

from auth.user_repository import UserRepository


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_db():
    """Mock database connection with cursor() and commit()."""
    db = MagicMock()
    cursor = MagicMock()
    db.cursor.return_value = cursor
    return db


@pytest.fixture
def mock_password_handler():
    handler = MagicMock()
    handler.hash_password.return_value = "hashed_pw"
    return handler


@pytest.fixture
def repo(mock_db, mock_password_handler):
    return UserRepository(mock_db, mock_password_handler)


# ---------------------------------------------------------------------------
# Create user
# ---------------------------------------------------------------------------

def test_create_user_success(repo, mock_db, mock_password_handler):
    result = repo.create_user("john", "secret", "admin", None)

    assert result is True
    mock_password_handler.hash_password.assert_called_once_with("secret")
    mock_db.cursor.return_value.execute.assert_called_once()
    mock_db.commit.assert_called_once()


def test_create_user_duplicate(repo, mock_db):
    # Simulate duplicate entry error
    exc = Exception("Duplicate entry")
    mock_db.cursor.return_value.execute.side_effect = exc

    result = repo.create_user("john", "pw", "admin")

    assert result is False


# ---------------------------------------------------------------------------
# Read operations
# ---------------------------------------------------------------------------

def test_get_user_by_id_success(repo, mock_db):
    mock_cursor = mock_db.cursor.return_value
    mock_cursor.fetchone.return_value = {"user_id": 1, "username": "john"}

    result = repo.get_user_by_id(1)

    assert result == {"user_id": 1, "username": "john"}
    mock_cursor.execute.assert_called_once()


def test_get_user_by_id_error(repo, mock_db):
    mock_db.cursor.return_value.execute.side_effect = Exception("DB error")

    result = repo.get_user_by_id(1)

    assert result is None


def test_get_all_users_success(repo, mock_db):
    mock_cursor = mock_db.cursor.return_value
    mock_cursor.fetchall.return_value = [
        {"user_id": 1, "username": "john"},
        {"user_id": 2, "username": "anna"},
    ]

    result = repo.get_all_users()

    assert len(result) == 2
    mock_cursor.execute.assert_called_once()


def test_get_all_users_error(repo, mock_db):
    mock_db.cursor.return_value.execute.side_effect = Exception("DB error")

    result = repo.get_all_users()

    assert result == []


# ---------------------------------------------------------------------------
# Update operations
# ---------------------------------------------------------------------------

def test_update_user_role_success(repo, mock_db):
    result = repo.update_user_role(1, "manager")

    assert result is True
    mock_db.cursor.return_value.execute.assert_called_once()
    mock_db.commit.assert_called_once()


def test_update_user_role_error(repo, mock_db):
    mock_db.cursor.return_value.execute.side_effect = Exception("DB error")

    result = repo.update_user_role(1, "manager")

    assert result is False


def test_set_user_active_status_success(repo, mock_db):
    result = repo.set_user_active_status(1, True)

    assert result is True
    mock_db.cursor.return_value.execute.assert_called_once()
    mock_db.commit.assert_called_once()


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------

def test_delete_user_success(repo, mock_db):
    mock_cursor = mock_db.cursor.return_value
    mock_cursor.rowcount = 1

    result = repo.delete_user(5)

    assert result is True
    mock_cursor.execute.assert_called_once()
    mock_db.commit.assert_called_once()


def test_delete_user_not_found(repo, mock_db):
    mock_cursor = mock_db.cursor.return_value
    mock_cursor.rowcount = 0

    result = repo.delete_user(5)

    assert result is False


def test_delete_user_error(repo, mock_db):
    mock_db.cursor.return_value.execute.side_effect = Exception("DB error")

    result = repo.delete_user(5)

    assert result is False
