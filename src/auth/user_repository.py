r"""
C:\Economy\Invest\TrendMaster\src\auth\user_repository.py
User data access and CRUD operations.
"""

import logging
from contextlib import contextmanager
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
import pymysql.cursors

from .session import UserData

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

_USER_BASE_SELECT = """
    SELECT u.user_id, u.username, u.role, u.staff_id, u.active, u.last_login, u.created_at,
           s.name, s.last_name, s.email
    FROM users u
    LEFT JOIN staffs s ON u.staff_id = s.staff_id
"""

_GET_USER_BY_ID_QUERY = """
    SELECT u.user_id, u.username, u.role, u.staff_id, u.active, u.last_login, u.created_at,
           s.name, s.last_name, s.email, s.phone
    FROM users u
    LEFT JOIN staffs s ON u.staff_id = s.staff_id
    WHERE u.user_id = %s
"""

_GET_ALL_USERS_QUERY = _USER_BASE_SELECT + "ORDER BY u.user_id ASC"

_CREATE_USER_QUERY = """
    INSERT INTO users (username, password_hash, role, staff_id, created_at)
    VALUES (%s, %s, %s, %s, %s)
"""

_UPDATE_ROLE_QUERY   = "UPDATE users SET role = %s WHERE user_id = %s"
_UPDATE_ACTIVE_QUERY = "UPDATE users SET active = %s WHERE user_id = %s"
_DELETE_USER_QUERY   = "DELETE FROM users WHERE user_id = %s"

# ---------------------------------------------------------------------------
# Cursor helpers
# ---------------------------------------------------------------------------

def _get_dict_cursor(db_connection):
    return db_connection.cursor(pymysql.cursors.DictCursor)


@contextmanager
def _dict_cursor(db_connection):
    cur = _get_dict_cursor(db_connection)
    try:
        yield cur
    finally:
        cur.close()


@contextmanager
def _plain_cursor(db_connection):
    cur = db_connection.cursor()
    try:
        yield cur
    finally:
        cur.close()


def _is_duplicate_error(exc: Exception) -> bool:
    errno = getattr(exc, "errno", None)
    if errno == 1062:
        return True
    return "duplicate entry" in str(exc).lower()


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------

class UserRepository:
    """Handles user database operations (CRUD)."""

    def __init__(self, db_connection, password_handler):
        """
        Initialize UserRepository.

        Args:
            db_connection: Database connection object (mysql.connector or PyMySQL).
            password_handler: PasswordHandler instance for hashing.
        """
        self._db = db_connection
        self._password_handler = password_handler

    # ------------------------------------------------------------------
    # Create
    # ------------------------------------------------------------------

    def create_user(
        self,
        username: str,
        password: str,
        role: str,
        staff_id: Optional[int] = None,
    ) -> bool:
        try:
            password_hash = self._password_handler.hash_password(password)
            with _plain_cursor(self._db) as cur:
                cur.execute(_CREATE_USER_QUERY, (username, password_hash, role, staff_id, datetime.now(timezone.utc)))
            self._db.commit()
            _logger.info("Created user '%s' with role '%s'", username, role)
            return True
        except Exception as e:
            if _is_duplicate_error(e):
                _logger.error("Failed to create user '%s': duplicate entry or integrity constraint", username)
            else:
                _logger.error("Error creating user '%s': %s", username, e)
            return False

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get_user_by_id(self, user_id: int) -> Optional[UserData]:
        try:
            with _dict_cursor(self._db) as cur:
                cur.execute(_GET_USER_BY_ID_QUERY, (user_id,))
                return cur.fetchone()
        except Exception as e:
            _logger.error("Error getting user %s: %s", user_id, e)
            return None

    def get_all_users(self) -> List[UserData]:
        try:
            with _dict_cursor(self._db) as cur:
                cur.execute(_GET_ALL_USERS_QUERY)
                return cur.fetchall()
        except Exception as e:
            _logger.error("Error getting all users: %s", e)
            return []

    # ------------------------------------------------------------------
    # Update
    # ------------------------------------------------------------------

    def update_user_role(self, user_id: int, new_role: str) -> bool:
        return self._simple_update(
            _UPDATE_ROLE_QUERY,
            (new_role, user_id),
            on_success=lambda: _logger.info("Updated user %s role to '%s'", user_id, new_role),
            on_error=lambda e: _logger.error("Error updating user %s role: %s", user_id, e),
        )

    def set_user_active_status(self, user_id: int, active: bool) -> bool:
        status = "Activated" if active else "Deactivated"
        return self._simple_update(
            _UPDATE_ACTIVE_QUERY,
            (active, user_id),
            on_success=lambda: _logger.info("%s user %s", status, user_id),
            on_error=lambda e: _logger.error("Error changing active status for user %s: %s", user_id, e),
        )

    def activate_user(self, user_id: int) -> bool:
        """Activate a user account."""
        return self.set_user_active_status(user_id, True)

    def deactivate_user(self, user_id: int) -> bool:
        """Deactivate a user account."""
        return self.set_user_active_status(user_id, False)

    # ------------------------------------------------------------------
    # Delete
    # ------------------------------------------------------------------

    def delete_user(self, user_id: int) -> bool:
        try:
            with _plain_cursor(self._db) as cur:
                cur.execute(_DELETE_USER_QUERY, (user_id,))
                affected = cur.rowcount
            self._db.commit()

            if affected:
                _logger.warning("Permanently deleted user %s", user_id)
                return True

            _logger.warning("No user found with ID %s to delete", user_id)
            return False
        except Exception as e:
            _logger.error("Error deleting user %s: %s", user_id, e)
            return False

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _simple_update(self, query: str, params: tuple, on_success, on_error) -> bool:
        try:
            with _plain_cursor(self._db) as cur:
                cur.execute(query, params)
            self._db.commit()
            on_success()
            return True
        except Exception as e:
            on_error(e)
            return False
