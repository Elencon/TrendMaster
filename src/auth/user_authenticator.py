"""
User authentication and verification.
"""
import logging
from contextlib import contextmanager
from typing import Optional, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

_SENSITIVE_FIELDS = {"password_hash"}

_AUTH_QUERY = """
    SELECT
        u.user_id,
        u.username,
        u.password_hash,
        u.role,
        u.staff_id,
        u.active,
        s.name,
        s.last_name,
        s.email
    FROM users u
    LEFT JOIN staffs s ON u.staff_id = s.staff_id
    WHERE u.username = %s
"""

_UPDATE_LAST_LOGIN_QUERY = "UPDATE users SET last_login = %s WHERE user_id = %s"


def _get_dict_cursor(db_connection):
    """Return a dictionary cursor, compatible with mysql.connector and PyMySQL."""
    try:
        return db_connection.cursor(dictionary=True)
    except TypeError:
        import pymysql.cursors
        return db_connection.cursor(pymysql.cursors.DictCursor)


@contextmanager
def _dict_cursor(db_connection):
    """Context manager for a dictionary cursor."""
    cur = _get_dict_cursor(db_connection)
    try:
        yield cur
    finally:
        cur.close()


@contextmanager
def _plain_cursor(db_connection):
    """Context manager for a plain cursor."""
    cur = db_connection.cursor()
    try:
        yield cur
    finally:
        cur.close()


def _scrub(user: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of user with all sensitive fields removed."""
    return {k: v for k, v in user.items() if k not in _SENSITIVE_FIELDS}


class UserAuthenticator:
    """Handles user authentication and login tracking."""

    def __init__(self, db_connection, password_handler):
        """
        Initialize UserAuthenticator.

        Args:
            db_connection: Database connection object (mysql.connector or PyMySQL).
            password_handler: PasswordHandler instance for password verification.
        """
        self.db_connection = db_connection
        self.password_handler = password_handler

    def authenticate(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        """
        Authenticate a user with username and password.

        Args:
            username: Username.
            password: Plain-text password.

        Returns:
            Dict with user info (sensitive fields stripped) if authenticated,
            None otherwise.
        """
        try:
            user = self._fetch_user(username)
        except Exception as e:
            logger.error("DB error while fetching user '%s': %s", username, e)
            return None

        if not user or not user.get("active"):
            logger.warning("Authentication failed for user '%s'", username)
            return None

        if not self.password_handler.verify_password(password, user["password_hash"]):
            logger.warning("Authentication failed for user '%s'", username)
            return None

        self._update_last_login(user["user_id"])
        logger.info("User '%s' authenticated successfully", username)
        return _scrub(user)

    def _fetch_user(self, username: str) -> Optional[Dict[str, Any]]:
        """Query the DB and return the raw user row, or None if not found."""
        with _dict_cursor(self.db_connection) as cur:
            cur.execute(_AUTH_QUERY, (username,))
            return cur.fetchone()

    def _update_last_login(self, user_id: int) -> None:
        """Stamp last_login for user_id; logs but never raises on failure."""
        try:
            with _plain_cursor(self.db_connection) as cur:
                cur.execute(_UPDATE_LAST_LOGIN_QUERY, (datetime.now(), user_id))
            self.db_connection.commit()
        except Exception as e:
            logger.error("Could not update last_login for user_id %s: %s", user_id, e)