r"""
C:\Economy\Invest\TrendMaster\src\auth\user_authenticator.py
User authentication and verification.
"""

import logging
from contextlib import contextmanager
from typing import Any, Dict, Optional
from datetime import datetime, timezone

from .password_handler import PasswordHandler   # for type clarity (optional)

_logger = logging.getLogger(__name__)

_SENSITIVE_FIELDS = frozenset({"password_hash"})

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


# ---------------------------------------------------------------------------
# Cursor helpers
# ---------------------------------------------------------------------------

@contextmanager
def _dict_cursor(db_connection):
    """Context manager for a dictionary cursor (supports mysql.connector and PyMySQL)."""
    cursor = None
    try:
        # mysql.connector style
        cursor = db_connection.cursor(dictionary=True)
    except (TypeError, AttributeError):
        # PyMySQL style
        import pymysql.cursors
        cursor = db_connection.cursor(pymysql.cursors.DictCursor)

    try:
        yield cursor
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass


@contextmanager
def _plain_cursor(db_connection):
    """Context manager for a plain (non-dict) cursor."""
    cursor = db_connection.cursor()
    try:
        yield cursor
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _scrub(user: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of user data with sensitive fields removed."""
    return {k: v for k, v in user.items() if k not in _SENSITIVE_FIELDS}


# ---------------------------------------------------------------------------
# Authenticator
# ---------------------------------------------------------------------------

class UserAuthenticator:
    """Handles user authentication and login tracking."""

    def __init__(self, db_connection: Any, password_handler: PasswordHandler):
        """
        Args:
            db_connection:    Database connection (mysql.connector or PyMySQL).
            password_handler: PasswordHandler instance for password verification.
        """
        self._db = db_connection
        self._password_handler = password_handler

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def authenticate(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        """
        Authenticate a user with username and password.

        Returns:
            Dict with user info (sensitive fields stripped) if successful,
            None otherwise.
        """
        if not username or not password:
            _logger.warning("Authentication rejected: empty username or password")
            return None

        try:
            user = self._fetch_user(username)
        except Exception as e:
            _logger.error("Database error while fetching user '%s': %s", username, e)
            return None

        if not user:
            _logger.warning("Authentication failed: user '%s' not found", username)
            return None

        if not user.get("active"):
            _logger.warning("Authentication failed: user '%s' is inactive", username)
            return None

        if not self._password_handler.verify_password(password, user.get("password_hash")):
            _logger.warning("Authentication failed: wrong password for '%s'", username)
            return None

        # Update last login (best effort - do not fail auth on error)
        self._update_last_login(user["user_id"])

        _logger.info("User '%s' authenticated successfully", username)
        return _scrub(user)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_user(self, username: str) -> Optional[Dict[str, Any]]:
        """Query the DB and return the raw user row, or None if not found."""
        with _dict_cursor(self._db) as cur:
            cur.execute(_AUTH_QUERY, (username,))
            row = cur.fetchone()
            return dict(row) if row else None   # ensure dict

    def _update_last_login(self, user_id: int) -> None:
        """Update last_login timestamp. Failures are logged but do not raise."""
        try:
            with _plain_cursor(self._db) as cur:
                cur.execute(_UPDATE_LAST_LOGIN_QUERY, (datetime.now(timezone.utc), user_id))
            self._db.commit()
        except Exception as e:
            _logger.error("Failed to update last_login for user_id %s: %s", user_id, e)