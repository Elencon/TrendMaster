r"""
C:\Economy\Invest\TrendMaster\src\auth\password_handler.py
Password hashing and verification utilities.

Uses bcrypt for hashing. Hashing and verification functions are
dependency-injectable to allow future migration (e.g., Argon2).
Install: pip install bcrypt
"""

import logging
from typing import Callable, Any
import bcrypt
import pymysql.cursors  # single supported backend

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cursor helper (PyMySQL only)
# ---------------------------------------------------------------------------

def _dict_cursor(connection):
    """Return a dictionary cursor using PyMySQL."""
    return connection.cursor(pymysql.cursors.DictCursor)


# ---------------------------------------------------------------------------
# Hashing helpers (dependency-injectable)
# ---------------------------------------------------------------------------

def default_hash_password(password: str) -> str:
    if not isinstance(password, str):
        raise TypeError("Password must be a string")

    hashed = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt())
    return hashed.decode("utf-8")


def default_verify_password(password: str, password_hash: str) -> bool:
    try:
        return bcrypt.checkpw(
            password.encode("utf-8"),
            password_hash.encode("utf-8"),
        )
    except Exception as e:
        _logger.error("Password verification error: %s", e)
        return False


# ---------------------------------------------------------------------------
# SQL Queries
# ---------------------------------------------------------------------------

SQL_GET_PASSWORD = "SELECT password_hash FROM users WHERE user_id = %s"
SQL_UPDATE_PASSWORD = "UPDATE users SET password_hash = %s WHERE user_id = %s"


# ---------------------------------------------------------------------------
# Password handler
# ---------------------------------------------------------------------------

class PasswordHandler:
    """
    Handles password hashing, verification, and updates.
    """

    def __init__(
        self,
        db_connection: Any,
        hash_password: Callable[[str], str] = default_hash_password,
        verify_password: Callable[[str, str], bool] = default_verify_password,
    ):
        self._db_connection = db_connection
        self._hash = hash_password
        self._verify = verify_password

    def change_password(self, user_id: int, old_password: str, new_password: str) -> bool:
        if not new_password:
            _logger.error("Rejecting empty new password for user %s", user_id)
            return False

        cursor = None
        try:
            cursor = _dict_cursor(self._db_connection)

            cursor.execute(SQL_GET_PASSWORD, (user_id,))
            row = cursor.fetchone()

            if not row:
                _logger.warning("User %s not found", user_id)
                return False

            if not self._verify(old_password, row["password_hash"]):
                _logger.warning(
                    "Password change failed: invalid old password for user %s",
                    user_id,
                )
                return False

            new_hash = self._hash(new_password)

            cursor.execute(SQL_UPDATE_PASSWORD, (new_hash, user_id))
            self._db_connection.commit()

            _logger.info("Password changed for user %s", user_id)
            return True

        except Exception as e:
            _logger.error("Error changing password for user %s: %s", user_id, e)
            try:
                self._db_connection.rollback()
            except Exception as rb_err:
                _logger.error("Rollback failed for user %s: %s", user_id, rb_err)
            return False

        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
