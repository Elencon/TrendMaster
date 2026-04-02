r"""
C:\Economy\Invest\TrendMaster\src\auth\password_handler.py
Password hashing and verification utilities.

Uses bcrypt for hashing. Hashing and verification functions are
dependency-injectable to allow future migration (e.g., Argon2).
Install: pip install bcrypt
"""

import logging
from typing import Callable, Any, Optional
import bcrypt
import pymysql.cursors

from .password_policy import default_validator, PasswordPolicyValidator

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Hashing helpers (dependency-injectable)
# ---------------------------------------------------------------------------

def default_hash_password(password: str) -> str:
    """Hash password using bcrypt."""
    if not isinstance(password, str):
        raise TypeError("Password must be a string")

    hashed = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt())
    return hashed.decode("utf-8")


def default_verify_password(password: str, password_hash: str) -> bool:
    """Verify password against bcrypt hash."""
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
        password_validator: Optional[PasswordPolicyValidator] = None,
    ):
        self._db_connection = db_connection
        self._hash = hash_password
        self._verify = verify_password
        self._validator = password_validator or default_validator

    def change_password(self, user_id: int, old_password: str, new_password: str) -> bool:
        """
        Change user password after verifying the old one.
        Enforces password policy on the new password.
        """
        if not isinstance(user_id, int) or user_id <= 0:
            _logger.error("Invalid user_id: %s", user_id)
            return False

        if not old_password or not new_password:
            _logger.warning("Empty old or new password provided for user %s", user_id)
            return False

        # Enforce password policy
        is_valid, errors = self._validator.validate(new_password)
        if not is_valid:
            _logger.warning(
                "Password change rejected for user %s: policy violation - %s",
                user_id, "; ".join(errors)
            )
            return False

        # Prevent setting the same password
        if old_password == new_password:
            _logger.warning("Password change rejected: new password same as old for user %s", user_id)
            return False

        cursor = None
        try:
            # Use context manager for cursor (cleaner)
            with self._db_connection.cursor(pymysql.cursors.DictCursor) as cursor:
                # Verify old password
                cursor.execute(SQL_GET_PASSWORD, (user_id,))
                row = cursor.fetchone()

                if not row or not row.get("password_hash"):
                    _logger.warning("User %s not found or has no password", user_id)
                    return False

                if not self._verify(old_password, row["password_hash"]):
                    _logger.warning(
                        "Password change failed: invalid old password for user %s",
                        user_id,
                    )
                    return False

                # Hash and update new password
                new_hash = self._hash(new_password)
                cursor.execute(SQL_UPDATE_PASSWORD, (new_hash, user_id))
                self._db_connection.commit()

                _logger.info("Password successfully changed for user %s", user_id)
                return True

        except Exception as e:
            _logger.error("Error changing password for user %s: %s", user_id, e)
            try:
                self._db_connection.rollback()
            except Exception as rb_err:
                _logger.error("Rollback failed for user %s: %s", user_id, rb_err)
            return False

    def verify_password(self, password: str, password_hash: str) -> bool:
        """Verify password against hash."""
        return self._verify(password, password_hash)
