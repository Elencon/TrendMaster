"""
Password hashing and verification operations.

Uses bcrypt for hashing. Argon2 migration can be applied in future
by swapping hash_password / verify_password without changing callers.

Install: pip install bcrypt
"""

import logging
import bcrypt

_logger = logging.getLogger(__name__)


def _dict_cursor(connection):
    """Return a dictionary cursor compatible with mysql.connector and PyMySQL."""
    try:
        return connection.cursor(dictionary=True)
    except TypeError:
        import pymysql.cursors
        return connection.cursor(pymysql.cursors.DictCursor)


# ---------------------------------------------------------------------------
# Hashing helpers (dependency-injectable)
# ---------------------------------------------------------------------------

def default_hash_password(password: str) -> str:
    """Hash a password using bcrypt."""
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def default_verify_password(password: str, password_hash: str) -> bool:
    """Verify a password against its bcrypt hash."""
    try:
        return bcrypt.checkpw(
            password.encode("utf-8"),
            password_hash.encode("utf-8"),
        )
    except Exception as e:
        _logger.error("Error verifying password: %s", e)
        return False


# ---------------------------------------------------------------------------
# Password handler
# ---------------------------------------------------------------------------

class PasswordHandler:
    """
    Handles password hashing, verification, and changes.

    Hashing and verification functions are dependency-injectable to allow
    future migration to Argon2 without modifying callers.
    """

    def __init__(
        self,
        db_connection,
        hash_password=default_hash_password,
        verify_password=default_verify_password,
    ):
        self._db_connection = db_connection
        self._hash = hash_password
        self._verify = verify_password

    # ------------------------------------------------------------------
    # Database operations
    # ------------------------------------------------------------------

    def change_password(
        self, user_id: int, old_password: str, new_password: str
    ) -> bool:
        """
        Change a user's password after verifying the current one.

        Returns:
            True if password changed successfully, False otherwise.
        """
        if not new_password:
            _logger.error("Rejecting empty new password for user %s", user_id)
            return False

        cursor = None
        try:
            cursor = _dict_cursor(self._db_connection)
            cursor.execute(
                "SELECT password_hash FROM users WHERE user_id = %s",
                (user_id,),
            )
            user = cursor.fetchone()

            if not user:
                _logger.error("User %s not found", user_id)
                return False

            if not self._verify(old_password, user["password_hash"]):
                _logger.warning("Password change failed: invalid old password for user %s", user_id)
                return False

            new_hash = self._hash(new_password)
            cursor.execute(
                "UPDATE users SET password_hash = %s WHERE user_id = %s",
                (new_hash, user_id),
            )
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
                cursor.close()