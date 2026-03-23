"""
Two-Factor Authentication (2FA) handler using TOTP (Time-based One-Time Password).
Supports Google Authenticator, Authy, and other TOTP-compatible apps.
"""

import json
import secrets
from contextlib import contextmanager
from io import BytesIO
from typing import List, Optional

import pyotp
import qrcode
import logging

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_APP_NAME          = "Store Manager"
_BACKUP_CODE_COUNT = 8
_TOTP_VALID_WINDOW = 1  # allow ±30 seconds drift

_ENABLE_2FA_QUERY = """
    UPDATE users
    SET two_factor_enabled = TRUE,
        two_factor_secret  = %s,
        backup_codes       = %s
    WHERE user_id = %s
"""

_DISABLE_2FA_QUERY = """
    UPDATE users
    SET two_factor_enabled = FALSE,
        two_factor_secret  = NULL,
        backup_codes       = NULL
    WHERE user_id = %s
"""

_SELECT_2FA_ENABLED_QUERY  = "SELECT two_factor_enabled FROM users WHERE user_id = %s"
_SELECT_SECRET_QUERY       = "SELECT two_factor_secret  FROM users WHERE user_id = %s"
_SELECT_BACKUP_CODES_QUERY = "SELECT backup_codes       FROM users WHERE user_id = %s"
_UPDATE_BACKUP_CODES_QUERY = "UPDATE users SET backup_codes = %s WHERE user_id = %s"

# ---------------------------------------------------------------------------
# Cursor helper
# ---------------------------------------------------------------------------

@contextmanager
def _plain_cursor(db_connection):
    cur = db_connection.cursor()
    try:
        yield cur
    finally:
        cur.close()

# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------

class TwoFactorAuth:
    """Handles TOTP-based two-factor authentication."""

    def __init__(self, db_connection):
        self._db = db_connection

    # ------------------------------------------------------------------
    # TOTP helpers (no DB access)
    # ------------------------------------------------------------------

    @staticmethod
    def generate_secret() -> str:
        """Generate a new TOTP secret key."""
        return pyotp.random_base32()

    def generate_qr_code(self, username: str, secret: str) -> bytes:
        """
        Generate a QR code PNG for TOTP setup.
        """
        uri = pyotp.TOTP(secret).provisioning_uri(
            name=username,
            issuer_name=_APP_NAME,
        )

        qr = qrcode.QRCode(version=1, box_size=10, border=5)
        qr.add_data(uri)
        qr.make(fit=True)

        buf = BytesIO()
        qr.make_image(fill_color="black", back_color="white").save(buf, format="PNG")
        return buf.getvalue()

    @staticmethod
    def verify_code(secret: str, code: str) -> bool:
        """
        Verify a TOTP code.
        """
        try:
            code = code.strip()
            return pyotp.TOTP(secret).verify(code, valid_window=_TOTP_VALID_WINDOW)
        except Exception as e:
            _logger.error("Error verifying TOTP code: %s", e)
            return False

    @staticmethod
    def generate_backup_codes(count: int = _BACKUP_CODE_COUNT) -> List[str]:
        """
        Generate one-time backup codes for account recovery.
        """
        return [secrets.token_hex(4).upper() for _ in range(count)]

    # ------------------------------------------------------------------
    # DB operations
    # ------------------------------------------------------------------

    def enable_2fa(self, user_id: int, secret: str, backup_codes: List[str]) -> bool:
        """Enable 2FA for a user."""
        try:
            with _plain_cursor(self._db) as cur:
                cur.execute(_ENABLE_2FA_QUERY, (secret, json.dumps(backup_codes), user_id))
            self._db.commit()
            _logger.info("2FA enabled for user_id: %s", user_id)
            return True
        except Exception as e:
            _logger.error("Error enabling 2FA for user_id %s: %s", user_id, e)
            return False

    def disable_2fa(self, user_id: int) -> bool:
        """Disable 2FA for a user."""
        try:
            with _plain_cursor(self._db) as cur:
                cur.execute(_DISABLE_2FA_QUERY, (user_id,))
            self._db.commit()
            _logger.info("2FA disabled for user_id: %s", user_id)
            return True
        except Exception as e:
            _logger.error("Error disabling 2FA for user_id %s: %s", user_id, e)
            return False

    def is_2fa_enabled(self, user_id: int) -> bool:
        """Check if 2FA is enabled for a user."""
        try:
            with _plain_cursor(self._db) as cur:
                cur.execute(_SELECT_2FA_ENABLED_QUERY, (user_id,))
                result = cur.fetchone()
            return bool(result[0]) if result else False
        except Exception as e:
            _logger.error("Error checking 2FA status for user_id %s: %s", user_id, e)
            return False

    def get_user_secret(self, user_id: int) -> Optional[str]:
        """Get user's TOTP secret."""
        try:
            with _plain_cursor(self._db) as cur:
                cur.execute(_SELECT_SECRET_QUERY, (user_id,))
                result = cur.fetchone()
            return result[0] if result else None
        except Exception as e:
            _logger.error("Error getting TOTP secret for user_id %s: %s", user_id, e)
            return None

    def verify_backup_code(self, user_id: int, code: str) -> bool:
        """
        Verify and consume a backup code (single-use).
        """
        try:
            with _plain_cursor(self._db) as cur:
                cur.execute(_SELECT_BACKUP_CODES_QUERY, (user_id,))
                result = cur.fetchone()

                if not result or not result[0]:
                    return False

                codes = json.loads(result[0])
                normalized = code.strip().upper()

                if normalized not in codes:
                    return False

                codes.remove(normalized)
                cur.execute(_UPDATE_BACKUP_CODES_QUERY, (json.dumps(codes), user_id))

            self._db.commit()
            _logger.info("Backup code consumed for user_id: %s", user_id)
            return True

        except Exception as e:
            _logger.error("Error verifying backup code for user_id %s: %s", user_id, e)
            return False

    def get_remaining_backup_codes(self, user_id: int) -> List[str]:
        """Get remaining (unused) backup codes for a user."""
        try:
            with _plain_cursor(self._db) as cur:
                cur.execute(_SELECT_BACKUP_CODES_QUERY, (user_id,))
                result = cur.fetchone()
            return json.loads(result[0]) if result and result[0] else []
        except Exception as e:
            _logger.error("Error getting backup codes for user_id %s: %s", user_id, e)
            return []