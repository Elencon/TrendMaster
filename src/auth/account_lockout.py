"""
Account Lockout Manager
Handles account locking after failed login attempts.
"""

import logging
from datetime import datetime, timedelta
from typing import Optional, Dict
from dataclasses import dataclass

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_MAX_ATTEMPTS    = 5
_DEFAULT_LOCKOUT_MINUTES = 15

_PERSIST_QUERY = """
    UPDATE users
    SET failed_login_attempts = %s,
        locked_until          = %s,
        last_failed_attempt   = %s
    WHERE username = %s
"""

_LOAD_QUERY = """
    SELECT failed_login_attempts, locked_until, last_failed_attempt
    FROM users
    WHERE username = %s
"""

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class LockoutInfo:
    """Snapshot of an account's lockout state."""
    is_locked:           bool
    failed_attempts:     int
    locked_until:        Optional[datetime]
    last_failed_attempt: Optional[datetime]

    def time_remaining(self) -> int:
        """Seconds remaining in lockout (0 if not locked)."""
        if not self.is_locked or not self.locked_until:
            return 0
        return max(0, int((self.locked_until - datetime.now()).total_seconds()))

    def is_lockout_expired(self) -> bool:
        """True if a lockout was set but has now passed."""
        if not self.is_locked or not self.locked_until:
            return False
        return datetime.now() >= self.locked_until


def _cleared_lockout() -> LockoutInfo:
    """Return a zeroed-out LockoutInfo (not locked, no history)."""
    return LockoutInfo(
        is_locked=False,
        failed_attempts=0,
        locked_until=None,
        last_failed_attempt=None,
    )

# ---------------------------------------------------------------------------
# Manager
# ---------------------------------------------------------------------------

class AccountLockoutManager:
    """Manages account lockout after repeated failed login attempts."""

    def __init__(
        self,
        max_attempts: int = _DEFAULT_MAX_ATTEMPTS,
        lockout_duration_minutes: int = _DEFAULT_LOCKOUT_MINUTES,
        db_manager=None,
    ):
        """
        Args:
            max_attempts:             Failed attempts allowed before lockout.
            lockout_duration_minutes: How long the lockout lasts.
            db_manager:               Optional DB manager for persistent storage.
        """
        self._max_attempts    = max_attempts
        self._lockout_minutes = lockout_duration_minutes
        self._db              = db_manager
        self._cache: Dict[str, LockoutInfo] = {}

        _logger.info(
            "AccountLockoutManager initialised: %s attempts, %s min lockout",
            max_attempts, lockout_duration_minutes,
        )

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def record_failed_attempt(self, username: str) -> LockoutInfo:
        """
        Record a failed login attempt and lock the account if the threshold is reached.
        """
        info = self.get_lockout_info(username)

        # If already locked and not expired, do not increment further
        if info.is_locked and not info.is_lockout_expired():
            _logger.warning("Locked account '%s' attempted login", username)
            return info

        # Update failed attempts
        info.failed_attempts += 1
        info.last_failed_attempt = datetime.now()

        # Lock if threshold reached
        if info.failed_attempts >= self._max_attempts:
            info.is_locked = True
            info.locked_until = datetime.now() + timedelta(minutes=self._lockout_minutes)
            _logger.warning(
                "Account '%s' locked after %s failed attempts. Locked until %s",
                username, info.failed_attempts, info.locked_until,
            )
        else:
            _logger.info(
                "Failed login attempt for '%s': %s/%s",
                username, info.failed_attempts, self._max_attempts,
            )

        self._update(username, info)
        return info

    def record_successful_login(self, username: str) -> None:
        """Clear failed-attempt state after a successful login."""
        self._update(username, _cleared_lockout())
        _logger.info("Successful login for '%s', lockout info cleared", username)

    def get_lockout_info(self, username: str) -> LockoutInfo:
        """
        Return current lockout state, auto-expiring a stale lockout if needed.
        """
        # Check cache first
        if username in self._cache:
            info = self._cache[username]

            # Auto-expire lockout
            if info.is_locked and info.is_lockout_expired():
                _logger.info("Lockout expired for '%s', resetting", username)
                info.is_locked = False
                info.failed_attempts = 0
                info.locked_until = None
                self._persist(username, info)

            return info

        # Load from DB if available
        if self._db:
            info = self._load(username)
            if info:
                self._cache[username] = info
                return info

        # Default: no lockout
        return _cleared_lockout()

    def is_account_locked(self, username: str) -> bool:
        """Check whether an account is currently locked."""
        info = self.get_lockout_info(username)
        return info.is_locked and not info.is_lockout_expired()

    def unlock_account(self, username: str) -> None:
        """Manually unlock an account (admin action)."""
        self._update(username, _cleared_lockout())
        _logger.info("Account '%s' manually unlocked by administrator", username)

    def get_attempts_remaining(self, username: str) -> int:
        """Return how many failed attempts remain before lockout."""
        return max(0, self._max_attempts - self.get_lockout_info(username).failed_attempts)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _update(self, username: str, info: LockoutInfo) -> None:
        """Write info to the cache and persist to DB."""
        self._cache[username] = info
        self._persist(username, info)

    def _persist(self, username: str, info: LockoutInfo) -> None:
        """Write lockout state to the database (no-op if no db_manager)."""
        if not self._db:
            return
        try:
            with self._db.get_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    _PERSIST_QUERY,
                    (
                        info.failed_attempts,
                        info.locked_until,
                        info.last_failed_attempt,
                        username,
                    ),
                )
                conn.commit()
        except Exception as e:
            _logger.error("Failed to persist lockout info for '%s': %s", username, e)

    def _load(self, username: str) -> Optional[LockoutInfo]:
        """Load lockout state from the database (returns None on miss or error)."""
        if not self._db:
            return None
        try:
            with self._db.get_connection() as conn:
                cur = conn.cursor()
                cur.execute(_LOAD_QUERY, (username,))
                row = cur.fetchone()

            if row:
                failed_attempts, locked_until, last_failed_attempt = row
                return LockoutInfo(
                    is_locked=locked_until is not None and datetime.now() < locked_until,
                    failed_attempts=failed_attempts or 0,
                    locked_until=locked_until,
                    last_failed_attempt=last_failed_attempt,
                )
        except Exception as e:
            _logger.error("Failed to load lockout info for '%s': %s", username, e)

        return None
