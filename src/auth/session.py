r"""
C:\Economy\Invest\TrendMaster\src\auth\session.py
Thread-safe singleton session manager for tracking authenticated users.
"""

import logging
from typing import Optional, TypedDict
from datetime import datetime, timezone
from threading import Lock

_logger = logging.getLogger(__name__)


class UserData(TypedDict, total=False):
    user_id:  int
    username: str
    role:     str
    staff_id: Optional[int]


class SessionManager:
    """Thread-safe singleton session manager for tracking the current logged-in user."""

    _instance = None
    _instance_lock = Lock()  # Ensures singleton creation is thread-safe

    def __new__(cls):
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        # Guard ensures instance attributes are only set once,
        # even though __init__ is called every time SessionManager() is used.
        if not hasattr(self, "_initialized"):
            self._current_user: Optional[UserData] = None
            self._login_time: Optional[datetime]   = None
            self._lock = Lock()  # Protects session state
            self._initialized = True

    def __repr__(self) -> str:
        return f"<SessionManager user={self.get_username() or 'None'} logged_in={self.is_logged_in()}>"

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _clear_session(self) -> None:
        """Clear session state. Caller must hold self._lock."""
        self._current_user = None
        self._login_time   = None

    # ------------------------------------------------------------------
    # Session lifecycle
    # ------------------------------------------------------------------

    def login(self, user_data: UserData) -> bool:
        """Set the current logged-in user."""
        with self._lock:
            try:
                self._current_user = user_data.copy()
                self._login_time   = datetime.now(timezone.utc)
                _logger.info(
                    "Session started for user '%s' with role '%s'",
                    user_data.get("username"),
                    user_data.get("role"),
                )
                return True
            except Exception as e:
                _logger.error("Error starting session: %s", e)
                return False

    def logout(self) -> None:
        """Clear the current session."""
        with self._lock:
            if self._current_user:
                _logger.info(
                    "Session ended for user '%s'",
                    self._current_user.get("username", "unknown"),
                )
            self._clear_session()

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    def is_logged_in(self) -> bool:
        with self._lock:
            return self._current_user is not None

    def get_current_user(self) -> Optional[UserData]:
        with self._lock:
            return self._current_user.copy() if self._current_user else None

    def get_user_id(self) -> Optional[int]:
        with self._lock:
            return self._current_user.get("user_id") if self._current_user else None

    def get_username(self) -> Optional[str]:
        with self._lock:
            return self._current_user.get("username") if self._current_user else None

    def get_role(self) -> Optional[str]:
        with self._lock:
            return self._current_user.get("role") if self._current_user else None

    def get_login_time(self) -> Optional[datetime]:
        with self._lock:
            return self._login_time

    def update_user_data(self, updated_data: UserData) -> None:
        """
        Update selected fields in the current user's session data.

        This performs a controlled update:
        - Only keys present in `updated_data` are modified.
        - Existing fields may be explicitly set to None.
        - New fields are added only if their value is not None.
        - Missing keys in `updated_data` are left unchanged.

        Args:
            updated_data: Partial user information to merge into the session.

        Raises:
            RuntimeError: If no user is currently logged in.
        """
        with self._lock:
            if not self._current_user:
                raise RuntimeError("Cannot update session data: no user is logged in")
            
            # Safer update - only update provided fields
            for key, value in updated_data.items():
                if value is not None or key in self._current_user:  # optional: decide on None handling
                    self._current_user[key] = value
                    
            _logger.debug("Updated session data for user '%s'", 
                        self._current_user.get("username"))
