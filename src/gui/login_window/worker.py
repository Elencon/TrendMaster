"""
Worker thread for login window background operations.
Handles database authentication without blocking the UI.
"""

import logging
from PySide6.QtCore import QThread, Signal

logger = logging.getLogger(__name__)


class LoginWorker(QThread):
    """Worker thread for login authentication."""

    # Signals
    authentication_complete = Signal(bool, object)  # success, user_data

    def __init__(self, user_manager, username: str, password: str):
        """
        Initialize LoginWorker.

        Args:
            user_manager: UserManager instance
            username: Username to authenticate
            password: Password to authenticate
        """
        super().__init__()
        self._user_manager = user_manager
        self._username = username
        self._password = password

    def run(self):
        """Run authentication in background thread."""
        try:
            logger.info(f"Authenticating user '{self._username}'")

            # UserManager.authenticate may return:
            #   - dict (success)
            #   - None / False (failure)
            user_data = self._user_manager.authenticate(
                self._username,
                self._password
            )

            if user_data:
                self.authentication_complete.emit(True, user_data)
                return

            # Authentication failed
            self.authentication_complete.emit(False, None)

        except Exception as exc:
            logger.error(f"Authentication error: {exc}")
            self.authentication_complete.emit(False, None)
