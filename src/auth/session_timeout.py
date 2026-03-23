"""
Session Timeout Manager
Handles automatic logout after inactivity period.
"""

import logging
import time
from typing import Optional

from PySide6.QtCore import QTimer, QObject, Signal

logger = logging.getLogger(__name__)


class SessionTimeoutManager(QObject):
    """Manages session timeout with inactivity tracking."""

    timeout_warning = Signal(int)  # seconds remaining
    timeout_occurred = Signal()    # session timed out

    def __init__(
        self,
        timeout_minutes: int = 30,
        warning_seconds: int = 60,
        parent: Optional[QObject] = None,
    ):
        """
        Initialize session timeout manager.

        Args:
            timeout_minutes: Minutes of inactivity before timeout
            warning_seconds: Seconds before timeout to emit timeout_warning
            parent: Parent QObject
        """
        super().__init__(parent)

        # Read-only configuration
        self._timeout_minutes = timeout_minutes
        self._warning_seconds = warning_seconds

        # Runtime state
        self._last_activity: Optional[float] = None  # monotonic timestamp
        self._is_active: bool = False
        self._warning_shown: bool = False

        # Timer fires every second to check inactivity
        self._check_timer = QTimer(self)
        self._check_timer.setInterval(1000)
        self._check_timer.timeout.connect(self._check_inactivity)

        logger.info(
            f"SessionTimeoutManager initialised: "
            f"{timeout_minutes} min timeout, {warning_seconds}s warning"
        )

    # ------------------------------------------------------------------
    # Properties (read-only configuration)
    # ------------------------------------------------------------------

    @property
    def timeout_minutes(self) -> int:
        """Configured inactivity timeout in minutes."""
        return self._timeout_minutes

    @property
    def warning_seconds(self) -> int:
        """Seconds before timeout at which the warning signal is emitted."""
        return self._warning_seconds

    @property
    def is_active(self) -> bool:
        """True if timeout monitoring is currently running."""
        return self._is_active

    # ------------------------------------------------------------------
    # Session lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """
        Start monitoring session activity.

        If already active, logs a warning and does nothing to avoid
        silently resetting the inactivity timer.
        """
        if self._is_active:
            logger.warning("SessionTimeoutManager.start() called while already active — ignored")
            return

        self._last_activity = time.monotonic()
        self._is_active = True
        self._warning_shown = False
        self._check_timer.start()
        logger.info("Session timeout monitoring started")

    def stop(self) -> None:
        """Stop monitoring session activity and clear all state."""
        if not self._is_active:
            return

        self._is_active = False
        self._warning_shown = False
        self._last_activity = None  # Prevent stale data after stop
        self._check_timer.stop()
        logger.info("Session timeout monitoring stopped")

    def reset(self) -> None:
        """
        Reset the inactivity timer (call on any user action).

        Has no effect if monitoring is not active.
        """
        if not self._is_active:
            logger.warning("SessionTimeoutManager.reset() called while inactive — ignored")
            return

        self._last_activity = time.monotonic()
        self._warning_shown = False

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    def get_time_remaining(self) -> int:
        """
        Return seconds remaining before timeout.

        Returns the full timeout duration if monitoring is not active.
        """
        if not self._is_active or self._last_activity is None:
            return self._timeout_minutes * 60

        elapsed = time.monotonic() - self._last_activity
        remaining = self._timeout_minutes * 60 - elapsed
        return max(0, int(remaining))

    def get_inactive_duration(self) -> int:
        """
        Return seconds elapsed since the last recorded activity.

        Returns 0 if monitoring is not active or was never started.
        """
        if not self._is_active or self._last_activity is None:
            return 0
        return int(time.monotonic() - self._last_activity)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _check_inactivity(self) -> None:
        """Called every second by _check_timer to evaluate inactivity."""
        if not self._is_active or self._last_activity is None:
            return

        remaining = self.get_time_remaining()

        if remaining <= 0:
            logger.warning("Session timed out due to inactivity")
            self.stop()
            self.timeout_occurred.emit()
            return

        if remaining <= self._warning_seconds and not self._warning_shown:
            logger.info(f"Session timeout warning: {remaining}s remaining")
            self._warning_shown = True
            self.timeout_warning.emit(remaining)