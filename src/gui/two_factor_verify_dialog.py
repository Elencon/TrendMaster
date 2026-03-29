"""
Two-Factor Authentication Verification Dialog
Shown after password login to verify TOTP code
"""

import logging
from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QLineEdit, QMessageBox, QInputDialog
)
from PySide6.QtCore import Qt, Signal

from auth.two_factor_auth import TwoFactorAuth

logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# Centralized style constants
# ---------------------------------------------------------
TITLE_STYLE = "font-size: 14pt; font-weight: bold; color: #0d6efd;"
CODE_STYLE = "font-size: 16pt; font-family: monospace;"
BACKUP_HINT_STYLE = "color: #666; font-style: italic;"


class TwoFactorVerifyDialog(QDialog):
    """Dialog for verifying 2FA code during login."""

    verification_successful = Signal()

    def __init__(self, user_id: int, username: str, db_connection, parent=None):
        super().__init__(parent)

        # Protected internal state
        self._user_id = user_id
        self._username = username
        self._db_connection = db_connection
        self._two_factor = TwoFactorAuth(db_connection)

        self.setWindowTitle("Two-Factor Authentication")
        self.setMinimumWidth(400)
        self.setModal(True)

        self._setup_ui()

    # ---------------------------------------------------------
    # UI Setup
    # ---------------------------------------------------------
    def _setup_ui(self):
        layout = QVBoxLayout(self)

        # Title
        title = QLabel("Two-Factor Authentication Required")
        title.setStyleSheet(TITLE_STYLE)
        title.setAlignment(Qt.AlignCenter)
        layout.addWidget(title)

        layout.addSpacing(20)

        # Instructions
        info = QLabel(
            f"Enter the 6-digit verification code from your authenticator app.\n\n"
            f"Username: {self._username}"
        )
        info.setWordWrap(True)
        info.setAlignment(Qt.AlignCenter)
        layout.addWidget(info)

        layout.addSpacing(20)

        # Code input
        code_layout = QHBoxLayout()
        code_layout.addStretch()

        code_label = QLabel("Verification Code:")
        code_label.setStyleSheet("font-weight: bold;")
        code_layout.addWidget(code_label)

        self._code_input = QLineEdit()
        self._code_input.setPlaceholderText("000000")
        self._code_input.setMaxLength(6)
        self._code_input.setMinimumWidth(120)
        self._code_input.setAlignment(Qt.AlignCenter)
        self._code_input.setStyleSheet(CODE_STYLE)
        self._code_input.returnPressed.connect(self._verify_code)
        code_layout.addWidget(self._code_input)

        code_layout.addStretch()
        layout.addLayout(code_layout)

        layout.addSpacing(20)

        # Backup code option
        backup_label = QLabel("Lost your device? Use a backup code instead.")
        backup_label.setStyleSheet(BACKUP_HINT_STYLE)
        backup_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(backup_label)

        self._use_backup_btn = QPushButton("Use Backup Code")
        self._use_backup_btn.clicked.connect(self._use_backup_code)
        layout.addWidget(self._use_backup_btn)

        layout.addSpacing(20)

        # Buttons
        button_layout = QHBoxLayout()

        self._verify_btn = QPushButton("Verify")
        self._verify_btn.clicked.connect(self._verify_code)
        self._verify_btn.setDefault(True)
        button_layout.addWidget(self._verify_btn)

        self._cancel_btn = QPushButton("Cancel")
        self._cancel_btn.clicked.connect(self.reject)
        button_layout.addWidget(self._cancel_btn)

        layout.addLayout(button_layout)

        # Focus on code input
        self._code_input.setFocus()

    # ---------------------------------------------------------
    # Verification Logic
    # ---------------------------------------------------------
    def _verify_code(self):
        code = self._code_input.text().strip()

        if len(code) != 6:
            self._warn("Please enter a 6-digit code.")
            return

        secret = self._two_factor.get_user_secret(self._user_id)
        if not secret:
            self._error("2FA configuration error. Please contact administrator.")
            self.reject()
            return

        if not self._two_factor.verify_code(secret, code):
            self._warn("Incorrect verification code. Please try again.")
            self._code_input.clear()
            self._code_input.setFocus()
            return

        logger.info(f"2FA verification successful for user: {self._username}")
        self.verification_successful.emit()
        self.accept()

    # ---------------------------------------------------------
    # Backup Code Logic
    # ---------------------------------------------------------
    def _use_backup_code(self):
        backup_code, ok = self._prompt_for_backup_code()

        if not ok or not backup_code:
            return

        if not self._two_factor.verify_backup_code(self._user_id, backup_code):
            self._warn("Invalid or already used backup code.")
            return

        remaining = len(self._two_factor.get_remaining_backup_codes(self._user_id))

        QMessageBox.information(
            self,
            "Backup Code Accepted",
            f"Backup code verified successfully!\n\n"
            f"You have {remaining} backup codes remaining.\n"
            f"Consider regenerating codes if you're running low."
        )

        logger.info(f"Backup code used for user: {self._username}")
        self.verification_successful.emit()
        self.accept()

    def _prompt_for_backup_code(self):
        code, ok = QInputDialog.getText(
            self,
            "Enter Backup Code",
            "Backup Code (8 characters):",
            QLineEdit.Normal,
            ""
        )
        return code.strip().upper(), ok

    # ---------------------------------------------------------
    # Protected helpers
    # ---------------------------------------------------------
    def _error(self, message: str):
        QMessageBox.critical(self, "Error", message)

    def _warn(self, message: str):
        QMessageBox.warning(self, "Warning", message)
