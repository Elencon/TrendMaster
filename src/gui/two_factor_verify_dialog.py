"""
Two-Factor Authentication Verification Dialog
Shown after password login to verify TOTP code
"""

import sys
from pathlib import Path

from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QLineEdit, QMessageBox,
)
from PySide6.QtCore import Qt, Signal

from auth.two_factor_auth import TwoFactorAuth
import logging

# Add src to path
gui_path = Path(__file__).parent.parent
src_path = gui_path / "src"
sys.path.insert(0, str(src_path))

logger = logging.getLogger(__name__)


class TwoFactorVerifyDialog(QDialog):
    """Dialog for verifying 2FA code during login."""

    verification_successful = Signal()

    def __init__(self, user_id: int, username: str, db_connection, parent=None):
        super().__init__(parent)
        self.user_id = user_id
        self.username = username
        self.db_connection = db_connection
        self.two_factor = TwoFactorAuth(db_connection)

        self.setWindowTitle("Two-Factor Authentication")
        self.setMinimumWidth(400)
        self.setModal(True)

        self._setup_ui()

    def _setup_ui(self):
        """Set up the user interface."""
        layout = QVBoxLayout(self)

        # Title
        title = QLabel("Two-Factor Authentication Required")
        title.setStyleSheet("font-size: 14pt; font-weight: bold; color: #0d6efd;")
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)

        layout.addSpacing(20)

        # Instructions
        info = QLabel(
            f"Enter the 6-digit verification code from your authenticator app.\n\n"
            f"Username: {self.username}"
        )
        info.setWordWrap(True)
        info.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(info)

        layout.addSpacing(20)

        # Code input
        code_layout = QHBoxLayout()
        code_layout.addStretch()

        code_label = QLabel("Verification Code:")
        code_label.setStyleSheet("font-weight: bold;")
        code_layout.addWidget(code_label)

        self.code_input = QLineEdit()
        self.code_input.setPlaceholderText("000000")
        self.code_input.setMaxLength(6)
        self.code_input.setMinimumWidth(120)
        self.code_input.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.code_input.setStyleSheet("font-size: 16pt; font-family: monospace;")
        self.code_input.returnPressed.connect(self._verify_code)
        code_layout.addWidget(self.code_input)

        code_layout.addStretch()
        layout.addLayout(code_layout)

        layout.addSpacing(20)

        # Backup code option
        backup_label = QLabel("Lost your device? Use a backup code instead.")
        backup_label.setStyleSheet("color: #666; font-style: italic;")
        backup_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(backup_label)

        self.use_backup_btn = QPushButton("Use Backup Code")
        self.use_backup_btn.clicked.connect(self._use_backup_code)
        layout.addWidget(self.use_backup_btn)

        layout.addSpacing(20)

        # Buttons
        button_layout = QHBoxLayout()

        self.verify_btn = QPushButton("Verify")
        self.verify_btn.clicked.connect(self._verify_code)
        self.verify_btn.setDefault(True)
        button_layout.addWidget(self.verify_btn)

        self.cancel_btn = QPushButton("Cancel")
        self.cancel_btn.clicked.connect(self.reject)
        button_layout.addWidget(self.cancel_btn)

        layout.addLayout(button_layout)

        # Focus on code input
        self.code_input.setFocus()

    def _verify_code(self):
        """Verify the entered code."""
        code = self.code_input.text().strip()

        if not code or len(code) != 6:
            QMessageBox.warning(self, "Invalid Code", "Please enter a 6-digit code.")
            return

        # Get user's secret
        secret = self.two_factor.get_user_secret(self.user_id)

        if not secret:
            QMessageBox.critical(self, "Error", "2FA configuration error. Please contact administrator.")
            self.reject()
            return

        # Verify the code
        if self.two_factor.verify_code(secret, code):
            logger.info(f"2FA verification successful for user: {self.username}")
            self.verification_successful.emit()
            self.accept()
        else:
            QMessageBox.warning(
                self,
                "Verification Failed",
                "The code you entered is incorrect. Please try again."
            )
            self.code_input.clear()
            self.code_input.setFocus()

    def _use_backup_code(self):
        """Use a backup code instead of TOTP code."""
        backup_code, ok = self._prompt_for_backup_code()

        if ok and backup_code:
            if self.two_factor.verify_backup_code(self.user_id, backup_code):
                remaining = len(self.two_factor.get_remaining_backup_codes(self.user_id))

                QMessageBox.information(
                    self,
                    "Backup Code Accepted",
                    f"Backup code verified successfully!\n\n"
                    f"You have {remaining} backup codes remaining.\n"
                    f"Consider regenerating codes if you're running low."
                )

                logger.info(f"Backup code used for user: {self.username}")
                self.verification_successful.emit()
                self.accept()
            else:
                QMessageBox.warning(
                    self,
                    "Invalid Backup Code",
                    "The backup code you entered is invalid or has already been used."
                )

    def _prompt_for_backup_code(self):
        """Prompt user to enter a backup code."""
        from PySide6.QtWidgets import QInputDialog

        code, ok = QInputDialog.getText(
            self,
            "Enter Backup Code",
            "Backup Code (8 characters):",
            QLineEdit.EchoMode.Normal,
            ""
        )

        return code.strip().upper(), ok
