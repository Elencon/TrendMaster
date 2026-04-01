"""
Two-Factor Authentication Setup Dialog
Allows users to enable/disable 2FA with QR code scanning
"""

import sys
from pathlib import Path

from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QLineEdit, QTextEdit, QGroupBox, QMessageBox
)
from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QPixmap, QImage

from src.auth.two_factor_auth import TwoFactorAuth
import logging

logger = logging.getLogger(__name__)


class TwoFactorSetupDialog(QDialog):
    """Dialog for setting up two-factor authentication."""
    
    setup_completed = Signal(bool)  # Emits True if 2FA was enabled
    
    def __init__(self, user_id: int, username: str, db_connection, parent=None):
        super().__init__(parent)
        self.user_id = user_id
        self.username = username
        self.db_connection = db_connection
        self.two_factor = TwoFactorAuth(db_connection)
        self.secret = None
        self.backup_codes = None
        
        self.setWindowTitle("Two-Factor Authentication Setup")
        self.setMinimumWidth(500)
        self.setModal(True)
        
        self._setup_ui()
        self._check_2fa_status()
    
    def _setup_ui(self):
        """Set up the user interface."""
        layout = QVBoxLayout(self)
        
        # Title
        title = QLabel("Two-Factor Authentication (2FA)")
        title.setObjectName("section_title")
        title.setStyleSheet("font-size: 16pt; font-weight: bold; color: #0d6efd;")
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)
        
        # Status group
        self.status_group = QGroupBox("Current Status")
        status_layout = QVBoxLayout()
        
        self.status_label = QLabel("Checking status...")
        self.status_label.setWordWrap(True)
        status_layout.addWidget(self.status_label)
        
        self.status_group.setLayout(status_layout)
        layout.addWidget(self.status_group)
        
        # Setup group (shown when enabling 2FA)
        self.setup_group = QGroupBox("Enable Two-Factor Authentication")
        setup_layout = QVBoxLayout()
        
        info_label = QLabel(
            "Scan this QR code with your authenticator app:\n"
            "• Google Authenticator\n"
            "• Microsoft Authenticator\n"
            "• Authy\n"
            "• Or any TOTP-compatible app"
        )
        info_label.setWordWrap(True)
        setup_layout.addWidget(info_label)
        
        # QR code display
        self.qr_label = QLabel()
        self.qr_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.qr_label.setMinimumSize(250, 250)
        self.qr_label.setStyleSheet("border: 1px solid #ccc; background-color: white;")
        setup_layout.addWidget(self.qr_label)
        
        # Verification code input
        verify_layout = QHBoxLayout()
        verify_layout.addWidget(QLabel("Enter verification code:"))
        self.code_input = QLineEdit()
        self.code_input.setPlaceholderText("6-digit code")
        self.code_input.setMaxLength(6)
        self.code_input.setMaximumWidth(150)
        verify_layout.addWidget(self.code_input)
        verify_layout.addStretch()
        setup_layout.addLayout(verify_layout)
        
        # Backup codes display
        backup_label = QLabel("Backup Codes (save these in a safe place):")
        backup_label.setStyleSheet("font-weight: bold; margin-top: 10px;")
        setup_layout.addWidget(backup_label)
        
        self.backup_codes_text = QTextEdit()
        self.backup_codes_text.setReadOnly(True)
        self.backup_codes_text.setMaximumHeight(120)
        self.backup_codes_text.setStyleSheet("font-family: monospace; background-color: #f8f9fa;")
        setup_layout.addWidget(self.backup_codes_text)
        
        self.setup_group.setLayout(setup_layout)
        self.setup_group.setVisible(False)
        layout.addWidget(self.setup_group)
        
        # Buttons
        button_layout = QHBoxLayout()
        
        self.enable_btn = QPushButton("Enable 2FA")
        self.enable_btn.clicked.connect(self._start_2fa_setup)
        button_layout.addWidget(self.enable_btn)
        
        self.disable_btn = QPushButton("Disable 2FA")
        self.disable_btn.clicked.connect(self._disable_2fa)
        button_layout.addWidget(self.disable_btn)
        
        self.verify_btn = QPushButton("Verify & Complete Setup")
        self.verify_btn.clicked.connect(self._verify_and_enable)
        self.verify_btn.setVisible(False)
        button_layout.addWidget(self.verify_btn)
        
        button_layout.addStretch()
        
        self.close_btn = QPushButton("Close")
        self.close_btn.clicked.connect(self.accept)
        button_layout.addWidget(self.close_btn)
        
        layout.addLayout(button_layout)
    
    def _check_2fa_status(self):
        """Check if 2FA is currently enabled."""
        is_enabled = self.two_factor.is_2fa_enabled(self.user_id)
        
        if is_enabled:
            self.status_label.setText("✅ Two-Factor Authentication is ENABLED")
            self.status_label.setStyleSheet("color: green; font-weight: bold;")
            self.enable_btn.setEnabled(False)
            self.disable_btn.setEnabled(True)
        else:
            self.status_label.setText("❌ Two-Factor Authentication is DISABLED")
            self.status_label.setStyleSheet("color: #666; font-weight: bold;")
            self.enable_btn.setEnabled(True)
            self.disable_btn.setEnabled(False)
    
    def _start_2fa_setup(self):
        """Start the 2FA setup process."""
        try:
            # Generate secret and QR code
            self.secret = self.two_factor.generate_secret()
            qr_bytes = self.two_factor.generate_qr_code(self.username, self.secret)
            
            # Display QR code
            qimage = QImage.fromData(qr_bytes)
            pixmap = QPixmap.fromImage(qimage)
            scaled_pixmap = pixmap.scaled(250, 250, Qt.AspectRatioMode.KeepAspectRatio)
            self.qr_label.setPixmap(scaled_pixmap)
            
            # Generate backup codes
            self.backup_codes = self.two_factor.generate_backup_codes()
            backup_text = "\n".join([f"{i+1}. {code}" for i, code in enumerate(self.backup_codes)])
            self.backup_codes_text.setPlainText(backup_text)
            
            # Show setup UI
            self.setup_group.setVisible(True)
            self.enable_btn.setVisible(False)
            self.verify_btn.setVisible(True)
            
            logger.info(f"2FA setup started for user: {self.username}")
            
        except Exception as e:
            logger.error(f"Error starting 2FA setup: {e}")
            QMessageBox.critical(self, "Error", f"Failed to start 2FA setup:\n{str(e)}")
    
    def _verify_and_enable(self):
        """Verify the code and enable 2FA."""
        code = self.code_input.text().strip()
        
        if not code or len(code) != 6:
            QMessageBox.warning(self, "Invalid Code", "Please enter a 6-digit verification code.")
            return
        
        # Verify the code
        if not self.two_factor.verify_code(self.secret, code):
            QMessageBox.warning(
                self,
                "Verification Failed",
                "The code you entered is incorrect. Please try again."
            )
            return
        
        # Enable 2FA in database
        if self.two_factor.enable_2fa(self.user_id, self.secret, self.backup_codes):
            QMessageBox.information(
                self,
                "Success",
                "Two-Factor Authentication has been enabled successfully!\n\n"
                "⚠️ IMPORTANT: Save your backup codes in a secure location.\n"
                "You'll need them if you lose access to your authenticator app."
            )
            
            self._check_2fa_status()
            self.setup_group.setVisible(False)
            self.enable_btn.setVisible(True)
            self.verify_btn.setVisible(False)
            self.setup_completed.emit(True)
            
            logger.info(f"2FA enabled successfully for user: {self.username}")
        else:
            QMessageBox.critical(self, "Error", "Failed to enable 2FA. Please try again.")
    
    def _disable_2fa(self):
        """Disable two-factor authentication."""
        reply = QMessageBox.question(
            self,
            "Disable 2FA",
            "Are you sure you want to disable Two-Factor Authentication?\n\n"
            "This will make your account less secure.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            if self.two_factor.disable_2fa(self.user_id):
                QMessageBox.information(
                    self,
                    "Success",
                    "Two-Factor Authentication has been disabled."
                )
                
                self._check_2fa_status()
                self.setup_completed.emit(False)
                
                logger.info(f"2FA disabled for user: {self.username}")
            else:
                QMessageBox.critical(self, "Error", "Failed to disable 2FA. Please try again.")
