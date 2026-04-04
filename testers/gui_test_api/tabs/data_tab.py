from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QFormLayout, QLineEdit, QPushButton,
    QTextEdit, QLabel, QGroupBox, QHBoxLayout
)
import pyotp
from datetime import datetime


class DataTab(QWidget):
    def __init__(self, main_window):
        super().__init__()
        self.main = main_window
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # Authentication Group
        auth_group = QGroupBox("Zerodha Authentication")
        auth_layout = QFormLayout()

        self.user_id = QLineEdit("YOUR_USER_ID")
        self.password = QLineEdit()
        self.password.setEchoMode(QLineEdit.Password)
        self.totp_key = QLineEdit("YOUR_TOTP_SECRET_KEY")

        auth_layout.addRow("User ID:", self.user_id)
        auth_layout.addRow("Password:", self.password)
        auth_layout.addRow("TOTP Key:", self.totp_key)

        btn_auth = QPushButton("🔐 Authenticate with Zerodha")
        btn_auth.clicked.connect(self.authenticate)

        auth_layout.addRow(btn_auth)
        auth_group.setLayout(auth_layout)
        layout.addWidget(auth_group)

        # Data Preparation
        data_group = QGroupBox("Prepare Training Data")
        data_layout = QVBoxLayout()

        form = QFormLayout()
        self.symbol_input = QLineEdit("RELIANCE")
        self.from_date = QLineEdit("2023-01-01")
        self.to_date = QLineEdit(datetime.now().strftime("%Y-%m-%d"))

        form.addRow("Symbol:", self.symbol_input)
        form.addRow("From Date:", self.from_date)
        form.addRow("To Date:", self.to_date)

        btn_prepare = QPushButton("📊 Prepare Data (Train + Test)")
        btn_prepare.clicked.connect(self.prepare_data)

        data_layout.addLayout(form)
        data_layout.addWidget(btn_prepare)
        data_group.setLayout(data_layout)
        layout.addWidget(data_group)

        # Log Area
        self.log = QTextEdit()
        self.log.setReadOnly(True)
        layout.addWidget(QLabel("Log Output:"))
        layout.addWidget(self.log)

    def authenticate(self):
        try:
            totp = pyotp.TOTP(self.totp_key.text().strip()).now()
            kite = self.main.data_loader.authenticate(
                user_id=self.user_id.text().strip(),
                password=self.password.text().strip(),
                twofa=totp
            )
            self.log.append(f"✅ [{datetime.now().strftime('%H:%M:%S')}] Authentication Successful!")
            self.main.show_message("Success", "Successfully authenticated with Zerodha Kite Connect.")
        except Exception as e:
            self.log.append(f"❌ Error: {str(e)}")
            self.main.show_message("Authentication Failed", str(e), QMessageBox.Critical)

    def prepare_data(self):
        try:
            symbol = self.symbol_input.text().strip().upper()
            self.main.symbol = symbol

            self.log.append(f"🔄 Preparing data for {symbol}...")

            train_data, test_data = self.main.data_loader.prepare_data(
                symbol=symbol,
                from_date=self.from_date.text().strip(),
                to_date=self.to_date.text().strip(),
                input_window=30,
                output_window=10,
                train_test_split=0.8
            )

            self.main.train_data = train_data
            self.main.test_data = test_data

            self.log.append(f"✅ Data prepared successfully!")
            self.log.append(f"   Train samples: {len(train_data) if hasattr(train_data, '__len__') else 'N/A'}")
            self.log.append(f"   Test samples : {len(test_data) if hasattr(test_data, '__len__') else 'N/A'}")

            self.main.show_message("Success", f"Data for {symbol} prepared successfully.\nReady for training.")

        except Exception as e:
            self.log.append(f"❌ Error during data preparation: {str(e)}")
            self.main.show_message("Error", str(e), QMessageBox.Critical)