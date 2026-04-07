from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QPushButton,
    QTextEdit,
    QHBoxLayout,
    QMessageBox,
)
from PySide6.QtCore import QThread, Signal

from integrity_service.integrity_runner import (
    run_integrity_only,
    run_integrity_and_backup,
)

from integrity_service.gui.themes import ThemeManager


class MainWindow(QWidget):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Integrity & Backup Service")
        self.setMinimumSize(800, 600)

        layout = QVBoxLayout()

        # -------------------------
        # Buttons row
        # -------------------------
        btn_layout = QHBoxLayout()

        self.btn_integrity = QPushButton("Run Integrity Check")
        self.btn_integrity.clicked.connect(self.run_integrity)

        self.btn_full = QPushButton("Run Integrity + Backup")
        self.btn_full.clicked.connect(self.run_full)

        btn_layout.addWidget(self.btn_integrity)
        btn_layout.addWidget(self.btn_full)

        # -------------------------
        # Output panel
        # -------------------------
        self.output = QTextEdit()
        self.output.setReadOnly(True)
        self.output.setLineWrapMode(QTextEdit.NoWrap)

        layout.addLayout(btn_layout)
        layout.addWidget(self.output)

        self.setLayout(layout)

        self.worker = None

    # -------------------------
    # Button handlers
    # -------------------------
    def run_integrity(self):
        self._start_worker("integrity")

    def run_full(self):
        self._start_worker("full")

    # -------------------------
    # Worker thread handling
    # -------------------------
    def _start_worker(self, mode: str):
        if self.worker is not None and self.worker.isRunning():
            QMessageBox.information(self, "Please wait", "A task is already running.")
            return

        self.output.append(f"Starting {mode}...\n")

        self.worker = Worker(mode)
        self.worker.finished.connect(self._on_finished)
        self.worker.start()

    def _on_finished(self, result: dict):
        self.output.append("=== Result ===")
        self.output.append(str(result) + "\n")

        if not result.get("success", False):
            QMessageBox.warning(self, "Integrity Check Failed", "Some checks failed.")
        else:
            QMessageBox.information(self, "Success", "All checks passed.")