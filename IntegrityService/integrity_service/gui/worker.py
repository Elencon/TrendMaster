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

class Worker(QThread):
    finished = Signal(dict)

    def __init__(self, mode: str):
        super().__init__()
        self.mode = mode

    def run(self):
        if self.mode == "integrity":
            result = run_integrity_only()
        else:
            result = run_integrity_and_backup()

        self.finished.emit(result)

