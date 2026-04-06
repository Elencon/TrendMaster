"""
run from TrendMaster
cd IntegrityService
python -m integrity_service.gui.app
"""
import sys
from PySide6.QtWidgets import QApplication
from .main_window import MainWindow


def main():
    app = QApplication(sys.argv)

    # Load QSS stylesheet
    try:
        with open("integrity_service/gui/styles.qss", "r", encoding="utf-8") as f:
            app.setStyleSheet(f.read())
    except FileNotFoundError:
        # Optional: silently ignore if style file is missing
        pass

    window = MainWindow()
    window.show()

    sys.exit(app.exec())


if __name__ == "__main__":
    main()