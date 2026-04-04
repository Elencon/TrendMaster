import sys
from PySide6.QtWidgets import QApplication
from .main_window import TestApiMainWindow


def main():
    app = QApplication(sys.argv)
    app.setApplicationName("TestApi")
    app.setApplicationDisplayName("TestApi - src.api Tester")

    # Set style (optional - clean look)
    app.setStyle("Fusion")

    window = TestApiMainWindow()
    window.show()

    sys.exit(app.exec())


if __name__ == "__main__":
    main()