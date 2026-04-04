from PySide6.QtWidgets import (
    QMainWindow, QTabWidget, QVBoxLayout, QWidget, QLabel, QMessageBox
)
from PySide6.QtCore import Qt


class TestApiMainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("TestApi - Real-time src.api Tester")
        self.setGeometry(100, 100, 1250, 850)

        # Core objects - No torch here
        self.data_loader = None
        self.model = None
        self.trainer = None
        self.inferencer = None

        self.train_data = None
        self.test_data = None
        self.symbol = None

        self.init_ui()

    def init_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)

        # Header
        header = QLabel("TestApi - Testing src.api Package")
        header.setAlignment(Qt.AlignCenter)
        header.setStyleSheet(
            "font-size: 20px; font-weight: bold; padding: 15px; "
            "background-color: #2c3e50; color: white;"
        )
        layout.addWidget(header)

        # Tabs
        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)

        # Import tabs here (lazy import to avoid early torch loading)
        from .tabs.data_tab import DataTab
        from .tabs.train_tab import TrainTab
        from .tabs.predict_tab import PredictTab
        from .tabs.visualize_tab import VisualizeTab

        # Initialize tabs
        self.data_tab = DataTab(self)
        self.train_tab = TrainTab(self)
        self.predict_tab = PredictTab(self)
        self.visualize_tab = VisualizeTab(self)

        self.tabs.addTab(self.data_tab, "1. Data & Authentication")
        self.tabs.addTab(self.train_tab, "2. Train Model")
        self.tabs.addTab(self.predict_tab, "3. Inference / Predict")
        self.tabs.addTab(self.visualize_tab, "4. Visualization")

    def show_message(self, title: str, message: str, icon=QMessageBox.Information):
        msg = QMessageBox(self)
        msg.setIcon(icon)
        msg.setWindowTitle(title)
        msg.setText(message)
        msg.exec()


# Optional: Helper to lazy import api classes when needed
def get_api_classes():
    """Lazy import to avoid torch dependency at startup"""
    from src.api import DataLoader, TransAm, Trainer, Inferencer, set_seed
    return DataLoader, TransAm, Trainer, Inferencer, set_seed