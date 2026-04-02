"""UI Builder for Admin Window - Extracted UI creation methods"""

from typing import Callable, TYPE_CHECKING

from PySide6.QtWidgets import (
    QVBoxLayout, QPushButton, QLineEdit, QLabel,
    QSplitter, QWidget, QToolBar
)
from PySide6.QtCore import Qt

from .ui_components import (
    create_title_section, create_api_section, create_file_section,
    create_data_section, create_database_section, create_test_section,
    create_progress_bar, create_output_section
)

if TYPE_CHECKING:
    from .main_window import ETLMainWindow


class AdminUIBuilder:
    """Handles all UI construction for the admin window"""

    DEFAULT_API_URL = "https://etl-server.fly.dev"
    SPLITTER_SIZES = (400, 300)

    def __init__(self, window: "ETLMainWindow"):
        self.window = window

    def create_toolbar(self) -> QToolBar:
        toolbar = QToolBar("Main Toolbar")
        toolbar.setMovable(False)
        return toolbar

    def create_main_layout(self) -> QSplitter:
        splitter = QSplitter(Qt.Vertical)

        controls_widget = QWidget()
        controls_layout = QVBoxLayout(controls_widget)

        # Optional UI polish
        controls_layout.setContentsMargins(8, 8, 8, 8)
        controls_layout.setSpacing(6)

        self._create_all_sections(controls_layout)

        self.window.progress_bar = create_progress_bar()
        controls_layout.addWidget(self.window.progress_bar)

        output_widget, self.window.output_text = create_output_section()

        splitter.addWidget(controls_widget)
        splitter.addWidget(output_widget)
        splitter.setSizes(list(self.SPLITTER_SIZES))

        return splitter

    def _create_all_sections(self, layout: QVBoxLayout):
        create_title_section(layout)
        self._create_api_section(layout)
        self._create_file_section(layout)
        self._create_data_section(layout)
        self._create_database_section(layout)
        self._create_test_section(layout)
        layout.addStretch()

    def _create_api_section(self, layout: QVBoxLayout):
        self.window.api_url_input = QLineEdit()

        settings = getattr(self.window, "settings", None)
        api_url = (
            settings.value("api_url", self.DEFAULT_API_URL)
            if settings else self.DEFAULT_API_URL
        )
        self.window.api_url_input.setText(api_url)

        self.window.load_api_btn = self._create_button(
            "Test",
            self.window.test_api_connection,
            "load_api_btn"
        )

        create_api_section(
            layout,
            self.window.api_url_input,
            self.window.load_api_btn
        )

    def _create_file_section(self, layout: QVBoxLayout):
        self.window.select_csv_btn = self._create_button(
            "Select CSV Files",
            self.window.select_csv_files,
            "select_csv_btn"
        )

        self.window.load_selected_files_btn = self._create_button(
            "Load CSV Files",
            self.window.load_selected_files,
            "load_selected_files_btn"
        )

        self.window.selected_files_label = QLabel()

        create_file_section(
            layout,
            self.window.select_csv_btn,
            self.window.load_selected_files_btn,
            self.window.selected_files_label
        )

    def _create_data_section(self, layout: QVBoxLayout):
        self.window.load_csv_btn = self._create_button(
            "Load CSV Data",
            self.window.load_csv_data,
            "load_csv_btn"
        )

        self.window.load_api_data_btn = self._create_button(
            "Load API Data",
            self.window.load_api_data,
            "load_api_data_btn"
        )

        create_data_section(
            layout,
            self.window.load_csv_btn,
            self.window.load_api_data_btn
        )

    def _create_database_section(self, layout: QVBoxLayout):
        self.window.test_conn_btn = self._create_button(
            "Test Connection",
            self.window.test_db_connection,
            "test_conn_btn"
        )

        self.window.create_tables_btn = self._create_button(
            "Create Tables",
            self.window.create_tables,
            "create_tables_btn"
        )

        create_database_section(
            layout,
            self.window.test_conn_btn,
            self.window.create_tables_btn
        )

    def _create_test_section(self, layout: QVBoxLayout):
        self.window.test_csv_btn = self._create_button(
            "Test CSV Access",
            self.window.test_csv_access,
            "test_csv_btn"
        )

        self.window.test_api_export_btn = self._create_button(
            "Test API Export",
            self.window.test_api_export,
            "test_api_export_btn"
        )

        create_test_section(
            layout,
            self.window.test_csv_btn,
            self.window.test_api_export_btn
        )

    def _create_button(
        self,
        text: str,
        callback: Callable,
        button_id: str
    ) -> QPushButton:
        btn = QPushButton(text)
        btn.setObjectName(button_id)
        btn.clicked.connect(callback)

        self.window.operation_buttons[button_id] = btn
        return btn
