"""
UI Builder for Admin Window - Extracted UI creation methods
"""

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


class AdminUIBuilder:
    """Handles all UI construction for the admin window."""

    def __init__(self, window):
        self._window = window   # protected reference to parent window

    # ---------------------------------------------------------
    # Toolbar
    # ---------------------------------------------------------
    def create_toolbar(self) -> QToolBar:
        toolbar = QToolBar("Main Toolbar")
        toolbar.setMovable(False)
        return toolbar

    # ---------------------------------------------------------
    # Main Layout
    # ---------------------------------------------------------
    def create_main_layout(self) -> QSplitter:
        splitter = QSplitter(Qt.Vertical)

        # Controls
        controls_widget = QWidget()
        controls_layout = QVBoxLayout(controls_widget)
        self._create_all_sections(controls_layout)

        # Progress bar
        self._window.progress_bar = create_progress_bar()
        controls_layout.addWidget(self._window.progress_bar)

        # Output
        output_widget, self._window.output_text = create_output_section()

        splitter.addWidget(controls_widget)
        splitter.addWidget(output_widget)
        splitter.setSizes([400, 300])

        return splitter

    # ---------------------------------------------------------
    # Section Builder
    # ---------------------------------------------------------
    def _create_all_sections(self, layout: QVBoxLayout):
        create_title_section(layout)
        self._create_api_section(layout)
        self._create_file_section(layout)
        self._create_data_section(layout)
        self._create_database_section(layout)
        self._create_test_section(layout)
        layout.addStretch()

    # ---------------------------------------------------------
    # API Section
    # ---------------------------------------------------------
    def _create_api_section(self, layout: QVBoxLayout):
        self._window.api_url_input = QLineEdit()
        self._window.api_url_input.setText(
            self._window.settings.value("api_url", "https://etl-server.fly.dev")
        )

        self._window.load_api_btn = self._create_button(
            "Test",
            self._window.test_api_connection,
            "load_api_btn"
        )

        create_api_section(
            layout,
            self._window.api_url_input,
            self._window.load_api_btn
        )

    # ---------------------------------------------------------
    # File Section
    # ---------------------------------------------------------
    def _create_file_section(self, layout: QVBoxLayout):
        self._window.select_csv_btn = self._create_button(
            "Select CSV Files",
            self._window.select_csv_files,
            "select_csv_btn"
        )

        self._window.load_selected_files_btn = self._create_button(
            "Load CSV Files",
            self._window.load_selected_files,
            "load_selected_files_btn"
        )

        self._window.selected_files_label = QLabel()

        create_file_section(
            layout,
            self._window.select_csv_btn,
            self._window.load_selected_files_btn,
            self._window.selected_files_label
        )

    # ---------------------------------------------------------
    # Data Section
    # ---------------------------------------------------------
    def _create_data_section(self, layout: QVBoxLayout):
        self._window.load_csv_btn = self._create_button(
            "Load CSV Data",
            self._window.load_csv_data,
            "load_csv_btn"
        )

        self._window.load_api_data_btn = self._create_button(
            "Load API Data",
            self._window.load_api_data,
            "load_api_data_btn"
        )

        create_data_section(
            layout,
            self._window.load_csv_btn,
            self._window.load_api_data_btn
        )

    # ---------------------------------------------------------
    # Database Section
    # ---------------------------------------------------------
    def _create_database_section(self, layout: QVBoxLayout):
        self._window.test_conn_btn = self._create_button(
            "Test Connection",
            self._window.test_db_connection,
            "test_conn_btn"
        )

        self._window.create_tables_btn = self._create_button(
            "Create Tables",
            self._window.create_tables,
            "create_tables_btn"
        )

        create_database_section(
            layout,
            self._window.test_conn_btn,
            self._window.create_tables_btn
        )

    # ---------------------------------------------------------
    # Test Section
    # ---------------------------------------------------------
    def _create_test_section(self, layout: QVBoxLayout):
        self._window.test_csv_btn = self._create_button(
            "Test CSV Access",
            self._window.test_csv_access,
            "test_csv_btn"
        )

        self._window.test_api_export_btn = self._create_button(
            "Test API Export",
            self._window.test_api_export,
            "test_api_export_btn"
        )

        create_test_section(
            layout,
            self._window.test_csv_btn,
            self._window.test_api_export_btn
        )

    # ---------------------------------------------------------
    # Button Factory
    # ---------------------------------------------------------
    def _create_button(self, text: str, callback, button_id: str) -> QPushButton:
        btn = QPushButton(text)
        btn.setObjectName(button_id)
        btn.clicked.connect(callback)
        self._window.operation_buttons[button_id] = btn
        return btn
