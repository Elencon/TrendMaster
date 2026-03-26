"""UI Builder for Admin Window - Extracted UI creation methods"""


from PySide6.QtWidgets import (QVBoxLayout, QPushButton, QLineEdit, QLabel,
                               QSplitter, QWidget, QToolBar)
from PySide6.QtCore import Qt

from .ui_components import (create_title_section, create_api_section, create_file_section,
                            create_data_section, create_database_section, create_test_section,
                            create_progress_bar, create_output_section)


class AdminUIBuilder:
    """Handles all UI construction for the admin window"""

    def __init__(self, window):
        """
        Initialize the UI builder.
        
        Args:
            window: Reference to the parent ETLMainWindow instance
        """
        self.window = window

    def create_toolbar(self) -> QToolBar:
        """
        Create toolbar (empty for consistency with dashboard).
        
        Returns:
            QToolBar: The created toolbar
        """
        toolbar = QToolBar("Main Toolbar")
        toolbar.setMovable(False)
        return toolbar

    def create_main_layout(self) -> QSplitter:
        """
        Create the main layout with controls and output sections.
        
        Returns:
            QSplitter: The main splitter widget
        """
        splitter = QSplitter(Qt.Vertical)

        # Controls widget
        controls_widget = QWidget()
        controls_layout = QVBoxLayout(controls_widget)

        self._create_all_sections(controls_layout)

        # Progress bar
        self.window.progress_bar = create_progress_bar()
        controls_layout.addWidget(self.window.progress_bar)

        # Output widget
        output_widget, self.window.output_text = create_output_section()

        # Add to splitter
        splitter.addWidget(controls_widget)
        splitter.addWidget(output_widget)
        splitter.setSizes([400, 300])

        return splitter

    def _create_all_sections(self, layout: QVBoxLayout):
        """
        Create all UI sections.
        
        Args:
            layout: Main vertical layout to add sections to
        """
        # Title section
        create_title_section(layout)

        # API section
        self._create_api_section(layout)

        # File selection section
        self._create_file_section(layout)

        # Data loading section
        self._create_data_section(layout)

        # Database section
        self._create_database_section(layout)

        # Test section
        self._create_test_section(layout)

        layout.addStretch()

    def _create_api_section(self, layout: QVBoxLayout):
        """Create API configuration section"""
        self.window.api_url_input = QLineEdit()
        self.window.api_url_input.setText(
            self.window.settings.value("api_url", "https://etl-server.fly.dev")
        )
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
        """Create file selection section"""
        self.window.select_csv_btn = self._create_button(
            "Select CSV Files",
            self.window.select_csv_files,
            "select_csv_btn"
        )

        self.window.load_selected_files_btn = QPushButton("Load CSV Files")
        self.window.load_selected_files_btn.setObjectName("load_selected_files_btn")
        self.window.load_selected_files_btn.clicked.connect(self.window.load_selected_files)

        self.window.selected_files_label = QLabel()

        create_file_section(
            layout,
            self.window.select_csv_btn,
            self.window.load_selected_files_btn,
            self.window.selected_files_label
        )

    def _create_data_section(self, layout: QVBoxLayout):
        """Create data loading section"""
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
        """Create database operations section"""
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
        """Create test operations section"""
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

    def _create_button(self, text: str, callback, button_id: str) -> QPushButton:
        """
        Create and register a button in operation_buttons.
        
        Args:
            text: Button text
            callback: Click callback function
            button_id: Unique identifier for the button

        Returns:
            QPushButton: The created button
        """
        btn = QPushButton(text)
        btn.clicked.connect(callback)
        self.window.operation_buttons[button_id] = btn
        return btn
