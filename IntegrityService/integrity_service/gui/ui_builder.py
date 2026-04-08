from __future__ import annotations

"""UI Builder for Admin Window - Extracted UI creation methods"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QLabel,
    QLineEdit,
    QPushButton,
    QSplitter,
    QToolBar,
    QVBoxLayout,
    QWidget,
)

from .ui_components import (
    create_api_section,
    create_data_section,
    create_database_section,
    create_file_section,
    create_output_section,
    create_progress_bar,
    create_test_section,
    create_title_section,
)

if TYPE_CHECKING:
    from .main_window import ETLMainWindow

__all__ = ["AdminUIBuilder", "UIConfig"]


@dataclass(frozen=True, slots=True)
class UIConfig:
    """Configuration for UI constants."""

    default_api_url: str = "https://etl-server.fly.dev"
    splitter_sizes: tuple[int, int] = (400, 300)


class AdminUIBuilder:
    """Handles all UI construction for the admin window."""

    def __init__(self, window: ETLMainWindow) -> None:
        self.window = window
        self.config = UIConfig()

    # -----------------------------
    # Toolbar
    # -----------------------------
    def create_toolbar(self) -> QToolBar:
        """Create the main toolbar."""
        toolbar = QToolBar("Main Toolbar")
        toolbar.setMovable(False)
        return toolbar

    # -----------------------------
    # Main Layout
    # -----------------------------
    def create_main_layout(self) -> QSplitter:
        """Create the main vertical splitter with controls and output."""
        splitter = QSplitter(Qt.Orientation.Vertical)

        # Controls section (top)
        controls_widget = QWidget()
        controls_layout = QVBoxLayout(controls_widget)
        controls_layout.setContentsMargins(8, 8, 8, 8)
        controls_layout.setSpacing(6)

        self._create_all_sections(controls_layout)

        # Progress bar
        self.window.progress_bar = create_progress_bar()
        controls_layout.addWidget(self.window.progress_bar)

        # Output section (bottom)
        output_widget, self.window.output_text = create_output_section()

        splitter.addWidget(controls_widget)
        splitter.addWidget(output_widget)
        splitter.setSizes(self.config.splitter_sizes)

        return splitter

    # -----------------------------
    # Section Builders
    # -----------------------------
    def _create_all_sections(self, layout: QVBoxLayout) -> None:
        """Create all control sections in order."""
        create_title_section(layout)
        self._create_api_section(layout)
        self._create_file_section(layout)
        self._create_data_section(layout)
        self._create_database_section(layout)
        self._create_test_section(layout)
        layout.addStretch()

    def _create_api_section(self, layout: QVBoxLayout) -> None:
        """Create API configuration section."""
        settings = getattr(self.window, "settings", None)
        api_url = (
            settings.value("api_url", self.config.default_api_url)
            if settings is not None
            else self.config.default_api_url
        )

        self.window.api_url_input = QLineEdit(api_url)
        self.window.load_api_btn = self._create_button(
            text="Test",
            callback=self.window.test_api_connection,
            button_id="load_api_btn",
        )

        create_api_section(
            layout=layout,
            api_url_input=self.window.api_url_input,
            test_button=self.window.load_api_btn,
        )

    def _create_file_section(self, layout: QVBoxLayout) -> None:
        """Create CSV file selection section."""
        self.window.select_csv_btn = self._create_button(
            text="Select CSV Files",
            callback=self.window.select_csv_files,
            button_id="select_csv_btn",
        )
        self.window.load_selected_files_btn = self._create_button(
            text="Load CSV Files",
            callback=self.window.load_selected_files,
            button_id="load_selected_files_btn",
        )
        self.window.selected_files_label = QLabel()

        create_file_section(
            layout=layout,
            select_btn=self.window.select_csv_btn,
            load_btn=self.window.load_selected_files_btn,
            files_label=self.window.selected_files_label,
        )

    def _create_data_section(self, layout: QVBoxLayout) -> None:
        """Create data loading section."""
        self.window.load_csv_btn = self._create_button(
            text="Load CSV Data",
            callback=self.window.load_csv_data,
            button_id="load_csv_btn",
        )
        self.window.load_api_data_btn = self._create_button(
            text="Load API Data",
            callback=self.window.load_api_data,
            button_id="load_api_data_btn",
        )

        create_data_section(
            layout=layout,
            load_csv_btn=self.window.load_csv_btn,
            load_api_btn=self.window.load_api_data_btn,
        )

    def _create_database_section(self, layout: QVBoxLayout) -> None:
        """Create database management section."""
        self.window.test_conn_btn = self._create_button(
            text="Test Connection",
            callback=self.window.test_db_connection,
            button_id="test_conn_btn",
        )
        self.window.create_tables_btn = self._create_button(
            text="Create Tables",
            callback=self.window.create_tables,
            button_id="create_tables_btn",
        )

        create_database_section(
            layout=layout,
            test_btn=self.window.test_conn_btn,
            create_tables_btn=self.window.create_tables_btn,
        )

    def _create_test_section(self, layout: QVBoxLayout) -> None:
        """Create testing utilities section."""
        self.window.test_csv_btn = self._create_button(
            text="Test CSV Access",
            callback=self.window.test_csv_access,
            button_id="test_csv_btn",
        )
        self.window.test_api_export_btn = self._create_button(
            text="Test API Export",
            callback=self.window.test_api_export,
            button_id="test_api_export_btn",
        )

        create_test_section(
            layout=layout,
            test_csv_btn=self.window.test_csv_btn,
            test_api_export_btn=self.window.test_api_export_btn,
        )

    # -----------------------------
    # Button Factory
    # -----------------------------
    def _create_button(
        self,
        text: str,
        callback: Callable[[], None],
        button_id: str,
    ) -> QPushButton:
        """Create a button and register it for state management."""
        btn = QPushButton(text)
        btn.setObjectName(button_id)
        btn.clicked.connect(callback)

        # Register for bulk enable/disable during long operations
        self.window.operation_buttons[button_id] = btn
        return btn