"""
UI component creation and management
"""

from PySide6.QtWidgets import (
    QGroupBox, QHBoxLayout, QVBoxLayout, QGridLayout,
    QLabel, QLineEdit, QPushButton, QTextEdit,
    QProgressBar, QWidget, QSizePolicy
)


from PySide6.QtCore import Qt
from PySide6.QtGui import QFont


# ---------------------------------------------------------
# Shared style constants
# ---------------------------------------------------------
TITLE_FONT = QFont("Arial", 18, QFont.Bold)
OUTPUT_LABEL_FONT = QFont("Arial", 10, QFont.Bold)
OUTPUT_TEXT_FONT = QFont("Consolas", 9)

FILE_LABEL_MIN_HEIGHT = 30
FILE_LABEL_MAX_HEIGHT = 80
FILE_GROUP_MIN_HEIGHT = 200
FILE_GROUP_MAX_HEIGHT = 230


# ---------------------------------------------------------
# Title Section
# ---------------------------------------------------------
def create_title_section(layout: QVBoxLayout):
    """Create title section."""
    title_label = QLabel("ETL Pipeline Manager - FULLY OPERATIONAL")
    title_label.setObjectName("title_label")
    title_label.setFont(TITLE_FONT)
    title_label.setAlignment(Qt.AlignCenter)
    layout.addWidget(title_label)


# ---------------------------------------------------------
# API Section
# ---------------------------------------------------------
def create_api_section(layout: QVBoxLayout, api_url_input: QLineEdit, load_api_btn: QPushButton):
    """Create API configuration section."""
    api_group = QGroupBox("API Configuration")
    api_layout = QHBoxLayout(api_group)

    api_url_input.setPlaceholderText(
        "Enter API URL (e.g., https://etl-server.fly.dev or https://jsonplaceholder.typicode.com)"
    )
    api_url_input.setObjectName("api_url_input")

    load_api_btn.setObjectName("load_api_btn")

    api_layout.addWidget(QLabel("API URL:"))
    api_layout.addWidget(api_url_input, 1)
    api_layout.addWidget(load_api_btn)

    api_layout.setContentsMargins(10, 10, 10, 10)
    api_layout.setSpacing(10)

    layout.addWidget(api_group)


# ---------------------------------------------------------
# File Section
# ---------------------------------------------------------
def create_file_section(
    layout: QVBoxLayout,
    select_csv_btn: QPushButton,
    load_selected_files_btn: QPushButton,
    selected_files_label: QLabel
):
    """Create file management section."""
    file_group = QGroupBox("File Management")
    file_group.setMinimumHeight(FILE_GROUP_MIN_HEIGHT)
    file_group.setMaximumHeight(FILE_GROUP_MAX_HEIGHT)

    file_layout = QGridLayout(file_group)
    file_layout.setVerticalSpacing(10)
    file_layout.setHorizontalSpacing(10)

    load_selected_files_btn.setObjectName("load_selected_files_btn")
    load_selected_files_btn.setEnabled(False)

    selected_files_label.setObjectName("selected_files_label")
    selected_files_label.setText("No files selected")
    selected_files_label.setWordWrap(True)
    selected_files_label.setMinimumHeight(FILE_LABEL_MIN_HEIGHT)
    selected_files_label.setMaximumHeight(FILE_LABEL_MAX_HEIGHT)
    selected_files_label.setAlignment(Qt.AlignLeft | Qt.AlignTop)

    file_layout.addWidget(select_csv_btn, 0, 0)
    file_layout.addWidget(load_selected_files_btn, 0, 1)
    file_layout.addWidget(selected_files_label, 1, 0, 1, 2)

    # Row heights
    file_layout.setRowMinimumHeight(0, 50)
    file_layout.setRowMinimumHeight(1, FILE_LABEL_MAX_HEIGHT)

    layout.addWidget(file_group)


# ---------------------------------------------------------
# Data Section
# ---------------------------------------------------------
def create_data_section(layout: QVBoxLayout, load_csv_btn: QPushButton, load_api_data_btn: QPushButton):
    """Create data loading section."""
    data_group = QGroupBox("Data Loading")
    data_layout = QGridLayout(data_group)

    data_layout.addWidget(load_csv_btn, 0, 0)
    data_layout.addWidget(load_api_data_btn, 0, 1)

    layout.addWidget(data_group)


# ---------------------------------------------------------
# Database Section
# ---------------------------------------------------------
def create_database_section(layout: QVBoxLayout, test_conn_btn: QPushButton, create_tables_btn: QPushButton):
    """Create database operations section."""
    db_group = QGroupBox("Database Operations")
    db_layout = QGridLayout(db_group)

    db_layout.addWidget(test_conn_btn, 0, 0)
    db_layout.addWidget(create_tables_btn, 0, 1)

    layout.addWidget(db_group)


# ---------------------------------------------------------
# Test Section
# ---------------------------------------------------------
def create_test_section(layout: QVBoxLayout, test_csv_btn: QPushButton, test_api_export_btn: QPushButton):
    """Create test operations section."""
    test_group = QGroupBox("Test Operations")
    test_layout = QGridLayout(test_group)

    test_layout.addWidget(test_csv_btn, 0, 0)
    test_layout.addWidget(test_api_export_btn, 0, 1)

    layout.addWidget(test_group)


# ---------------------------------------------------------
# Theme Section
# ---------------------------------------------------------
def create_theme_section(layout: QVBoxLayout, theme_toggle_btn: QPushButton):
    """Create theme toggle section."""
    theme_group = QGroupBox("Theme Settings")
    theme_layout = QHBoxLayout(theme_group)

    theme_toggle_btn.setObjectName("theme_toggle_btn")

    theme_layout.addWidget(theme_toggle_btn)
    theme_layout.addStretch()

    layout.addWidget(theme_group)


# ---------------------------------------------------------
# Progress Bar
# ---------------------------------------------------------
def create_progress_bar() -> QProgressBar:
    """Create styled progress bar."""
    progress_bar = QProgressBar()
    progress_bar.setObjectName("progress_bar")
    progress_bar.setVisible(False)
    return progress_bar


# ---------------------------------------------------------
# Output Section
# ---------------------------------------------------------
def create_output_section() -> tuple[QWidget, QTextEdit]:
    """Create the output section containing a label and a read-only text area."""

    output_widget = QWidget()
    layout = QVBoxLayout(output_widget)  # concise + clear

    output_label = QLabel("Output:")
    output_label.setObjectName("output_label")
    output_label.setFont(OUTPUT_LABEL_FONT)
    layout.addWidget(output_label)

    output_text = QTextEdit()
    output_text.setObjectName("output_text")
    output_text.setReadOnly(True)
    output_text.setFont(OUTPUT_TEXT_FONT)

    output_text.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
    output_text.setMinimumHeight(200)

    layout.addWidget(output_text)

    return output_widget, output_text