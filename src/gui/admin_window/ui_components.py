"""UI component creation and management"""

from PySide6.QtWidgets import (QGroupBox, QHBoxLayout, QVBoxLayout, QGridLayout, 
                               QLabel, QLineEdit, QPushButton, QTextEdit, QProgressBar, QWidget)
from PySide6.QtCore import Qt
from PySide6.QtGui import QFont


def create_title_section(layout: QVBoxLayout):
    """Create title section"""
    title_label = QLabel("ETL Pipeline Manager - FULLY OPERATIONAL")
    title_label.setObjectName("title_label")
    title_font = QFont()
    title_font.setPointSize(18)
    title_font.setBold(True)
    title_label.setFont(title_font)
    title_label.setAlignment(Qt.AlignCenter)
    layout.addWidget(title_label)


def create_api_section(layout: QVBoxLayout, api_url_input: QLineEdit, load_api_btn: QPushButton):
    """Create API configuration section"""
    api_group = QGroupBox("API Configuration")
    api_layout = QHBoxLayout(api_group)
    
    api_url_input.setPlaceholderText("Enter API URL (e.g., https://etl-server.fly.dev or https://jsonplaceholder.typicode.com)")
    api_url_input.setObjectName("api_url_input")
    
    load_api_btn.setObjectName("load_api_btn")
    
    api_layout.addWidget(QLabel("API URL:"))
    api_layout.addWidget(api_url_input, 1)
    api_layout.addWidget(load_api_btn)
    api_layout.setContentsMargins(10, 10, 10, 10)
    api_layout.setSpacing(10)
    
    layout.addWidget(api_group)


def create_file_section(layout: QVBoxLayout, select_csv_btn: QPushButton, 
                       load_selected_files_btn: QPushButton, selected_files_label: QLabel):
    """Create file management section"""
    file_group = QGroupBox("File Management")
    file_group.setMinimumHeight(200)  # Reduced height
    file_group.setMaximumHeight(230)  # Reduced height
    file_layout = QGridLayout(file_group)
    file_layout.setVerticalSpacing(10)  # Reduced spacing
    file_layout.setHorizontalSpacing(10)
    
    load_selected_files_btn.setObjectName("load_selected_files_btn")
    load_selected_files_btn.setEnabled(False)
    
    selected_files_label.setObjectName("selected_files_label")
    selected_files_label.setText("No files selected")
    selected_files_label.setWordWrap(True)
    selected_files_label.setMinimumHeight(30)  # Reduced height
    selected_files_label.setMaximumHeight(80)  # Reduced height
    selected_files_label.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
    
    file_layout.addWidget(select_csv_btn, 0, 0)
    file_layout.addWidget(load_selected_files_btn, 0, 1)
    file_layout.addWidget(selected_files_label, 1, 0, 1, 2)
    
    # Set row heights
    file_layout.setRowMinimumHeight(0, 50)  # Button row height
    file_layout.setRowMinimumHeight(1, 80)  # Label row height - reduced
    
    layout.addWidget(file_group)


def create_data_section(layout: QVBoxLayout, load_csv_btn: QPushButton, 
                       load_api_data_btn: QPushButton):
    """Create data loading section"""
    data_group = QGroupBox("Data Loading")
    data_layout = QGridLayout(data_group)
    
    data_layout.addWidget(load_csv_btn, 0, 0)
    data_layout.addWidget(load_api_data_btn, 0, 1)
    
    layout.addWidget(data_group)


def create_database_section(layout: QVBoxLayout, test_conn_btn: QPushButton, 
                           create_tables_btn: QPushButton):
    """Create database operations section"""
    db_group = QGroupBox("Database Operations")
    db_layout = QGridLayout(db_group)
    
    db_layout.addWidget(test_conn_btn, 0, 0)
    db_layout.addWidget(create_tables_btn, 0, 1)
    
    layout.addWidget(db_group)


def create_test_section(layout: QVBoxLayout, test_csv_btn: QPushButton, 
                       test_api_export_btn: QPushButton):
    """Create test operations section"""
    test_group = QGroupBox("Test Operations")
    test_layout = QGridLayout(test_group)
    
    test_layout.addWidget(test_csv_btn, 0, 0)
    test_layout.addWidget(test_api_export_btn, 0, 1)
    
    layout.addWidget(test_group)


def create_theme_section(layout: QVBoxLayout, theme_toggle_btn: QPushButton):
    """Create theme toggle section"""
    theme_group = QGroupBox("Theme Settings")
    theme_layout = QHBoxLayout(theme_group)
    
    theme_toggle_btn.setObjectName("theme_toggle_btn")
    
    theme_layout.addWidget(theme_toggle_btn)
    theme_layout.addStretch()
    
    layout.addWidget(theme_group)


def create_progress_bar() -> QProgressBar:
    """Create styled progress bar"""
    progress_bar = QProgressBar()
    progress_bar.setObjectName("progress_bar")
    progress_bar.setVisible(False)
    return progress_bar


def create_output_section() -> tuple:
    """Create output section"""
    output_widget = QWidget()
    output_layout = QVBoxLayout(output_widget)
    
    output_label = QLabel("Output:")
    output_label.setObjectName("output_label")
    output_label.setFont(QFont("Arial", 10, QFont.Bold))
    output_layout.addWidget(output_label)
    
    output_text = QTextEdit()
    output_text.setObjectName("output_text")
    output_text.setReadOnly(True)
    output_text.setFont(QFont("Consolas", 9))
    output_layout.addWidget(output_text)
    
    return output_widget, output_text
