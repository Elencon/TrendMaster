"""UI component creation for dashboard"""

from PySide6.QtWidgets import (QGroupBox, QVBoxLayout, QHBoxLayout, 
                               QLabel, QPushButton, QListWidget, QWidget)
from PySide6.QtCore import Qt
from PySide6.QtGui import QFont
from typing import Union


def create_title_section(layout: QVBoxLayout):
    """Create title section"""
    title_label = QLabel("ETL Pipeline Dashboard")
    title_label.setObjectName("dashboard_title_label")
    title_font = QFont()
    title_font.setPointSize(20)
    title_font.setBold(True)
    title_label.setFont(title_font)
    title_label.setAlignment(Qt.AlignCenter)
    layout.addWidget(title_label)


def create_manage_section(layout: QVBoxLayout, content: Union[QPushButton, QHBoxLayout]):
    """Create database management button section
    
    Args:
        layout: Parent layout to add section to
        content: Either a single QPushButton or QHBoxLayout with multiple buttons
    """
    manage_group = QGroupBox("Database Management")
    manage_layout = QHBoxLayout(manage_group)
    
    if isinstance(content, QPushButton):
        # Single button (legacy)
        content.setObjectName("manage_db_btn")
        manage_layout.addWidget(content)
    elif isinstance(content, QHBoxLayout):
        # Multiple buttons layout
        # Transfer all widgets from content layout to manage_layout
        while content.count():
            item = content.takeAt(0)
            if item.widget():
                manage_layout.addWidget(item.widget())
            elif item.layout():
                manage_layout.addLayout(item.layout())
    
    manage_layout.addStretch()
    
    layout.addWidget(manage_group)


def create_tables_section(layout: QVBoxLayout, tables_list: QListWidget):
    """Create database tables list section"""
    tables_group = QGroupBox("Database Tables")
    tables_layout = QVBoxLayout(tables_group)
    
    tables_list.setObjectName("tables_list")
    tables_list.setAlternatingRowColors(True)
    
    tables_layout.addWidget(tables_list)
    
    layout.addWidget(tables_group)


def create_theme_section(layout: QVBoxLayout, theme_toggle_btn: QPushButton):
    """Create theme toggle section"""
    theme_group = QGroupBox("Theme Settings")
    theme_layout = QHBoxLayout(theme_group)
    
    theme_toggle_btn.setObjectName("theme_toggle_btn")
    
    theme_layout.addWidget(theme_toggle_btn)
    theme_layout.addStretch()
    
    layout.addWidget(theme_group)
