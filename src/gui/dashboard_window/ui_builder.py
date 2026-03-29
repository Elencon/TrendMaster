"""UI Builder for Dashboard Window - Extracted UI creation methods"""

import logging

from PySide6.QtWidgets import (QVBoxLayout, QHBoxLayout, QPushButton, QListWidget,
                               QComboBox, QLabel, QGroupBox, QTableWidget,
                               QWidget, QSizePolicy, QToolBar)
from PySide6.QtGui import QFont, QAction

from auth.session import SessionManager  # type: ignore
from .ui_components import create_tables_section
from .gauge_widget import SalesGaugeWidget

logger = logging.getLogger(__name__)


class DashboardUIBuilder:
    """Handles all UI construction for the dashboard window"""

    def __init__(self, window):
        """
        Initialize the UI builder.

        Args:
            window: Reference to the parent DashboardMainWindow instance
        """
        self.window = window

    def create_all_sections(self, layout: QVBoxLayout):
        """
        Create all UI sections in the dashboard.

        Args:
            layout: Main vertical layout to add sections to
        """
        self._create_database_management_section(layout)
        self._create_employee_section_if_authorized(layout)
        self._create_content_section(layout)
        self._create_logout_section(layout)

    def _create_database_management_section(self, layout: QVBoxLayout):
        """Create the database management section with buttons and customer report"""
        manage_group = QGroupBox("Database Management")
        manage_layout = QHBoxLayout(manage_group)

        # Check user role once for conditional UI
        session = SessionManager()
        user_role = session.get_role()

        # Manage Database button (omit entirely for Employee role)
        if user_role != "Employee":
            self.window.manage_db_btn = QPushButton("Manage Database")
            self.window.manage_db_btn.setObjectName("manage_db_btn")
            self.window.manage_db_btn.clicked.connect(self.window.open_admin_window)
            manage_layout.addWidget(self.window.manage_db_btn)

        # Manage Users button (shown only for Administrators)
        self.window.manage_users_btn = QPushButton("Manage Users")
        self.window.manage_users_btn.setObjectName("manage_users_btn")
        self.window.manage_users_btn.clicked.connect(self.window.open_user_management)
        # Add and then hide if not admin to keep minimal change footprint
        manage_layout.addWidget(self.window.manage_users_btn)
        if user_role != "Administrator":
            self.window.manage_users_btn.hide()

        # Add spacer to push customer report to the right
        manage_layout.addStretch()

        # Customer Report Section (inline on the right)
        report_label = QLabel("Customer Report:")
        manage_layout.addWidget(report_label)

        # Customer dropdown with search
        self.window.customer_combo = QComboBox()
        self.window.customer_combo.setEditable(True)  # Enable search/filter
        self.window.customer_combo.setPlaceholderText("Search customer...")
        self.window.customer_combo.setMinimumWidth(250)
        manage_layout.addWidget(self.window.customer_combo)

        # Generate PDF button
        self.window.generate_pdf_btn = QPushButton("Generate PDF")
        self.window.generate_pdf_btn.setObjectName("generate_pdf_btn")
        self.window.generate_pdf_btn.clicked.connect(self.window.generate_customer_pdf)
        manage_layout.addWidget(self.window.generate_pdf_btn)

        layout.addWidget(manage_group)

    def _create_employee_section_if_authorized(self, layout: QVBoxLayout):
        """Create employee list section (Manager/Admin only)"""
        session = SessionManager()
        user_role = session.get_role()

        if user_role not in ["Manager", "Administrator"]:
            return

        from PySide6.QtWidgets import QHeaderView

        employee_group = QGroupBox("Employee Users")
        employee_layout = QVBoxLayout(employee_group)

        # Create table widget
        self.window.employee_table = QTableWidget()
        self.window.employee_table.setColumnCount(2)
        self.window.employee_table.setHorizontalHeaderLabels(['Name', 'Email'])

        # Configure table
        self.window.employee_table.setAlternatingRowColors(True)
        self.window.employee_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.window.employee_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.window.employee_table.horizontalHeader().setStretchLastSection(True)
        self.window.employee_table.setMaximumHeight(200)

        # Auto-resize columns to content
        header = self.window.employee_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)

        employee_layout.addWidget(self.window.employee_table)

        layout.addWidget(employee_group)

    def _create_content_section(self, layout: QVBoxLayout):
        """Create the main content section with tables list and sales gauge"""
        # Create horizontal layout for gauge and tables side by side
        content_layout = QHBoxLayout()

        # Left side: Tables list (takes up more space)
        tables_container = QVBoxLayout()
        self.window.tables_list = QListWidget()
        create_tables_section(tables_container, self.window.tables_list)

        # Right side: Sales gauge chart (fixed width)
        gauge_container = QVBoxLayout()
        self.window.sales_gauge = SalesGaugeWidget()
        self.window.sales_gauge.setMinimumSize(350, 350)
        self.window.sales_gauge.setMaximumWidth(400)
        gauge_container.addWidget(self.window.sales_gauge)
        gauge_container.addStretch()  # Push gauge to top

        # Add both sides to horizontal layout
        # Tables take 60% of width, gauge takes 40%
        content_layout.addLayout(tables_container, 60)
        content_layout.addLayout(gauge_container, 40)

        # Add the horizontal layout to main layout
        layout.addLayout(content_layout)

    def _create_logout_section(self, layout: QVBoxLayout):
        """Create logout button section with user info"""
        logout_group = QGroupBox("Session")
        logout_layout = QHBoxLayout(logout_group)

        # Show current user info
        session = SessionManager()
        username = session.get_username()
        role = session.get_role()

        user_info_label = QLabel(f"Logged in as: {username} ({role})")
        user_info_label.setObjectName("user_info_label")

        # Create logout button
        self.window.logout_btn = QPushButton("Logout")
        self.window.logout_btn.setObjectName("logout_btn")
        self.window.logout_btn.clicked.connect(self.window.logout)

        logout_layout.addWidget(user_info_label)
        logout_layout.addStretch()
        logout_layout.addWidget(self.window.logout_btn)

        layout.addWidget(logout_group)

    def create_toolbar(self) -> QToolBar:
        """
        Create toolbar with title on left, settings on right.

        Returns:
            QToolBar: The created toolbar
        """
        toolbar = self.window.addToolBar("Main Toolbar")
        toolbar.setMovable(False)

        # Title label on the left
        title_label = QLabel("ETL Pipeline Dashboard")
        title_label.setObjectName("toolbar_title_label")
        title_font = QFont()
        title_font.setPointSize(14)
        title_font.setBold(True)
        title_label.setFont(title_font)
        title_label.setStyleSheet("padding: 5px 10px;")
        toolbar.addWidget(title_label)

        # Add spacer to push settings to the right
        spacer = QWidget()
        spacer.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        toolbar.addWidget(spacer)

        # Settings action with gear icon only - no hover effects
        settings_action = QAction("⚙", self.window)
        settings_action.setObjectName("settings_action")
        settings_action.triggered.connect(self.window.show_settings_menu)
        toolbar.addAction(settings_action)

        return toolbar
