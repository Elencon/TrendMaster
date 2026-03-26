"""
Dark Theme Implementation - Modern dark theme with cyan accents
"""

from .base_theme import BaseTheme


class DarkTheme(BaseTheme):
    """Dark theme with cyan accents and modern styling"""

    def __init__(self):
        super().__init__("Dark Theme")

    def get_qt_material_theme(self) -> str:
        return 'dark_cyan.xml'

    def get_button_text(self) -> str:
        return "Toggle Light Theme"

    def _get_component_styles(self) -> str:
        """Shared component styles used by both custom and fallback"""
        return """
            /* Login Window */
            QMainWindow#login_window { background-color: #2b2b2b; }
            QWidget#login_form { color: #ffffff; }
            QLabel[objectName="login_title"] { color: #42a5f5; font-size: 18pt; font-weight: bold; margin-bottom: 5px; }
            QLabel[objectName="login_subtitle"] { color: #bdbdbd; font-size: 11pt; margin-bottom: 10px; }
            QLabel[objectName="field_label"] { font-weight: bold; color: #e0e0e0; margin-bottom: 2px; }
            QLabel[objectName="status_label"] { font-size: 10pt; margin-top: 10px; }
            QLineEdit[objectName="username_input"], QLineEdit[objectName="password_input"] {
                border: 2px solid #555; border-radius: 4px; padding: 8px; font-size: 11pt;
                background-color: #3c3c3c; color: #ffffff; min-height: 31px;
                selection-background-color: #42a5f5; selection-color: white;
            }
            QLineEdit[objectName="username_input"]:focus, QLineEdit[objectName="password_input"]:focus { border: 2px solid #42a5f5; background-color: #1a1a1a; }
            QPushButton[objectName="password_toggle_btn"], QPushButton[objectName="confirm_toggle_btn"] { color: #aaaaaa; background-color: transparent; border: none; font-size: 16pt; padding: 0px; font-family: "Segoe UI Symbol", "Apple Color Emoji", sans-serif; }
            QPushButton[objectName="password_toggle_btn"]:hover, QPushButton[objectName="confirm_toggle_btn"]:hover { color: #aaaaaa; background-color: transparent; border: none; }
            QPushButton[objectName="password_toggle_btn"]:focus, QPushButton[objectName="confirm_toggle_btn"]:focus { color: #aaaaaa; background-color: transparent; border: none; outline: none; }
            QPushButton[objectName="password_toggle_btn"]:pressed, QPushButton[objectName="confirm_toggle_btn"]:pressed { color: #aaaaaa; background-color: transparent; border: none; }
            QPushButton[objectName="login_btn"] { background-color: #42a5f5; color: white; border: none; border-radius: 4px; font-size: 12pt; font-weight: bold; padding: 10px; min-height: 40px; }
            QPushButton[objectName="login_btn"]:hover { background-color: #1e88e5; }
            QPushButton[objectName="login_btn"]:pressed { background-color: #1565c0; }
            QPushButton[objectName="login_btn"]:disabled { background-color: #505050; color: #999999; }
            
            /* Dashboard */
            QLabel[objectName="title_label"], QLabel[objectName="dashboard_title_label"] { color: #0d6efd; font-weight: bold; border-radius: 8px; background-color: rgba(13, 110, 253, 0.2); border: 2px solid #0d6efd; }
            QLabel[objectName="title_label"] { font-size: 16px; margin: 10px 0; padding: 10px 15px; }
            QLabel[objectName="dashboard_title_label"] { font-size: 20px; margin: 15px 0; padding: 15px 20px; }
            QLabel[objectName="selected_files_label"] { color: #999; font-style: italic; padding: 8px 12px; font-size: 10px; background-color: #3c3c3c; border: 1px solid #555; border-radius: 6px; }
            QLabel[objectName="user_info_label"] { color: #e0e0e0; font-size: 11pt; font-weight: 600; padding: 5px 10px; }
            QPushButton { height: 50px; min-height: 50px; }
            QPushButton[objectName="load_api_btn"] { min-width: 80px; }
            QPushButton[objectName="select_csv_btn"] { min-width: 140px; }
            QPushButton[objectName="theme_toggle_btn"] { background-color: #404040; color: #fff; border: 1px solid #555; border-radius: 4px; padding: 6px 12px; font-weight: 600; font-size: 10px; }
            QPushButton[objectName="theme_toggle_btn"]:hover { background-color: #505050; }
            QPushButton[objectName="theme_toggle_btn"]:pressed { background-color: #2b2b2b; }
            QPushButton[objectName="load_selected_files_btn"], QPushButton[objectName="manage_db_btn"], QPushButton[objectName="manage_users_btn"] { background-color: #0d6efd; color: #fff; border: 2px solid #0b5ed7; border-radius: 6px; font-weight: 600; font-size: 10px; }
            QPushButton[objectName="load_selected_files_btn"] { padding: 4px 12px; }
            QPushButton[objectName="manage_db_btn"], QPushButton[objectName="manage_users_btn"] { padding: 12px 24px; font-weight: 700; font-size: 12px; min-width: 180px; }
            QPushButton[objectName="load_selected_files_btn"]:disabled, QPushButton[objectName="manage_db_btn"]:disabled, QPushButton[objectName="manage_users_btn"]:disabled { background-color: #505050; color: #999; border: 2px solid #666; }
            QPushButton[objectName="load_selected_files_btn"]:hover, QPushButton[objectName="manage_db_btn"]:hover, QPushButton[objectName="manage_users_btn"]:hover { background-color: #0b5ed7; border: 2px solid #0a58ca; }
            QPushButton[objectName="load_selected_files_btn"]:pressed, QPushButton[objectName="manage_db_btn"]:pressed, QPushButton[objectName="manage_users_btn"]:pressed { background-color: #0a58ca; border: 2px solid #084298; }
            QLineEdit[objectName="api_url_input"] { border: 2px solid #555; border-radius: 6px; padding: 10px 15px; background-color: #2b2b2b; color: #fff; font-size: 11px; }
            QLineEdit[objectName="api_url_input"]:focus { border-color: #0d6efd; background-color: #1a1a1a; }
            QProgressBar[objectName="progress_bar"] { border: 2px solid #555; border-radius: 8px; background-color: #3c3c3c; height: 25px; }
            QProgressBar[objectName="progress_bar"]::chunk { background: qlineargradient(x1:0, y1:0, x2:1, y2:0, stop:0 #0d6efd, stop:1 #0b5ed7); border-radius: 6px; }
            QTextEdit[objectName="output_text"] { background-color: #2b2b2b; border: 2px solid #555; border-radius: 8px; padding: 15px; color: #fff; font-size: 9px; font-family: 'Consolas', 'Monaco', monospace; selection-background-color: #0d6efd; selection-color: #fff; }
            QTextEdit[objectName="output_text"]:focus { border-color: #0d6efd; }
            QGroupBox { background-color: #3c3c3c; border: 1px solid #555; border-radius: 8px; margin-top: 8px; padding-top: 25px; color: #fff; font-weight: bold; }
            QGroupBox::title { color: #fff; subcontrol-origin: padding; left: 10px; top: 8px; subcontrol-position: top left; padding: 8px 12px; }
            QListWidget[objectName="tables_list"] { background-color: #2b2b2b; border: 2px solid #555; border-radius: 8px; padding: 10px; color: #fff; font-size: 11px; }
            QListWidget[objectName="tables_list"]::item { padding: 8px; border-radius: 4px; margin: 2px 0; }
            QListWidget[objectName="tables_list"]::item:hover { background-color: #3c3c3c; }
            QListWidget[objectName="tables_list"]::item:selected { background-color: #0d6efd; color: #fff; }
            
            /* User Management */
            QTabWidget#user_mgmt_tabs { background-color: #2b2b2b; }
            QTabWidget#user_mgmt_tabs QTabBar::tab { background-color: #3c3c3c; color: #e0e0e0; padding: 10px 20px; border: 1px solid #555; border-bottom: none; border-radius: 4px 4px 0 0; font-weight: 600; margin-right: 2px; }
            QTabWidget#user_mgmt_tabs QTabBar::tab:selected { background-color: #2b2b2b; color: #42a5f5; border-bottom: 2px solid #42a5f5; }
            QTabWidget#user_mgmt_tabs QTabBar::tab:hover:!selected { background-color: #505050; }
            QLabel[objectName="section_title"] { font-size: 18pt; font-weight: bold; color: #42a5f5; padding: 15px; margin-bottom: 10px; }
            QLabel[objectName="role_description"] { color: #bdbdbd; font-size: 11pt; padding: 5px; font-weight: normal; }
            QGroupBox[objectName="form_group"], QGroupBox[objectName="role_desc_group"] { background-color: #3c3c3c; border: 2px solid #555; border-radius: 8px; margin-top: 15px; padding: 20px; font-weight: bold; color: #e0e0e0; }
            QLineEdit[objectName="username_input"], QLineEdit[objectName="password_input"], QLineEdit[objectName="confirm_password_input"], QLineEdit[objectName="staff_id_input"] { border: 2px solid #555; border-radius: 4px; padding: 10px; background-color: #1a1a1a; color: #ffffff; font-size: 11pt; min-height: 35px; }
            QLineEdit[objectName="username_input"]:focus, QLineEdit[objectName="password_input"]:focus, QLineEdit[objectName="confirm_password_input"]:focus, QLineEdit[objectName="staff_id_input"]:focus { border: 2px solid #42a5f5; }
            QComboBox[objectName="role_combo"] { border: 2px solid #555; border-radius: 4px; padding: 8px; background-color: #1a1a1a; color: #ffffff; font-size: 11pt; min-height: 35px; }
            QComboBox[objectName="role_combo"]:focus { border: 2px solid #42a5f5; }
            QComboBox[objectName="role_combo"]::drop-down { border: none; width: 30px; }
            QComboBox[objectName="role_combo"]::down-arrow { image: none; border-left: 5px solid transparent; border-right: 5px solid transparent; border-top: 7px solid #e0e0e0; margin-right: 10px; }
            QPushButton[objectName="create_user_btn"] { background-color: #28a745; color: white; border: 2px solid #218838; border-radius: 6px; padding: 12px 30px; font-size: 12pt; font-weight: bold; min-width: 150px; }
            QPushButton[objectName="create_user_btn"]:hover { background-color: #218838; }
            QPushButton[objectName="create_user_btn"]:pressed { background-color: #1e7e34; }
            QTableWidget[objectName="users_table"] { background-color: #2b2b2b; border: 2px solid #555; border-radius: 8px; gridline-color: #3c3c3c; color: #ffffff; font-size: 10pt; }
            QTableWidget[objectName="users_table"]::item { padding: 8px; }
            QTableWidget[objectName="users_table"]::item:selected { background-color: #42a5f5; color: white; }
            QTableWidget[objectName="users_table"] QHeaderView::section { background-color: #3c3c3c; color: #e0e0e0; padding: 10px; border: 1px solid #555; font-weight: bold; }
            QPushButton[objectName="refresh_btn"], QPushButton[objectName="change_role_btn"], QPushButton[objectName="deactivate_btn"], QPushButton[objectName="activate_btn"], QPushButton[objectName="close_btn"] { border: 2px solid #555; border-radius: 4px; padding: 10px 20px; font-size: 11pt; font-weight: 600; min-width: 120px; }
            QPushButton[objectName="refresh_btn"] { background-color: #17a2b8; color: white; border-color: #138496; }
            QPushButton[objectName="refresh_btn"]:hover { background-color: #138496; }
            QPushButton[objectName="change_role_btn"] { background-color: #ffc107; color: #212529; border-color: #e0a800; }
            QPushButton[objectName="change_role_btn"]:hover { background-color: #e0a800; }
            QPushButton[objectName="deactivate_btn"], QPushButton[objectName="logout_btn"] { background-color: #dc3545; color: white; border-color: #c82333; }
            QPushButton[objectName="deactivate_btn"]:hover, QPushButton[objectName="logout_btn"]:hover { background-color: #c82333; }
            QPushButton[objectName="logout_btn"]:pressed { background-color: #bd2130; }
            QPushButton[objectName="activate_btn"] { background-color: #28a745; color: white; border-color: #218838; }
            QPushButton[objectName="activate_btn"]:hover { background-color: #218838; }
            QPushButton[objectName="close_btn"] { background-color: #6c757d; color: white; }
            QPushButton[objectName="close_btn"]:hover { background-color: #5a6268; }
            
            /* Tabbed Interface */
            QTabWidget { background-color: #2b2b2b; border: none; }
            QTabWidget::pane { border: 2px solid #555; border-radius: 4px; background-color: #2b2b2b; top: -1px; }
            QTabWidget::tab-bar { alignment: left; }
            QTabBar::tab { background-color: #3c3c3c; color: #e0e0e0; padding: 10px 20px; border: 2px solid #555; border-bottom: none; border-top-left-radius: 6px; border-top-right-radius: 6px; margin-right: 4px; font-weight: 600; font-size: 11pt; min-width: 120px; }
            QTabBar::tab:selected { background-color: #2b2b2b; color: #42a5f5; border-bottom: 2px solid #2b2b2b; margin-bottom: -2px; }
            QTabBar::tab:hover:!selected { background-color: #505050; }
            QPushButton[objectName="tab_close_btn"] { background: none; border: none; color: #666; font-size: 16px; font-weight: bold; padding: 0px; margin: 0px; }
            QPushButton[objectName="tab_close_btn"]:hover { background: none; color: #fff; }
            QPushButton[objectName="tab_close_btn"]:pressed { background: none; color: #fff; }
            
            /* Toolbar & Menus */
            QToolBar { background-color: #3c3c3c; border: none; padding: 4px; spacing: 3px; }
            QToolBar QToolButton, QToolBar QPushButton { background-color: transparent; color: #e0e0e0; border: none; padding: 6px 12px; font-weight: 600; }
            QToolBar QToolButton:hover, QToolBar QPushButton:hover { background-color: transparent; color: #e0e0e0; border: none; }
            QToolBar QToolButton:focus, QToolBar QPushButton:focus { background-color: transparent; color: #e0e0e0; border: none; outline: none; }
            QToolBar QToolButton:pressed, QToolBar QPushButton:pressed { background-color: transparent; color: #a0a0a0; border: none; }
            QMenu { background-color: #3c3c3c; border: 1px solid #555; color: #e0e0e0; }
            QMenu::item { padding: 8px 25px; }
            QMenu::item:selected { background-color: #42a5f5; color: #fff; }
            QMenu::separator { height: 1px; background-color: #555; margin: 4px 0; }
        """

    def get_custom_styles(self) -> str:
        return self._get_component_styles()

    def get_fallback_styles(self) -> str:
        return f"""
            QMainWindow {{ background-color: #2b2b2b; color: #fff; }}
            {self._get_component_styles()}
        """
