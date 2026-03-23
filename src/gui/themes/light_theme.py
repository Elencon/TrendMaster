"""
Light Theme Implementation - Clean, professional light theme
"""

from .base_theme import BaseTheme


class LightTheme(BaseTheme):
    """Light theme with white sections and professional styling"""
    
    def __init__(self):
        super().__init__("Light Theme")
    
    def get_qt_material_theme(self) -> str:
        return 'light_blue.xml'
    
    def get_button_text(self) -> str:
        return "Toggle Dark Theme"
    
    def _get_component_styles(self) -> str:
        """Shared component styles used by both custom and fallback"""
        return """
            /* Login Window */
            QMainWindow#login_window { background-color: #f5f5f5; }
            QWidget#login_form { color: #212121; }
            QLabel[objectName="login_title"] { color: #1976d2; font-size: 18pt; font-weight: bold; margin-bottom: 5px; }
            QLabel[objectName="login_subtitle"] { color: #757575; font-size: 11pt; margin-bottom: 10px; }
            QLabel[objectName="field_label"] { font-weight: bold; color: #424242; margin-bottom: 2px; }
            QLabel[objectName="status_label"] { font-size: 10pt; margin-top: 10px; }
            QLineEdit[objectName="username_input"], QLineEdit[objectName="password_input"] { border: 2px solid #e0e0e0; border-radius: 4px; padding: 8px; font-size: 11pt; background-color: white; color: #212121; min-height: 31px; selection-background-color: #1976d2; selection-color: white; }
            QLineEdit[objectName="username_input"]:focus, QLineEdit[objectName="password_input"]:focus { border: 2px solid #1976d2; background-color: white; }
            QPushButton[objectName="password_toggle_btn"], QPushButton[objectName="confirm_toggle_btn"] { color: #666666; background-color: transparent; border: none; font-size: 16pt; padding: 0px; font-family: "Segoe UI Symbol", "Apple Color Emoji", sans-serif; }
            QPushButton[objectName="password_toggle_btn"]:hover, QPushButton[objectName="confirm_toggle_btn"]:hover { color: #666666; background-color: transparent; border: none; }
            QPushButton[objectName="password_toggle_btn"]:focus, QPushButton[objectName="confirm_toggle_btn"]:focus { color: #666666; background-color: transparent; border: none; outline: none; }
            QPushButton[objectName="password_toggle_btn"]:pressed, QPushButton[objectName="confirm_toggle_btn"]:pressed { color: #666666; background-color: transparent; border: none; }
            QPushButton[objectName="login_btn"] { background-color: #1976d2; color: white; border: none; border-radius: 4px; font-size: 12pt; font-weight: bold; padding: 10px; min-height: 40px; }
            QPushButton[objectName="login_btn"]:hover { background-color: #1565c0; }
            QPushButton[objectName="login_btn"]:pressed { background-color: #0d47a1; }
            QPushButton[objectName="login_btn"]:disabled { background-color: #bdbdbd; color: #757575; }
            
            /* Dashboard */
            QLabel[objectName="title_label"], QLabel[objectName="dashboard_title_label"] { color: #0d6efd; font-weight: bold; border-radius: 8px; background-color: rgba(13, 110, 253, 0.1); border: 2px solid #90ee90; }
            QLabel[objectName="title_label"] { font-size: 16px; margin: 10px 0; padding: 10px 15px; }
            QLabel[objectName="dashboard_title_label"] { font-size: 20px; margin: 15px 0; padding: 15px 20px; }
            QLabel[objectName="selected_files_label"] { color: #6c757d; font-style: italic; padding: 8px 12px; font-size: 10px; background-color: #f8f9fa; border: 1px solid #e9ecef; border-radius: 6px; }
            QLabel[objectName="user_info_label"] { color: #495057; font-size: 11pt; font-weight: 600; padding: 5px 10px; }
            QPushButton { height: 50px; min-height: 50px; }
            QPushButton[objectName="load_api_btn"] { min-width: 80px; }
            QPushButton[objectName="select_csv_btn"] { min-width: 140px; }
            QPushButton[objectName="theme_toggle_btn"] { background-color: #6c757d; color: #fff; border: 1px solid #5a6268; border-radius: 4px; padding: 6px 12px; font-weight: 600; font-size: 10px; }
            QPushButton[objectName="theme_toggle_btn"]:hover { background-color: #5a6268; }
            QPushButton[objectName="theme_toggle_btn"]:pressed { background-color: #495057; }
            QPushButton[objectName="load_selected_files_btn"], QPushButton[objectName="manage_db_btn"], QPushButton[objectName="manage_users_btn"] { background-color: #0d6efd; color: #fff; border: 2px solid #0b5ed7; border-radius: 6px; font-weight: 600; font-size: 10px; }
            QPushButton[objectName="load_selected_files_btn"] { padding: 4px 12px; }
            QPushButton[objectName="manage_db_btn"], QPushButton[objectName="manage_users_btn"] { padding: 12px 24px; font-weight: 700; font-size: 12px; min-width: 180px; }
            QPushButton[objectName="load_selected_files_btn"]:disabled, QPushButton[objectName="manage_db_btn"]:disabled, QPushButton[objectName="manage_users_btn"]:disabled { background-color: #e9ecef; color: #6c757d; border: 2px solid #dee2e6; }
            QPushButton[objectName="load_selected_files_btn"]:hover, QPushButton[objectName="manage_db_btn"]:hover, QPushButton[objectName="manage_users_btn"]:hover { background-color: #0b5ed7; border: 2px solid #0a58ca; }
            QPushButton[objectName="load_selected_files_btn"]:pressed, QPushButton[objectName="manage_db_btn"]:pressed, QPushButton[objectName="manage_users_btn"]:pressed { background-color: #0a58ca; border: 2px solid #084298; }
            QLineEdit[objectName="api_url_input"] { border: 2px solid #0d6efd; border-radius: 6px; padding: 10px 15px; background-color: #fff; color: #495057; font-size: 11px; }
            QLineEdit[objectName="api_url_input"]:focus { border-color: #0b5ed7; background-color: #f8f9ff; }
            QProgressBar[objectName="progress_bar"] { border: 2px solid #dee2e6; border-radius: 8px; background-color: #f8f9fa; height: 25px; }
            QProgressBar[objectName="progress_bar"]::chunk { background: qlineargradient(x1:0, y1:0, x2:1, y2:0, stop:0 #0d6efd, stop:1 #0b5ed7); border-radius: 6px; }
            QTextEdit[objectName="output_text"] { background-color: #fff; border: 2px solid #dee2e6; border-radius: 8px; padding: 15px; color: #212529; font-size: 9px; font-family: 'Consolas', 'Monaco', monospace; selection-background-color: #0d6efd; selection-color: #fff; }
            QTextEdit[objectName="output_text"]:focus { border-color: #0d6efd; }
            QGroupBox { background-color: #fff; border: 1px solid #ccc; border-radius: 8px; margin-top: 8px; padding-top: 25px; font-weight: bold; }
            QGroupBox::title { color: #666; subcontrol-origin: padding; left: 10px; top: 8px; subcontrol-position: top left; padding: 8px 12px; }
            QListWidget[objectName="tables_list"] { background-color: #fff; border: 2px solid #dee2e6; border-radius: 8px; padding: 10px; color: #212529; font-size: 11px; }
            QListWidget[objectName="tables_list"]::item { padding: 8px; border-radius: 4px; margin: 2px 0; }
            QListWidget[objectName="tables_list"]::item:hover { background-color: #e9ecef; }
            QListWidget[objectName="tables_list"]::item:selected { background-color: #0d6efd; color: #fff; }
            
            /* User Management */
            QTabWidget#user_mgmt_tabs { background-color: white; }
            QTabWidget#user_mgmt_tabs QTabBar::tab { background-color: #e9ecef; color: #495057; padding: 10px 20px; border: 1px solid #dee2e6; border-bottom: none; border-radius: 4px 4px 0 0; font-weight: 600; margin-right: 2px; }
            QTabWidget#user_mgmt_tabs QTabBar::tab:selected { background-color: white; color: #0d6efd; border-bottom: 2px solid #0d6efd; }
            QTabWidget#user_mgmt_tabs QTabBar::tab:hover:!selected { background-color: #f8f9fa; }
            QLabel[objectName="section_title"] { font-size: 18pt; font-weight: bold; color: #0d6efd; padding: 15px; margin-bottom: 10px; }
            QLabel[objectName="role_description"] { color: #495057; font-size: 11pt; padding: 5px; font-weight: normal; }
            QGroupBox[objectName="form_group"], QGroupBox[objectName="role_desc_group"] { background-color: #f8f9fa; border: 2px solid #dee2e6; border-radius: 8px; margin-top: 15px; padding: 20px; font-weight: bold; color: #495057; }
            QLineEdit[objectName="username_input"], QLineEdit[objectName="password_input"], QLineEdit[objectName="confirm_password_input"], QLineEdit[objectName="staff_id_input"] { border: 2px solid #dee2e6; border-radius: 4px; padding: 10px; background-color: white; color: #212529; font-size: 11pt; min-height: 35px; }
            QLineEdit[objectName="username_input"]:focus, QLineEdit[objectName="password_input"]:focus, QLineEdit[objectName="confirm_password_input"]:focus, QLineEdit[objectName="staff_id_input"]:focus { border: 2px solid #0d6efd; }
            QComboBox[objectName="role_combo"] { border: 2px solid #dee2e6; border-radius: 4px; padding: 8px; background-color: white; color: #212529; font-size: 11pt; min-height: 35px; }
            QComboBox[objectName="role_combo"]:focus { border: 2px solid #0d6efd; }
            QComboBox[objectName="role_combo"]::drop-down { border: none; width: 30px; }
            QComboBox[objectName="role_combo"]::down-arrow { image: none; border-left: 5px solid transparent; border-right: 5px solid transparent; border-top: 7px solid #495057; margin-right: 10px; }
            QPushButton[objectName="create_user_btn"] { background-color: #28a745; color: white; border: 2px solid #218838; border-radius: 6px; padding: 12px 30px; font-size: 12pt; font-weight: bold; min-width: 150px; }
            QPushButton[objectName="create_user_btn"]:hover { background-color: #218838; }
            QPushButton[objectName="create_user_btn"]:pressed { background-color: #1e7e34; }
            QTableWidget[objectName="users_table"] { background-color: white; border: 2px solid #dee2e6; border-radius: 8px; gridline-color: #e9ecef; color: #212529; font-size: 10pt; }
            QTableWidget[objectName="users_table"]::item { padding: 8px; }
            QTableWidget[objectName="users_table"]::item:selected { background-color: #0d6efd; color: white; }
            QTableWidget[objectName="users_table"] QHeaderView::section { background-color: #f8f9fa; color: #495057; padding: 10px; border: 1px solid #dee2e6; font-weight: bold; }
            QPushButton[objectName="refresh_btn"], QPushButton[objectName="change_role_btn"], QPushButton[objectName="deactivate_btn"], QPushButton[objectName="activate_btn"], QPushButton[objectName="close_btn"] { border: 2px solid #6c757d; border-radius: 4px; padding: 10px 20px; font-size: 11pt; font-weight: 600; min-width: 120px; }
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
            QTabWidget { background-color: white; border: none; }
            QTabWidget::pane { border: 2px solid #dee2e6; border-radius: 4px; background-color: white; top: -1px; }
            QTabWidget::tab-bar { alignment: left; }
            QTabBar::tab { background-color: #e9ecef; color: #495057; padding: 10px 20px; border: 2px solid #dee2e6; border-bottom: none; border-top-left-radius: 6px; border-top-right-radius: 6px; margin-right: 4px; font-weight: 600; font-size: 11pt; min-width: 120px; }
            QTabBar::tab:selected { background-color: white; color: #0d6efd; border-bottom: 2px solid white; margin-bottom: -2px; }
            QTabBar::tab:hover:!selected { background-color: #f8f9fa; }
            QPushButton[objectName="tab_close_btn"] { background: none; border: none; color: #999; font-size: 16px; font-weight: bold; padding: 0px; margin: 0px; }
            QPushButton[objectName="tab_close_btn"]:hover { background: none; color: #212529; }
            QPushButton[objectName="tab_close_btn"]:pressed { background: none; color: #212529; }
            
            /* Toolbar & Menus */
            QToolBar { background-color: #f8f9fa; border-bottom: 1px solid #dee2e6; padding: 4px; spacing: 3px; }
            QToolBar QToolButton, QToolBar QPushButton { background-color: transparent; color: #495057; border: none; padding: 6px 12px; font-weight: 600; }
            QToolBar QToolButton:hover, QToolBar QPushButton:hover { background-color: transparent; color: #495057; border: none; }
            QToolBar QToolButton:focus, QToolBar QPushButton:focus { background-color: transparent; color: #495057; border: none; outline: none; }
            QToolBar QToolButton:pressed, QToolBar QPushButton:pressed { background-color: transparent; color: #2c3338; border: none; }
            QMenu { background-color: white; border: 1px solid #dee2e6; color: #212529; }
            QMenu::item { padding: 8px 25px; }
            QMenu::item:selected { background-color: #0d6efd; color: white; }
            QMenu::separator { height: 1px; background-color: #dee2e6; margin: 4px 0; }
        """
    
    def get_custom_styles(self) -> str:
        return self._get_component_styles()
    
    def get_fallback_styles(self) -> str:
        return f"""
            QMainWindow {{ background-color: #fff; color: #333; }}
            {self._get_component_styles()}
        """