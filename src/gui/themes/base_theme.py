"""
Base Theme Class
Abstract base for all theme implementations
"""

from abc import ABC, abstractmethod
from typing import Dict, Any

try:
    from qt_material import apply_stylesheet
    QT_MATERIAL_AVAILABLE = True
except ImportError:
    QT_MATERIAL_AVAILABLE = False


class BaseTheme(ABC):
    """Abstract base class for theme implementations"""
    
    def __init__(self, name: str):
        self.name = name
        self.qt_material_available = QT_MATERIAL_AVAILABLE
    
    @abstractmethod
    def get_qt_material_theme(self) -> str:
        """Return the qt-material theme name"""
        pass
    
    @abstractmethod
    def get_custom_styles(self) -> str:
        """Return custom CSS styles to apply on top of qt-material theme"""
        pass
    
    @abstractmethod
    def get_button_text(self) -> str:
        """Return the text for theme toggle button"""
        pass
    
    @abstractmethod
    def get_fallback_styles(self) -> str:
        """Return fallback styles if qt-material is not available"""
        pass
    
    def apply_theme(self, app) -> None:
        """Apply the complete theme to the application"""
        if self.qt_material_available:
            # Apply qt-material theme
            apply_stylesheet(app, theme=self.get_qt_material_theme())
            
            # Apply custom styles on top
            custom_styles = self.get_custom_styles()
            if custom_styles:
                current_stylesheet = app.styleSheet()
                app.setStyleSheet(current_stylesheet + custom_styles)
        else:
            # Use fallback styles
            app.setStyleSheet(self.get_fallback_styles())
    
    def get_theme_info(self) -> Dict[str, Any]:
        """Get theme information"""
        return {
            'name': self.name,
            'qt_material_available': self.qt_material_available,
            'qt_material_theme': self.get_qt_material_theme(),
            'button_text': self.get_button_text()
        }