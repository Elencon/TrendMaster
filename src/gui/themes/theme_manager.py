"""
Theme Manager
Manages theme switching and application for ETL Pipeline Manager
"""

from typing import Dict, Type
from .base_theme import BaseTheme
from .light_theme import LightTheme
from .dark_theme import DarkTheme


class ThemeManager:
    """Manages themes for the ETL Pipeline Manager"""

    def __init__(self):
        self._themes: Dict[str, Type[BaseTheme]] = {
            'light': LightTheme,
            'dark': DarkTheme
        }
        self._current_theme: BaseTheme = None
        self._current_theme_name: str = 'dark'  # Default theme

    def get_available_themes(self) -> list:
        """Get list of available theme names"""
        return list(self._themes.keys())

    def get_current_theme_name(self) -> str:
        """Get current theme name"""
        return self._current_theme_name

    def get_current_theme(self) -> BaseTheme:
        """Get current theme instance"""
        if self._current_theme is None:
            self._current_theme = self._themes[self._current_theme_name]()
        return self._current_theme

    def set_theme(self, theme_name: str) -> BaseTheme:
        """Set current theme by name"""
        if theme_name not in self._themes:
            raise ValueError(f"Theme '{theme_name}' not found. Available: {list(self._themes.keys())}")

        self._current_theme_name = theme_name
        self._current_theme = self._themes[theme_name]()
        return self._current_theme

    def toggle_theme(self) -> BaseTheme:
        """Toggle between light and dark themes"""
        new_theme = 'light' if self._current_theme_name == 'dark' else 'dark'
        return self.set_theme(new_theme)

    def apply_current_theme(self, app) -> None:
        """Apply current theme to application"""
        current_theme = self.get_current_theme()
        current_theme.apply_theme(app)

    def get_theme_button_text(self) -> str:
        """Get text for theme toggle button"""
        current_theme = self.get_current_theme()
        return current_theme.get_button_text()

    def is_dark_mode(self) -> bool:
        """Check if current theme is dark mode"""
        return self._current_theme_name == 'dark'
