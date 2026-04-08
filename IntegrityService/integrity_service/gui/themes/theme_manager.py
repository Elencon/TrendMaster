from typing import Dict, Type
from .base_theme import BaseTheme
from .light_theme import LightTheme
from .dark_theme import DarkTheme

class ThemeManager:
    """Modern QSS-based theme manager."""

    def __init__(self):
        # Map theme names to theme classes
        self._themes: Dict[str, Type[BaseTheme]] = {
            "light": LightTheme,
            "dark": DarkTheme,
        }
        self._current_theme: BaseTheme = None
        self._current_theme_name = "dark"

    def get_available_themes(self) -> list:
        return list(self._themes.keys())

    def get_current_theme_name(self) -> str:
        return self._current_theme_name

    def get_current_theme(self) -> BaseTheme:
        if self._current_theme is None:
            self._current_theme = self._themes[self._current_theme_name]()
        return self._current_theme

    def set_theme(self, theme_name: str) -> None:
        if theme_name not in self._themes:
            raise ValueError(f"Theme '{theme_name}' not found. Available: {self.get_available_themes()}")

        self._current_theme_name = theme_name
        self._current_theme = self._themes[theme_name]()

    def toggle_theme(self) -> str:
        new_theme = "light" if self._current_theme_name == "dark" else "dark"
        self.set_theme(new_theme)
        return new_theme

    def apply_current_theme(self, app) -> None:
        current_theme = self.get_current_theme()
        current_theme.apply_theme(app)

    def is_dark_mode(self) -> bool:
        return self._current_theme_name == "dark"

    def get_theme_button_text(self) -> str:
        current_theme = self.get_current_theme()
        return current_theme.get_button_text()
