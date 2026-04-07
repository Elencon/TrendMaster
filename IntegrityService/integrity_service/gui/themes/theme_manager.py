from pathlib import Path
from typing import Dict


class ThemeManager:
    """Modern QSS-based theme manager."""

    def __init__(self):
        self.theme_dir = Path(__file__).parent / "themes"

        # Map theme names to QSS files
        self._themes: Dict[str, Path] = {
            "light": self.theme_dir / "light.qss",
            "dark": self.theme_dir / "dark.qss",
        }

        self._current_theme_name = "dark"

    def get_available_themes(self) -> list:
        return list(self._themes.keys())

    def get_current_theme_name(self) -> str:
        return self._current_theme_name

    def set_theme(self, theme_name: str) -> None:
        if theme_name not in self._themes:
            raise ValueError(f"Theme '{theme_name}' not found. Available: {self.get_available_themes()}")

        self._current_theme_name = theme_name

    def toggle_theme(self) -> str:
        new_theme = "light" if self._current_theme_name == "dark" else "dark"
        self.set_theme(new_theme)
        return new_theme

    def apply_current_theme(self, app) -> None:
        qss_file = self._themes[self._current_theme_name]

        if qss_file.exists():
            app.setStyleSheet(qss_file.read_text(encoding="utf-8"))
        else:
            print(f"Warning: QSS file not found: {qss_file}")

    def is_dark_mode(self) -> bool:
        return self._current_theme_name == "dark"

    def get_theme_button_text(self) -> str:
        return "Switch to Light" if self.is_dark_mode() else "Switch to Dark"