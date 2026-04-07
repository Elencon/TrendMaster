from pathlib import Path
from abc import ABC, abstractmethod
from typing import Dict, Any

from qt_material import apply_stylesheet

class BaseTheme(ABC):
    def __init__(self, name: str):
        self.name = name

    def load_qss_template(self) -> str:
        theme_file = Path(__file__).parent / "themes" / f"{self.name.lower().replace(' ', '_')}.qss"
        return theme_file.read_text(encoding="utf-8")

    def build_qss(self) -> str:
        qss = self.load_qss_template()
        colors = self.get_palette_colors()

        for key, value in colors.items():
            qss = qss.replace(f"@{key}@", value)

        return qss

    def apply_theme(self, app):
        try:
            # optional qt-material base
            apply_stylesheet(app, theme=self.get_qt_material_theme())
        except Exception:
            pass

        app.setStyleSheet(self.build_qss())