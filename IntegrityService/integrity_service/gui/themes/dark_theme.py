from .base_theme import BaseTheme

class DarkTheme(BaseTheme):
    def __init__(self):
        super().__init__("dark")

    def get_qt_material_theme(self) -> str:
        return "dark_cyan.xml"

    def get_button_text(self) -> str:
        return "Switch to Light Mode"

    def get_palette_colors(self) -> dict:
        return {
            "background": "#1e1e1e",
            "surface": "#2d2d2d",
            "text": "#e0e0e0",
            "accent": "#00d4ff",
            "accent_hover": "#00b0d4",
            "border": "#444444",
            "button_bg": "#3a3a3a",
            "button_hover": "#4a4a4a",
            "button_pressed": "#2a2a2a",
            "button_text": "#ffffff",
            "input_bg": "#252525",
            "selection": "#00d4ff",
            "disabled_bg": "#333333",
            "disabled_text": "#777777",
        }