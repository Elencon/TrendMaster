from .base_theme import BaseTheme

class LightTheme(BaseTheme):
    def __init__(self):
        super().__init__("light")

    def get_qt_material_theme(self) -> str:
        return "light_cyan.xml"

    def get_button_text(self) -> str:
        return "Switch to Dark Mode"

    def get_palette_colors(self) -> dict:
        return {
            "background": "#ffffff",
            "surface": "#f2f2f2",
            "text": "#1e1e1e",
            "accent": "#00a4c8",
            "accent_hover": "#008fb0",
            "border": "#cccccc",
            "button_bg": "#e6e6e6",
            "button_hover": "#dcdcdc",
            "button_pressed": "#c8c8c8",
            "button_text": "#000000",
            "input_bg": "#ffffff",
            "selection": "#00a4c8",
            "disabled_bg": "#f0f0f0",
            "disabled_text": "#999999",
        }