"""
TrendMaster package initializer.

This module simply re-exports key project paths from config.path_config
so they can be imported directly from `src`.
"""

from .config.path_config import (
    PROJECT_ROOT,
    SRC_PATH,
    GUI_PATH,
    DATA_PATH,
    CSV_PATH,
    API_PATH,
    CACHE_PATH,
    ENV_PATH,
    LOGS_PATH,
    CONFIG_PATH,
)

__all__ = [
    "PROJECT_ROOT",
    "SRC_PATH",
    "GUI_PATH",
    "DATA_PATH",
    "CSV_PATH",
    "API_PATH",
    "CACHE_PATH",
    "ENV_PATH",
    "LOGS_PATH",
    "CONFIG_PATH",
]


# Optional: Debug mode when running directly
if __name__ == "__main__":
    print("PROJECT_ROOT:", PROJECT_ROOT)
    print("SRC_PATH:", SRC_PATH)
    print("GUI_PATH:", GUI_PATH)
    print("DATA_PATH:", DATA_PATH)
    print("CSV_PATH:", CSV_PATH)
    print("API_PATH:", API_PATH)
    print("CACHE_PATH:", CACHE_PATH)
    print("ENV_PATH:", ENV_PATH)
    print("LOGS_PATH:", LOGS_PATH)
    print("CONFIG_PATH:", CONFIG_PATH)