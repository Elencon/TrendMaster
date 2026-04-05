"""
Application bootstrap module.
Initializes logging and ensures required directories exist.
"""

from .config.path_config import (
    DATA_PATH,
    CSV_PATH,
    API_PATH,
    LOGS_PATH,
)
from .logging_system import setup_logging


def initialize() -> None:
    """Initialize logging and prepare runtime directories."""

    # Ensure required directories exist
    for p in (DATA_PATH, CSV_PATH, API_PATH, LOGS_PATH):
        p.mkdir(parents=True, exist_ok=True)

    # Initialize logging
    setup_logging(LOGS_PATH)