# src/common/base_db_config.py

from dataclasses import dataclass, field
from typing import Any, Dict

@dataclass
class BaseDbConfig:
    """Shared DB identity fields with simple built‑in defaults."""

    user: str = "root"
    password: str = ""
    host: str = "127.0.0.1"
    port: int = 3306
    database: str = "trend_master"

    def get_connection_string(self) -> str:
        """Safe connection string for logging."""
        return f"mysql://{self.user}@{self.host}:{self.port}/{self.database}"