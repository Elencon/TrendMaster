# src/common/base_db_config.py

from dataclasses import dataclass, field
from typing import Any, Dict

@dataclass
class BaseDbConfig:
    """Shared DB identity fields with environment-based defaults."""

    user:     str = field(default_factory=lambda: _env("DB_USER", "root"))
    password: str = field(default_factory=lambda: _env("DB_PASSWORD", ""))
    host:     str = field(default_factory=lambda: _env("DB_HOST", "127.0.0.1"))
    port:     int = field(default_factory=lambda: _env_int("DB_PORT", 3306))
    database: str = field(default_factory=lambda: _env("DB_NAME", "trend_master"))

    def get_connection_string(self) -> str:
        """Safe connection string for logging."""
        return f"mysql://{self.user}@{self.host}:{self.port}/{self.database}"

