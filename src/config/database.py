"""
Database-specific configuration utilities and presets.
"""

from dataclasses import dataclass
from typing import Any, Dict

from . import DatabaseConfig

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_SQL_MODE = (
    "STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO"
)

# ---------------------------------------------------------------------------
# MySQLConfig
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class MySQLConfig(DatabaseConfig):
    """
    MySQL-specific configuration extending DatabaseConfig.

    - Does NOT redeclare charset (inherited from DatabaseConfig)
    - Applies MySQL-specific defaults in __post_init__
    - Adds collation + sql_mode cleanly
    """

    collation: str = "utf8mb4_unicode_ci"
    sql_mode: str = _DEFAULT_SQL_MODE

    # Server-level tuning (documented, not included in to_dict)
    innodb_buffer_pool_size: str = "128M"
    innodb_log_file_size: str = "64M"
    max_connections: int = 151
    query_cache_size: str = "16M"
    tmp_table_size: str = "16M"
    max_heap_table_size: str = "16M"

    def __post_init__(self):
        # Run parent validation first
        super().__post_init__()

        # MySQL-specific validation
        if not self.collation:
            raise ValueError("MySQL collation cannot be empty")

        if not isinstance(self.sql_mode, str) or not self.sql_mode.strip():
            raise ValueError("sql_mode must be a non-empty string")

        # Override charset ONLY if user did not specify one
        if self.charset is None or self.charset.strip() == "":
            object.__setattr__(self, "charset", "utf8mb4")

    def to_dict(self) -> Dict[str, Any]:
        """
        Return a dict for mysql.connector or PyMySQL.
        Only client-side options are included.
        """
        base = super().to_dict()

        base.update(
            {
                "charset": self.charset,
                "collation": self.collation,
                "init_command": f"SET sql_mode='{self.sql_mode}'",
            }
        )

        return base

# ---------------------------------------------------------------------------
# Preset factories
# ---------------------------------------------------------------------------

def get_mysql_development_config() -> MySQLConfig:
    """Return a MySQLConfig optimized for local development."""
    return MySQLConfig(
        host="localhost",
        port=3306,
        database="store_manager_dev",
        pool_size=3,
        connect_timeout=10,
        query_cache_size="8M",
    )


def get_mysql_production_config() -> MySQLConfig:
    """Return a MySQLConfig optimized for production workloads."""
    return MySQLConfig(
        pool_size=20,
        connect_timeout=30,
        max_connections=500,
        query_cache_size="64M",
        innodb_buffer_pool_size="512M",
        innodb_log_file_size="256M",
    )


def get_mysql_testing_config() -> MySQLConfig:
    """Return a MySQLConfig suitable for automated test environments."""
    return MySQLConfig(
        database="store_manager_test",
        pool_size=2,
        connect_timeout=5,
        autocommit=True,
        raise_on_warnings=False,
    )
