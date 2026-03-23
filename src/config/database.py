"""
Database-specific configuration utilities and presets.
"""

from dataclasses import dataclass, field
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

@dataclass
class MySQLConfig(DatabaseConfig):
    """MySQL-specific configuration with optimized defaults."""

    # Character set
    charset: str = "utf8mb4"
    collation: str = "utf8mb4_unicode_ci"
    sql_mode: str = _DEFAULT_SQL_MODE

    # InnoDB (server-level tuning; kept for documentation)
    innodb_buffer_pool_size: str = "128M"
    innodb_log_file_size: str = "64M"

    # Performance (server-level tuning)
    max_connections: int = 151
    query_cache_size: str = "16M"  # Ignored in MySQL 8+
    tmp_table_size: str = "16M"
    max_heap_table_size: str = "16M"

    def to_dict(self) -> Dict[str, Any]:
        """
        Return a dict for mysql.connector, including MySQL-specific client options.
        Server-level tuning parameters are intentionally not included.
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