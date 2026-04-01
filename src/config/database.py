r"""
C:\Economy\Invest\TrendMaster\src\config\database.py
MySQL configuration utilities for PyMySQL + MySQL 8.0.
Clean, minimal, and focused on client-side settings only.
"""

from dataclasses import dataclass
from typing import Dict, Any
from .etl_config import DatabaseConfig


# ---------------------------------------------------------------------------
# MySQL Configuration Class
# ---------------------------------------------------------------------------

@dataclass
class MySQLConfig(DatabaseConfig):
    """
    Minimal MySQL 8.0 configuration for PyMySQL.
    Extends DatabaseConfig while remaining fully compatible.
    """

    charset: str = "utf8mb4"
    collation: str = "utf8mb4_unicode_ci"

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to a PyMySQL-compatible connection dictionary.
        """
        base = super().to_dict()

        base.update({
            "charset": self.charset,
        })

        return base


# ---------------------------------------------------------------------------
# Shared Factory Function
# ---------------------------------------------------------------------------

def mysql_config(**overrides) -> MySQLConfig:
    """
    Shared MySQL configuration factory.
    Applies common defaults and allows environment-specific overrides.
    """

    defaults = dict(
        host="localhost",
        port=3306,
        user="root",
        password="",
        pool_size=5,
        connect_timeout=10,
        charset="utf8mb4",
    )

    defaults.update(overrides)
    return MySQLConfig(**defaults)


# ---------------------------------------------------------------------------
# Environment Presets
# ---------------------------------------------------------------------------

def mysql_development() -> MySQLConfig:
    return mysql_config(
        database="store_manager_dev",
        pool_size=3,
    )


def mysql_production() -> MySQLConfig:
    return mysql_config(
        database="store_manager",
        user="etl_user",
        pool_size=20,
        connect_timeout=30,
    )


def mysql_testing() -> MySQLConfig:
    return mysql_config(
        database="store_manager_test",
        user="test_user",
        password="test_password",
        autocommit=True,
        pool_size=2,
        connect_timeout=5,
    )