"""
Database-specific configuration utilities and presets.
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional
from . import DatabaseConfig


@dataclass
class MySQLConfig(DatabaseConfig):
    """
    MySQL-specific configuration with optimized defaults.
    Extends DatabaseConfig while remaining fully compatible with its interface.
    """

    # MySQL-specific settings
    charset: str = field(default='utf8mb4')
    collation: str = field(default='utf8mb4_unicode_ci')
    sql_mode: str = (
        "STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO"
    )

    # InnoDB settings
    innodb_buffer_pool_size: str = "128M"
    innodb_log_file_size: str = "64M"

    # Performance settings
    max_connections: int = 151
    query_cache_size: str = "16M"
    tmp_table_size: str = "16M"
    max_heap_table_size: str = "16M"

    # Allow DatabaseConfig to accept extra MySQL-specific kwargs safely
    extra: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """
        Finalize MySQL configuration after initialization.
        Handles type coercion for environment variables and ensures
        derived connection properties are set.
        """

        # --- 1. Type Coercion: Convert numeric strings safely ---
        for attr, default in [
            ("port", 3306),
            ("pool_size", None),
            ("connect_timeout", None),
        ]:
            value = getattr(self, attr, None)
            if isinstance(value, str):
                try:
                    setattr(self, attr, int(value))
                except ValueError:
                    if default is not None:
                        setattr(self, attr, default)

        # --- 2. Boolean Coercion: Convert string booleans safely ---
        truthy = {"true", "1", "yes", "on"}
        falsey = {"false", "0", "no", "off"}

        for attr in ["enable_pooling", "pool_reset_session", "autocommit", "use_pure"]:
            value = getattr(self, attr, None)
            if isinstance(value, str):
                v = value.strip().lower()
                if v in truthy:
                    setattr(self, attr, True)
                elif v in falsey:
                    setattr(self, attr, False)
                # else: leave as original default

        # --- 3. Normalize charset/collation safely ---
        if isinstance(self.charset, str):
            self.charset = self.charset.lower()

        if isinstance(self.collation, str):
            self.collation = self.collation.lower()

        # --- 4. Validate port range ---
        if not (1 <= self.port <= 65535):
            self.port = 3306

        # --- 5. Unix socket precedence ---
        unix_socket = getattr(self, "unix_socket", None)
        if unix_socket:
            # In real apps you might log a warning here
            pass


    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary including MySQL-specific settings.
        Fully compatible with MySQL Python clients.
        """
        base_dict = super().to_dict()

        # Safely escape SQL mode
        safe_sql_mode = self.sql_mode.replace("'", "\\'")

        mysql_settings = {
            "charset": self.charset,
            "collation": self.collation,
            "init_command": f"SET sql_mode='{safe_sql_mode}'",
            "max_connections": self.max_connections,
            "query_cache_size": self.query_cache_size,
            "tmp_table_size": self.tmp_table_size,
            "max_heap_table_size": self.max_heap_table_size,
            "innodb_buffer_pool_size": self.innodb_buffer_pool_size,
            "innodb_log_file_size": self.innodb_log_file_size,
        }

        # Merge everything
        base_dict.update(mysql_settings)
        base_dict.update(self.extra)

        return base_dict


# ---------------------------------------------------------------------------
# Preset factory functions
# ---------------------------------------------------------------------------

def get_mysql_development_config() -> MySQLConfig:
    """Get MySQL configuration optimized for development."""
    return MySQLConfig(
        host="localhost",
        port=3306,
        database="store_manager_dev",
        pool_size=3,
        connect_timeout=10,
        query_cache_size="8M",
    )


def get_mysql_production_config() -> MySQLConfig:
    """Get MySQL configuration optimized for production."""
    return MySQLConfig(
        pool_size=20,
        connect_timeout=30,
        max_connections=500,
        query_cache_size="64M",
        innodb_buffer_pool_size="512M",
        innodb_log_file_size="256M",
    )


def get_mysql_testing_config() -> MySQLConfig:
    """Get MySQL configuration for testing environments."""
    return MySQLConfig(
        database="store_manager_test",
        pool_size=2,
        connect_timeout=5,
        autocommit=True,          # Faster test cleanup
        raise_on_warnings=False,  # Less strict for testing
    )