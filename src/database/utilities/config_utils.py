"""
Configuration management and validation utilities.
"""

import logging
from typing import Any

from dotenv import dotenv_values

_logger = logging.getLogger(__name__)


class ConfigUtils:
    """Configuration management utilities for database connections."""

    @staticmethod
    def merge_configs(*configs: dict) -> dict:
        """
        Merge multiple configuration dictionaries, later values winning.

        Args:
            *configs: Configuration dicts to merge in order.

        Returns:
            Merged configuration dictionary.
        """
        result: dict = {}
        for cfg in configs:
            if cfg:
                result.update(cfg)
        return result

    @staticmethod
    def get_env_config(
        prefix: str = "DB_",
        env_file: str = ".env",
    ) -> dict[str, Any]:
        """
        Get database configuration from a .env file without mutating os.environ.

        Args:
            prefix: Environment variable prefix (default: "DB_").
            env_file: Path to the .env file (default: ".env").

        Returns:
            Configuration dictionary derived from the .env file.
        """
        env = dotenv_values(env_file)

        env_mapping = {
            f"{prefix}USER":     "user",
            f"{prefix}PASSWORD": "password",
            f"{prefix}HOST":     "host",
            f"{prefix}PORT":     "port",
            f"{prefix}NAME":     "database",
            f"{prefix}CHARSET":  "charset",
        }

        config: dict[str, Any] = {}
        for env_key, config_key in env_mapping.items():
            value = env.get(env_key)
            if not value:
                continue
            if config_key == "port":
                try:
                    config[config_key] = int(value)
                except ValueError:
                    _logger.warning("Invalid port value in %s: %s", env_key, value)
            else:
                config[config_key] = value

        return config

    @staticmethod
    def validate_config(config: dict) -> tuple[bool, list[str]]:
        """
        Validate database configuration for required fields and value sanity.

        Args:
            config: Configuration dictionary to validate.

        Returns:
            Tuple of (is_valid, list_of_error_messages).
        """
        required_fields = ["user", "host", "database"]
        errors: list[str] = []

        missing = [f for f in required_fields if not config.get(f)]
        if missing:
            errors.append(f"Missing required config fields: {missing}")

        if "port" in config:
            port = config["port"]
            if not isinstance(port, int) or not (1 <= port <= 65535):
                errors.append(f"Invalid port: {port!r} (must be integer 1–65535)")

        if "host" in config and not str(config["host"]).strip():
            errors.append("Host cannot be empty")

        if "database" in config and not str(config["database"]).strip():
            errors.append("Database name cannot be empty")

        if config.get("password") == "":
            _logger.warning("Database password is empty — this may be insecure")

        return len(errors) == 0, errors

    @staticmethod
    def get_default_config() -> dict[str, Any]:
        """Return a default database configuration."""
        return {
            "user":     "root",
            "password": "",
            "host":     "127.0.0.1",
            "port":     3306,
            "database": "trend_master",
            "charset":  "utf8mb4",
        }

    @staticmethod
    def mask_sensitive_config(
        config: dict,
        sensitive_keys: list[str] | None = None,
    ) -> dict:
        """
        Return a copy of config with sensitive values masked for safe logging.

        Args:
            config: Configuration dictionary.
            sensitive_keys: Substrings that identify sensitive keys.
                Defaults to ['password', 'secret', 'key', 'token'].

        Returns:
            Config copy with sensitive values replaced by asterisks.
        """
        if sensitive_keys is None:
            sensitive_keys = ["password", "secret", "key", "token"]

        masked = config.copy()
        for key, value in masked.items():
            if any(s in key.lower() for s in sensitive_keys):
                masked[key] = "*" * min(len(str(value)), 8) if value else "***"

        return masked
