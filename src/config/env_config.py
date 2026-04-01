r"""
C:\Economy\Invest\TrendMaster\src\config\env_config.py
Centralized environment configuration using .env file.
Provides secure access to environment variables with fallback defaults.
python -m config.env_config
"""

from .path_config import PROJECT_ROOT, DATA_PATH, CSV_PATH, API_PATH, ENV_PATH
from pathlib import Path
import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------
# Optional dotenv import
# ---------------------------------------------------------
try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None
    logger.warning("python-dotenv not installed. Using system environment variables only.")

# ---------------------------------------------------------
# Load .env from project root
# ---------------------------------------------------------
if load_dotenv and ENV_PATH.exists():
    load_dotenv(ENV_PATH)
    logger.info(f"Loaded environment variables from {ENV_PATH}")
else:
    logger.debug("No .env file loaded (missing file or python-dotenv)")


class EnvConfig:
    """Centralized environment configuration"""

    # -----------------------------
    # Path Accessors
    # -----------------------------
    @property
    def project_root(self) -> Path:
        return Path(self.get("PROJECT_ROOT", str(PROJECT_ROOT)))

    @property
    def data_path(self) -> Path:
        override = self.get("DATA_PATH")
        return Path(override) if override else DATA_PATH

    @property
    def csv_path(self) -> Path:
        override = self.get("DATA_PATH")
        return Path(override) / "CSV" if override else CSV_PATH

    @property
    def api_data_path(self) -> Path:
        override = self.get("DATA_PATH")
        return Path(override) / "API" if override else API_PATH

    # -----------------------------
    # Generic Getters
    # -----------------------------
    @staticmethod
    def get(key: str, default: Optional[str] = None) -> Optional[str]:
        value = os.getenv(key, default)
        if isinstance(value, str):
            return value.strip()
        return value

    @staticmethod
    def get_int(key: str, default: int) -> int:
        raw = os.getenv(key)
        if raw is None:
            return default

        raw = raw.strip()
        try:
            return int(raw)
        except ValueError:
            logger.warning(f"Invalid integer for {key}='{raw}', using default {default}")
            return default

    @staticmethod
    def get_bool(key: str, default: bool) -> bool:
        raw = os.getenv(key)
        if raw is None:
            return default

        value = raw.strip().lower()

        truthy = {"true", "1", "yes", "on"}
        falsey = {"false", "0", "no", "off"}

        if value in truthy:
            return True
        if value in falsey:
            return False

        logger.warning(f"Invalid boolean for {key}='{raw}', using default {default}")
        return default

    # -----------------------------
    # Database Configuration
    # -----------------------------
    @property
    def db_host(self) -> str:
        return self.get("DB_HOST", "localhost")

    @property
    def db_port(self) -> int:
        return self.get_int("DB_PORT", 3306)

    @property
    def db_name(self) -> str:
        return self.get("DB_NAME", "store_manager")

    @property
    def db_user(self) -> str:
        return self.get("DB_USER", "root")

    @property
    def db_password(self) -> str:
        return self.get("DB_PASSWORD", "")

    # -----------------------------
    # API Configuration
    # -----------------------------
    @property
    def api_url(self) -> str:
        return self.get("API_URL", "https://etl-server.fly.dev")

    @property
    def api_key(self) -> Optional[str]:
        key = self.get("API_KEY")
        return key or None

    @property
    def api_bearer_token(self) -> Optional[str]:
        token = self.get("API_BEARER_TOKEN","")      # ????
        return token

    # -----------------------------
    # Security Settings
    # -----------------------------
    @property
    def session_timeout_minutes(self) -> int:
        return self.get_int("SESSION_TIMEOUT_MINUTES", 30)

    @property
    def max_login_attempts(self) -> int:
        return self.get_int("MAX_LOGIN_ATTEMPTS", 5)

    @property
    def lockout_duration_minutes(self) -> int:
        return self.get_int("LOCKOUT_DURATION_MINUTES", 15)

    # -----------------------------
    # Logging Settings
    # -----------------------------

    @property
    def log_level(self) -> str:
        return self.get("LOG_LEVEL", "INFO").upper()

    # -----------------------------
    # Application Settings
    # -----------------------------

    @property
    def environment(self) -> str:
        env = self.get("ENVIRONMENT", "development")
        return env.lower().strip()

    @property
    def debug(self) -> bool:
        return self.get_bool("DEBUG", True)


# Singleton instance
env_config = EnvConfig()

# ---------------------------------------------------------
# Basic self-tests (run only when executed directly)
# ---------------------------------------------------------
if __name__ == "__main__":
    print("Running EnvConfig self-tests...\n")

    cfg = EnvConfig()

    # Test: project root exists
    print("project_root:", cfg.project_root)
    assert cfg.project_root.exists(), "Project root does not exist!"

    # Test: data paths resolve correctly
    print("data_path:", cfg.data_path)
    print("csv_path:", cfg.csv_path)
    print("api_data_path:", cfg.api_data_path)

    # Test: environment getters
    print("environment:", cfg.environment)
    print("debug:", cfg.debug)
    print("log_level:", cfg.log_level)

    # Test: database config
    print("db_host:", cfg.db_host)
    print("db_port:", cfg.db_port)
    print("db_name:", cfg.db_name)
    print("db_user:", cfg.db_user)
    print("db_password:", cfg.db_password)
    
    # Test: API config
    print("api_url:", cfg.api_url)
    print("api_key:", cfg.api_key)

    print("\nAll EnvConfig self-tests completed.")