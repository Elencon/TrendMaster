# integrity_service/config.py

r"""
Configuration management for IntegrityService using Pydantic Settings.
Loads settings from .env file in the project root with strong validation.
cd IntegrityService
python -m integrity_service.config
"""

from __future__ import annotations

import sys
import os
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Main application settings with validation and .env support."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",           # Ignore unknown env vars
        case_sensitive=False,     # DATABASE_URL or database_url both work
        env_prefix="",            # No prefix
    )

    # Database configuration
    database_url: str = Field(
        ...,
        alias="DATABASE_URL",
        description="Database connection URL (supports PostgreSQL, SQLite, etc.)",
        examples=["postgresql://user:pass@localhost/dbname", "sqlite:///data.db"],
    )

    # Optional: Add more settings here in the future
    # debug: bool = False
    # log_level: str = "INFO"


def _find_project_root() -> Path:
    """
    Always return the TrendMaster project root.
    TrendMaster/
        IntegrityService/
            integrity_service/
                config.py
    """
    return Path(__file__).resolve().parents[2]


def load_config() -> dict[str, str]:
    """
    Load configuration from .env file in the TrendMaster root folder.

    Returns:
        dict: Configuration dictionary (keeps original interface).

    Raises:
        pydantic.ValidationError: If required fields are missing or invalid.
    """
    project_root = _find_project_root()

    # Temporarily change working directory so pydantic-settings can find .env
    original_cwd = Path.cwd()
    try:
        os.chdir(project_root)  # pydantic looks for .env relative to cwd by default

        settings = Settings()

        print(f"✅ Loaded configuration from: {project_root}/.env")
        print(f"   Database URL: {settings.database_url[:60]}...")

        # Return dict to maintain exact original interface
        return {"db_url": settings.database_url}

    except Exception as e:
        print(f"⚠️  Failed to load .env from: {project_root}", file=sys.stderr)
        raise RuntimeError(
            f"❌ Failed to load configuration!\n"
            f"Project root: {project_root}\n"
            f"Error: {e}"
        ) from e
    finally:
        os.chdir(original_cwd)


# Optional: Also expose the Settings object directly (recommended for new code)
def get_settings() -> Settings:
    """Return the full Settings object (preferred over dict for new code)."""
    project_root = _find_project_root()
    original_cwd = Path.cwd()
    try:
        os.chdir(project_root)
        return Settings()
    finally:
        os.chdir(original_cwd)



if __name__ == "__main__":
    from pathlib import Path
    from pprint import pprint

    print("\n=== Running config self‑test ===")

    # TrendMaster is two levels above this file:
    # TrendMaster/IntegrityService/integrity_service/config.py
    project_root = Path(__file__).resolve().parents[2]
    env_path = project_root / ".env"

    print(f"Project root: {project_root}")
    print(f".env path:    {env_path}")

    # Print .env contents
    if env_path.exists():
        print("\n--- .env contents ---")
        try:
            print(env_path.read_text())
        except Exception as e:
            print(f"⚠️ Could not read .env: {e}")
        print("----------------------\n")
    else:
        print("\n⚠️  .env file not found!\n")

    # Load configuration
    try:
        cfg = load_config()
        print("Loaded configuration:")
        pprint(cfg)
        print("\n✔ Configuration loaded successfully\n")
    except Exception as e:
        print("\n❌ Configuration failed to load\n")
        raise e