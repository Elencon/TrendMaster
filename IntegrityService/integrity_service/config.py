# integrity_service/config.py

import os
from pathlib import Path

from dotenv import load_dotenv


def load_config() -> dict:
    """
    Load configuration from .env file in the TrendMaster root folder.
    """
    # Find TrendMaster root directory
    current_file = Path(__file__).resolve()
    trendmaster_dir = current_file.parent.parent.parent  # Adjust levels if needed

    env_path = trendmaster_dir / ".env"

    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)
        print(f"✅ Loaded .env from: {env_path}")
    else:
        print(f"⚠️  .env file not found at: {env_path}")

    config = {
        "db_url": os.getenv("DATABASE_URL") or os.getenv("DB_URL"),
    }

    if not config["db_url"]:
        raise RuntimeError(
            "❌ DATABASE_URL is not set in .env file!\n"
            f"Please add it to: {env_path}"
        )

    return config