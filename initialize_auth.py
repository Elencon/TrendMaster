"""
Initialise the database with a users table and create the default administrator.
Run once to set up authentication for the first time.

Usage:
    python -m scripts.init_auth
"""

import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Path bootstrap  (must happen before local imports)
# ---------------------------------------------------------------------------

sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.database.connection_manager import DatabaseConnection
from src.database.schema_manager import SchemaManager
from src.auth.user_manager import UserManager
from src.config import DatabaseConfig

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SEPARATOR     = "=" * 60
_MIN_PASSWORD_LEN = 6
_DEFAULT_ADMIN = "admin"

# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------

def _connect(db_config: dict) -> DatabaseConnection:
    """Return a connected DatabaseConnection or raise."""
    manager = DatabaseConnection(db_config)
    if not manager.test_connection():
        raise RuntimeError("Could not connect to the database.")
    return manager


def _ensure_users_table(db_manager: DatabaseConnection) -> None:
    """Create the users table if it does not exist, or raise."""
    schema_manager = SchemaManager(db_manager)
    if not schema_manager.create_table("users"):
        raise RuntimeError("Failed to ensure the users table exists.")


def _resolve_credentials() -> tuple[str, str]:
    """
    Read admin credentials from environment variables.

    Returns:
        (username, password) tuple.

    Raises:
        ValueError: If the password is absent or too short.
    """
    username = os.getenv("ADMIN_USERNAME", _DEFAULT_ADMIN)
    password = os.getenv("ADMIN_PASSWORD", "")

    if not password:
        raise ValueError("ADMIN_PASSWORD is not set in the environment / .env file.")
    if len(password) < _MIN_PASSWORD_LEN:
        raise ValueError(
            f"ADMIN_PASSWORD must be at least {_MIN_PASSWORD_LEN} characters."
        )
    return username, password


def _user_exists(connection, username: str) -> bool:
    """Return True if *username* already exists in the users table."""
    cursor = connection.cursor(dictionary=True)
    try:
        cursor.execute(
            "SELECT user_id FROM users WHERE username = %s", (username,)
        )
        return cursor.fetchone() is not None
    finally:
        cursor.close()


def _create_admin(connection, username: str, password: str) -> None:
    """Create the administrator account, or skip if it already exists."""
    if _user_exists(connection, username):
        print(f"⚠️  User '{username}' already exists — skipping creation.")
        return

    user_manager = UserManager(connection)
    if not user_manager.create_user(username=username, password=password, role="Administrator"):
        raise RuntimeError(f"Failed to create administrator account '{username}'.")

    print(f"✅ Administrator account '{username}' created successfully.")


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def initialize_auth_system() -> bool:
    """
    Initialise the authentication system and create the default admin user.

    Returns:
        True on success, False on failure.
    """
    load_dotenv()

    print(_SEPARATOR)
    print("Authentication System Initialisation")
    print(_SEPARATOR)
    print()

    try:
        print("Connecting to database...")
        db_manager = _connect(DatabaseConfig().to_dict())
        print("✅ Connected to database.\n")

        print("Checking / creating users table...")
        _ensure_users_table(db_manager)
        print("✅ Users table ready.\n")

        username, password = _resolve_credentials()

        print(f"Creating administrator account '{username}'...")
        with db_manager.get_connection() as connection:
            _create_admin(connection, username, password)

        print()
        print(_SEPARATOR)
        print("Initialisation complete!")
        print(_SEPARATOR)
        print("\nYou can now log in with the credentials defined in your .env file.\n")
        return True

    except Exception as exc:
        _logger.error("Initialisation failed: %s", exc)
        print(f"\n❌ Error: {exc}")
        return False

    finally:
        print("Done.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        sys.exit(0 if initialize_auth_system() else 1)
    except KeyboardInterrupt:
        print("\n\nInitialisation cancelled by user.")
        sys.exit(1)