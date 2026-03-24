r"""
C:\Economy\Invest\TrendMaster\tests\auth\test_user_repository.py
Migration script to add security and 2FA columns to the users table.
Run this once to update existing databases.
"""

import logging
import sys
from pathlib import Path

# Add project root to path before local imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from connect import connect_to_mysql
from config.database import DatabaseConfig

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TABLE = "users"

_COLUMNS_TO_ADD = [
    ("failed_login_attempts", "INT DEFAULT 0"),
    ("last_failed_attempt",   "DATETIME"),
    ("locked_until",          "DATETIME"),
    ("password_last_changed", "DATETIME DEFAULT CURRENT_TIMESTAMP"),
    ("must_change_password",  "BOOLEAN DEFAULT FALSE"),
    ("two_factor_enabled",    "BOOLEAN DEFAULT FALSE"),
    ("two_factor_secret",     "VARCHAR(32)"),
    ("backup_codes",          "TEXT"),
]

_COLUMN_NAMES = [name for name, _ in _COLUMNS_TO_ADD]

_DESCRIBE_QUERY   = f"DESCRIBE {_TABLE}"
_ALTER_QUERY_TMPL = "ALTER TABLE {table} ADD COLUMN {col} {defn}"

_SEPARATOR = "=" * 60

# ---------------------------------------------------------------------------
# Migration
# ---------------------------------------------------------------------------

def migrate_users_table() -> bool:
    """
    Add security and 2FA columns to the users table.

    Skips columns that already exist.

    Returns:
        True if migration completed (even if some columns were skipped),
        False if a fatal error occurred.
    """
    try:
        connection = connect_to_mysql(DatabaseConfig().to_dict())
    except Exception as e:
        _logger.error("Could not create DB config: %s", e)
        return False

    if not connection:
        _logger.error("Failed to connect to database")
        return False

    try:
        cursor = connection.cursor()
        existing = _get_existing_columns(cursor)
        _apply_missing_columns(cursor, connection, existing)
        cursor.close()
        _logger.info("Migration completed successfully!")
        return True

    except Exception as e:
        _logger.error("Migration failed: %s", e)
        return False

    finally:
        connection.close()

# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _get_existing_columns(cursor) -> set:
    """Return the set of column names currently in the users table."""
    cursor.execute(_DESCRIBE_QUERY)
    return {row[0] for row in cursor.fetchall()}


def _apply_missing_columns(cursor, connection, existing: set) -> None:
    """Add each column in _COLUMNS_TO_ADD that is not yet present."""
    for col, defn in _COLUMNS_TO_ADD:
        if col in existing:
            _logger.info("Skipped (already exists): %s", col)
            continue
        try:
            cursor.execute(_ALTER_QUERY_TMPL.format(table=_TABLE, col=col, defn=defn))
            connection.commit()
            _logger.info("Added column: %s", col)
        except Exception as e:
            _logger.error("Failed to add column %s: %s", col, e)

# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _print_header() -> None:
    print(_SEPARATOR)
    print("DATABASE MIGRATION: Adding Security & 2FA Columns")
    print(_SEPARATOR)
    print(f"\nThis will add the following columns to '{_TABLE}':")
    for name in _COLUMN_NAMES:
        print(f"  • {name}")
    print(f"\n{_SEPARATOR}")


def _confirm() -> bool:
    response = input("\nProceed with migration? (yes/no): ").strip().lower()
    return response in {"yes", "y"}


if __name__ == "__main__":
    _print_header()

    if not _confirm():
        print("\nMigration cancelled.")
        sys.exit(0)

    print("\nRunning migration...\n")
    if migrate_users_table():
        print("\nDatabase updated successfully!")
        sys.exit(0)
    else:
        print("\nMigration failed. Check logs for details.")
        sys.exit(1)