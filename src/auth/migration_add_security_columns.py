"""
Database migration to add security fields for account lockout.
Run this script to add the necessary columns to the users table.
"""

import logging
import sys
from pathlib import Path

# Add project root to path before local imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from database import connect_to_mysql

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TABLE = "users"

# Correct column names matching AccountLockoutManager
_COLUMNS_TO_ADD = [
    ("failed_login_attempts", "INT DEFAULT 0"),
    ("locked_until",          "DATETIME NULL"),
    ("last_failed_attempt",   "DATETIME NULL"),
    ("last_login",            "DATETIME NULL"),
]

_DESCRIBE_QUERY   = f"DESCRIBE {_TABLE}"
_ALTER_QUERY_TMPL = "ALTER TABLE {table} ADD COLUMN {col} {defn}"
_DUPLICATE_MARKER = "Duplicate column"
_SEPARATOR        = "=" * 60

# ---------------------------------------------------------------------------
# Migration
# ---------------------------------------------------------------------------

def add_security_columns() -> None:
    """Add security-related columns to the users table."""
    conn = connect_to_mysql()

    try:
        cursor = conn.cursor()
        _logger.info("Adding security columns to '%s' table...", _TABLE)

        existing = _get_existing_columns(cursor)
        _apply_missing_columns(cursor, existing)

        conn.commit()
        _logger.info("Security columns added successfully!")
        _log_table_structure(cursor)

    except Exception as e:
        _logger.error("Error adding security columns: %s", e)
        conn.rollback()
        raise

    finally:
        try:
            cursor.close()
        except Exception:
            pass
        conn.close()

# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _get_existing_columns(cursor) -> set:
    """Return the set of column names currently in the table."""
    cursor.execute(_DESCRIBE_QUERY)
    return {row[0] for row in cursor.fetchall()}

def _apply_missing_columns(cursor, existing: set) -> None:
    """Execute ALTER TABLE for each column not yet present."""
    for col, defn in _COLUMNS_TO_ADD:
        if col in existing:
            _logger.info("Skipped (already exists): %s", col)
            continue

        try:
            cursor.execute(_ALTER_QUERY_TMPL.format(table=_TABLE, col=col, defn=defn))
            _logger.info("Added column: %s", col)

        except Exception as e:
            # MySQL sometimes throws a duplicate error even if DESCRIBE missed it
            if _DUPLICATE_MARKER in str(e):
                _logger.info("Skipped (already exists): %s", col)
            else:
                raise

def _log_table_structure(cursor) -> None:
    """Log the current column layout of the table."""
    cursor.execute(_DESCRIBE_QUERY)
    _logger.info("Current '%s' table structure:", _TABLE)
    for col in cursor.fetchall():
        _logger.info("  %s: %s", col[0], col[1])

# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print(_SEPARATOR)
    print("Database Migration: Add Security Columns")
    print(_SEPARATOR)
    print()

    add_security_columns()

    print()
    print(_SEPARATOR)
    print("Migration completed!")
    print(_SEPARATOR)
