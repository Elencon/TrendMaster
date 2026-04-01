r"""
C:\Economy\Invest\TrendMaster\src\database\utilities\database_utils.py
Core database operation utilities.
"""

import logging

logger = logging.getLogger(__name__)

# Characters not allowed in table or column identifiers
_IDENTIFIER_BLACKLIST = {";", "'", '"', "\\", "\0", "\n", "\r", "`"}


def _safe_identifier(name: str) -> str:
    """
    Validate and backtick-quote a table or column identifier.

    Raises:
        ValueError: If the identifier contains disallowed characters.
    """
    if any(c in name for c in _IDENTIFIER_BLACKLIST):
        raise ValueError(f"Unsafe identifier: {name!r}")
    return f"`{name}`"


class DatabaseUtils:
    """Database operation utilities for SQL generation and execution."""

    @staticmethod
    def batch_execute(cursor, sql: str, data: list, batch_size: int = 1000) -> int:
        """
        Execute a parameterised SQL statement in batches.

        Args:
            cursor: Database cursor.
            sql: Parameterised SQL statement.
            data: List of parameter tuples.
            batch_size: Number of rows per batch (default: 1000).

        Returns:
            Total number of affected rows.
        """
        total_affected = 0
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            cursor.executemany(sql, batch)
            total_affected += max(0, cursor.rowcount)
        return total_affected

    @staticmethod
    def generate_insert_sql(
        table_name: str,
        sample_record: dict | list,
        ignore_duplicates: bool = True,
    ) -> str:
        """
        Generate a parameterised INSERT SQL statement from a sample record.

        Args:
            table_name: Target table name.
            sample_record: Record whose keys (if dict) or elements (if list) 
                           determine the column list.
            ignore_duplicates: Use INSERT IGNORE to skip duplicate rows.

        Returns:
            Parameterised INSERT SQL string.
        """
        table = _safe_identifier(table_name)

        # Support both dict (use keys) and list (use elements as column names)
        if isinstance(sample_record, dict):
            columns = [_safe_identifier(c) for c in sample_record.keys()]
        elif isinstance(sample_record, (list, tuple)):
            columns = [_safe_identifier(c) for c in sample_record]
        else:
            raise TypeError(
                f"sample_record must be dict or list/tuple, got {type(sample_record).__name__}"
            )

        placeholders = ", ".join(["%s"] * len(columns))
        insert_type = "INSERT IGNORE" if ignore_duplicates else "INSERT"

        return f"{insert_type} INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"

    @staticmethod
    def generate_update_sql(
        table_name: str,
        update_columns: list[str],
        key_columns: list[str],
    ) -> str:
        """
        Generate a parameterised UPDATE SQL statement.

        Args:
            table_name: Target table name.
            update_columns: Columns to set.
            key_columns: Columns for the WHERE clause.

        Returns:
            Parameterised UPDATE SQL string.
        """
        table = _safe_identifier(table_name)
        set_clause = ", ".join(
            f"{_safe_identifier(c)} = %s" for c in update_columns
        )
        where_clause = " AND ".join(
            f"{_safe_identifier(c)} = %s" for c in key_columns
        )
        return f"UPDATE {table} SET {set_clause} WHERE {where_clause}"

    @staticmethod
    def generate_upsert_sql(
        table_name: str,
        columns: list[str],
        update_columns: list[str],
    ) -> str:
        """
        Generate an INSERT ... ON DUPLICATE KEY UPDATE SQL statement.

        Args:
            table_name: Target table name.
            columns: All columns for the INSERT clause.
            update_columns: Columns to update on duplicate key.

        Returns:
            Parameterised UPSERT SQL string.
        """
        table = _safe_identifier(table_name)
        quoted_cols = [_safe_identifier(c) for c in columns]
        placeholders = ", ".join(["%s"] * len(columns))
        update_clause = ", ".join(
            f"{_safe_identifier(c)} = VALUES({_safe_identifier(c)})"
            for c in update_columns
        )
        return (
            f"INSERT INTO {table} ({', '.join(quoted_cols)}) VALUES ({placeholders}) "
            f"ON DUPLICATE KEY UPDATE {update_clause}"
        )

    @staticmethod
    def generate_delete_sql(
        table_name: str,
        condition_columns: list[str],
    ) -> str:
        """
        Generate a parameterised DELETE SQL statement.

        Args:
            table_name: Target table name.
            condition_columns: Columns for the WHERE clause.

        Returns:
            Parameterised DELETE SQL string.
        """
        table = _safe_identifier(table_name)
        where_clause = " AND ".join(
            f"{_safe_identifier(c)} = %s" for c in condition_columns
        )
        return f"DELETE FROM {table} WHERE {where_clause}"

    @staticmethod
    def records_to_tuples(records: list[dict], columns: list[str]) -> list[tuple]:
        """
        Convert a list of record dicts to a list of tuples for SQL execution.

        Args:
            records: List of record dictionaries.
            columns: Column names in the order expected by the SQL statement.

        Returns:
            List of value tuples.
        """
        return [tuple(record.get(col) for col in columns) for record in records]

    @staticmethod
    def table_exists(cursor, table_name: str) -> bool:
        """
        Check whether a table exists in the current database.

        Args:
            cursor: Database cursor.
            table_name: Table name to check.

        Returns:
            True if the table exists, False otherwise.
        """
        try:
            cursor.execute("SHOW TABLES LIKE %s", (table_name,))
            return cursor.fetchone() is not None
        except Exception as e:
            logger.error("Error checking table existence for %r: %s", table_name, e)
            return False

    @staticmethod
    def get_table_row_count(cursor, table_name: str) -> int:
        """
        Return the row count for a table.

        Args:
            cursor: Database cursor.
            table_name: Table name to count.

        Returns:
            Row count, or 0 on error.
        """
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {_safe_identifier(table_name)}")
            return cursor.fetchone()[0]
        except Exception as e:
            logger.error("Error getting row count for %r: %s", table_name, e)
            return 0

    @staticmethod
    def get_table_columns(cursor, table_name: str) -> list[str]:
        """
        Return column names for a table.

        Args:
            cursor: Database cursor.
            table_name: Table name to describe.

        Returns:
            List of column name strings, or empty list on error.
        """
        try:
            cursor.execute(f"DESCRIBE {_safe_identifier(table_name)}")
            return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error("Error getting columns for %r: %s", table_name, e)
            return []