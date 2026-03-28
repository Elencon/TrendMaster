r"""
C:\Economy\Invest\TrendMaster\src\database\schema_manager.py
Compact schema manager - handles table schema definitions and creation using utilities.
"""

import logging
import re
from typing import Dict, List, Optional

# ---------------------------------------------------------------------------
# Schema definitions
#
# Key changes vs. previous version:
#   - staffs:      store_name (VARCHAR, no FK) → store_id INT FK → stores(store_id)
#   - stocks:      store_name (VARCHAR, no FK) → store_id INT FK → stores(store_id)
#                  PRIMARY KEY is now (store_id, product_id)
#   - orders:      store (VARCHAR, no FK)      → store_id INT FK → stores(store_id)
#                  staff_name (VARCHAR, no FK) → staff_id INT FK → staffs(staff_id)
#   - order_items: order_id and product_id now carry explicit FK declarations
# ---------------------------------------------------------------------------

SCHEMA_DEFINITIONS = {
    'daily_prices': """CREATE TABLE `daily_prices` (
  `price_id` bigint NOT NULL AUTO_INCREMENT,
  `ticker_id` int NOT NULL,
  `trade_date` date NOT NULL,
  `open_price` decimal(14,4) DEFAULT NULL,
  `high_price` decimal(14,4) DEFAULT NULL,
  `low_price` decimal(14,4) DEFAULT NULL,
  `close_price` decimal(14,4) DEFAULT NULL,
  `adj_close` decimal(14,4) DEFAULT NULL,
  `daily_yield_pct` decimal(8,4) DEFAULT NULL,
  `volume` bigint DEFAULT NULL,
  PRIMARY KEY (`price_id`),
  UNIQUE KEY `unique_ticker_date` (`ticker_id`,`trade_date`),
  KEY `idx_trade_date` (`trade_date`),
  CONSTRAINT `daily_prices_ibfk_1` FOREIGN KEY (`ticker_id`) REFERENCES `tickers` (`ticker_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci""",

    'portfolio_holdings': """CREATE TABLE `portfolio_holdings` (
  `holding_id` int NOT NULL AUTO_INCREMENT,
  `portfolio_id` int NOT NULL,
  `ticker_id` int NOT NULL,
  `quantity` decimal(14,4) NOT NULL DEFAULT '0.0000',
  `average_buy_price` decimal(14,4) NOT NULL DEFAULT '0.0000',
  `last_updated` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`holding_id`),
  UNIQUE KEY `unique_portfolio_ticker` (`portfolio_id`,`ticker_id`),
  KEY `ticker_id` (`ticker_id`),
  CONSTRAINT `portfolio_holdings_ibfk_1` FOREIGN KEY (`portfolio_id`) REFERENCES `portfolios` (`portfolio_id`) ON DELETE CASCADE,
  CONSTRAINT `portfolio_holdings_ibfk_2` FOREIGN KEY (`ticker_id`) REFERENCES `tickers` (`ticker_id`) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci""",

    'portfolios': """CREATE TABLE `portfolios` (
  `portfolio_id` int NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `name` varchar(100) NOT NULL,
  `description` text,
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`portfolio_id`),
  KEY `user_id` (`user_id`),
  CONSTRAINT `portfolios_ibfk_1` FOREIGN KEY (`user_id`) REFERENCES `users` (`user_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci""",

    'tickers': """CREATE TABLE `tickers` (
  `ticker_id` int NOT NULL AUTO_INCREMENT,
  `symbol` varchar(20) NOT NULL,
  `name` varchar(255) NOT NULL,
  `exchange` varchar(50) DEFAULT NULL,
  `sector` varchar(100) DEFAULT NULL,
  `industry` varchar(100) DEFAULT NULL,
  `asset_class` varchar(50) DEFAULT 'Equity',
  `currency` varchar(10) DEFAULT 'USD',
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`ticker_id`),
  UNIQUE KEY `symbol` (`symbol`),
  KEY `idx_symbol` (`symbol`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci""",

    'users': """CREATE TABLE `users` (
  `user_id` int NOT NULL AUTO_INCREMENT,
  `username` varchar(50) NOT NULL,
  `email` varchar(100) DEFAULT NULL,
  `password_hash` varchar(255) NOT NULL,
  `role` enum('Employee','Manager','Administrator','User') NOT NULL DEFAULT 'User',
  `last_login` datetime DEFAULT NULL,
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  `active` tinyint(1) DEFAULT '1',
  `failed_login_attempts` int DEFAULT '0',
  `last_failed_login` datetime DEFAULT NULL,
  `account_locked_until` datetime DEFAULT NULL,
  `password_last_changed` datetime DEFAULT CURRENT_TIMESTAMP,
  `must_change_password` tinyint(1) DEFAULT '0',
  `two_factor_enabled` tinyint(1) DEFAULT '0',
  `two_factor_secret` varchar(32) DEFAULT NULL,
  `backup_codes` text,
  PRIMARY KEY (`user_id`),
  UNIQUE KEY `username` (`username`),
  UNIQUE KEY `email` (`email`),
  KEY `idx_username` (`username`),
  KEY `idx_role` (`role`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci""",

}

TABLE_COLUMNS: Dict[str, List[str]] = {
    'daily_prices': ['price_id', 'ticker_id', 'trade_date', 'open_price', 'high_price', 'low_price', 'close_price', 'adj_close', 'daily_yield_pct', 'volume'],
    'portfolio_holdings': ['holding_id', 'portfolio_id', 'ticker_id', 'quantity', 'average_buy_price', 'last_updated'],
    'portfolios': ['portfolio_id', 'user_id', 'name', 'description', 'created_at'],
    'tickers': ['ticker_id', 'symbol', 'name', 'exchange', 'sector', 'industry', 'asset_class', 'currency', 'created_at'],
    'users': ['user_id', 'username', 'email', 'password_hash', 'role', 'last_login', 'created_at', 'active', 'failed_login_attempts', 'last_failed_login',
    'account_locked_until', 'password_last_changed', 'must_change_password', 'two_factor_enabled', 'two_factor_secret', 'backup_codes'],
}

# Foreign-key-safe creation order
# Constraints that drive this ordering:
#   stores      → (no deps)
#   staffs      → stores
#   customers   → (no deps)           must precede orders (orders.customer_id)
#   products    → brands, categories  must precede stocks and order_items
#   stocks      → stores, products
#   orders      → stores, staffs, customers
#   order_items → orders, products
#   users       → staffs
DEFAULT_TABLE_ORDER: List[str] = [
    'brands', 'categories', 'stores', 'staffs',
    'customers', 'products', 'stocks', 'orders', 'order_items', 'users',
]

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Import-time consistency check
# ---------------------------------------------------------------------------

def _extract_columns(sql: str) -> set:
    """
    Parse column names from a CREATE TABLE statement.

    Finds the outermost parenthesised block using a depth counter (handles
    nested parentheses in types like DECIMAL(10,2)), then pulls the first
    token from each comma-separated clause that is a real column definition —
    skipping table-level clauses such as PRIMARY KEY, FOREIGN KEY, INDEX, etc.
    """
    try:
        start = sql.index('(')
    except ValueError:
        return set()

    depth, end = 0, start
    for i, ch in enumerate(sql[start:], start):
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
            if depth == 0:
                end = i
                break

    inner_text = sql[start + 1:end]

    skip = re.compile(
        r'^\s*('
        r'PRIMARY\s+KEY|FOREIGN\s+KEY|UNIQUE\s+KEY|UNIQUE\s+INDEX'
        r'|INDEX|CHECK|CONSTRAINT|ENGINE|DEFAULT\s+CHARSET'
        r')\b',
        re.IGNORECASE,
    )

    columns = set()
    for line in inner_text.splitlines():
        line = line.strip().rstrip(',')
        if not line or skip.match(line):
            continue
        token = re.split(r'\s+', line)[0].strip('`\'"')
        if token:
            columns.add(token)

    return columns


def _validate_column_registry() -> None:
    """
    Raise AssertionError at import time if TABLE_COLUMNS and SCHEMA_DEFINITIONS
    are out of sync — either a table is missing from one dict, or its column
    list differs from what the SQL actually declares.
    """
    schema_tables = set(SCHEMA_DEFINITIONS)
    column_tables = set(TABLE_COLUMNS)

    only_in_schema  = schema_tables - column_tables
    only_in_columns = column_tables - schema_tables

    errors: List[str] = []

    if only_in_schema:
        errors.append(
            f"In SCHEMA_DEFINITIONS but missing from TABLE_COLUMNS: {sorted(only_in_schema)}"
        )
    if only_in_columns:
        errors.append(
            f"In TABLE_COLUMNS but missing from SCHEMA_DEFINITIONS: {sorted(only_in_columns)}"
        )

    for table in sorted(schema_tables & column_tables):
        declared   = _extract_columns(SCHEMA_DEFINITIONS[table])
        registered = set(TABLE_COLUMNS[table])
        if declared != registered:
            parts = []
            missing = declared - registered
            extra   = registered - declared
            if missing:
                parts.append(f"in SQL but not TABLE_COLUMNS: {sorted(missing)}")
            if extra:
                parts.append(f"in TABLE_COLUMNS but not SQL: {sorted(extra)}")
            errors.append(f"  '{table}' column mismatch — {'; '.join(parts)}")

    if errors:
        raise AssertionError(
            "TABLE_COLUMNS is out of sync with SCHEMA_DEFINITIONS:\n"
            + "\n".join(errors)
        )


# _validate_column_registry()


class SchemaManager:
    """Manages database table schemas using data-driven definitions."""

    def __init__(self, db_connection):
        # Protected attribute: internal use only
        self._db_connection = db_connection

    # ------------------------------------------------------------------
    # Schema introspection
    # ------------------------------------------------------------------

    def get_schema(self, table_name: str) -> Optional[str]:
        """Return the CREATE TABLE SQL for *table_name*, or None if unknown."""
        return SCHEMA_DEFINITIONS.get(table_name)

    def get_table_columns(self, table_name: str) -> List[str]:
        """Return the expected column list for *table_name* (used for validation)."""
        return TABLE_COLUMNS.get(table_name, [])

    def get_all_table_names(self) -> List[str]:
        """Return all defined table names."""
        return list(SCHEMA_DEFINITIONS.keys())

    # ------------------------------------------------------------------
    # DDL helpers
    # ------------------------------------------------------------------

    def create_table(self, table_name: str) -> bool:
        """
        Create *table_name* in the database.

        Returns True on success (including if the table already exists),
        False only on real errors.
        """
        schema = self.get_schema(table_name)
        if not schema:
            logger.error("No schema defined for table: %s", table_name)
            return False

        try:
            with self._db_connection.get_connection() as conn:
                if conn is None:
                    logger.error("Database connection failed")
                    return False

                with conn.cursor() as cursor:
                    try:
                        cursor.execute(schema)
                        conn.commit()
                        logger.info("Table '%s' created successfully", table_name)
                        return True

                    except Exception as exc:
                        msg = str(exc).lower()

                        # MySQL / MariaDB variations
                        if "already exists" in msg or "exists" in msg:
                            logger.info(
                                "Table '%s' already exists — skipping creation",
                                table_name,
                            )
                            return True

                        raise  # real error

        except Exception as exc:
            logger.error("Error creating table %s: %s", table_name, exc)
            return False

    def create_all_tables(self, table_order: Optional[List[str]] = None) -> bool:
        """
        Create all tables in foreign-key-safe order.

        Args:
            table_order: Override the default creation order when supplied.

        Returns:
            True only if every table was created successfully.
        """
        order = table_order or DEFAULT_TABLE_ORDER
        successes = 0

        for table in order:
            if self.create_table(table):
                successes += 1
            else:
                logger.error("Failed to create table '%s'", table)

        logger.info("Created %d/%d tables", successes, len(order))
        return successes == len(order)

    # ------------------------------------------------------------------
    # Live database checks
    # ------------------------------------------------------------------

    def table_exists(self, table_name: str) -> bool:
        """Return True if *table_name* exists in the connected database."""
        try:
            with self._db_connection.get_connection() as conn:
                if conn is None:
                    return False

                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_name = %s
                        """,
                        (table_name,),
                    )
                    return cursor.fetchone() is not None

        except Exception as exc:
            logger.error("Error checking existence of table %s: %s", table_name, exc)
            return False


# ---------------------------------------------------------------------------
# Factory helper (backward-compatible)
# ---------------------------------------------------------------------------

def create_schema_manager(db_connection) -> SchemaManager:
    """Instantiate and return a :class:`SchemaManager`."""
    return SchemaManager(db_connection)