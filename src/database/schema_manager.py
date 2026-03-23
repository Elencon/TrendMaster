"""
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
    'brands':      "CREATE TABLE IF NOT EXISTS brands (brand_id INT PRIMARY KEY, brand_name VARCHAR(255) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",

    'categories':  "CREATE TABLE IF NOT EXISTS categories (category_id INT PRIMARY KEY, category_name VARCHAR(255) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",

    'stores':      """CREATE TABLE IF NOT EXISTS stores (
                        store_id  INT          PRIMARY KEY AUTO_INCREMENT,
                        name      VARCHAR(255) NOT NULL,
                        phone     VARCHAR(20),
                        email     VARCHAR(255),
                        street    VARCHAR(255),
                        city      VARCHAR(100),
                        state     VARCHAR(50),
                        zip_code  VARCHAR(20)
                      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

    # store_name  → store_id  (FK to stores)
    # manager_id stays a self-referential FK
    'staffs':      """CREATE TABLE IF NOT EXISTS staffs (
                        staff_id   INT          PRIMARY KEY AUTO_INCREMENT,
                        name       VARCHAR(100) NOT NULL,
                        last_name  VARCHAR(100) NOT NULL,
                        email      VARCHAR(255),
                        phone      VARCHAR(20),
                        active     BOOLEAN      DEFAULT TRUE,
                        store_id   INT,
                        street     VARCHAR(255),
                        manager_id INT,
                        FOREIGN KEY (store_id)   REFERENCES stores(store_id),
                        FOREIGN KEY (manager_id) REFERENCES staffs(staff_id)
                      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

    'products':    """CREATE TABLE IF NOT EXISTS products (
                        product_id   INT          PRIMARY KEY,
                        product_name VARCHAR(255) NOT NULL,
                        brand_id     INT,
                        category_id  INT,
                        model_year   INT,
                        list_price   DECIMAL(10, 2),
                        FOREIGN KEY (brand_id)    REFERENCES brands(brand_id),
                        FOREIGN KEY (category_id) REFERENCES categories(category_id)
                      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

    # store_name → store_id  (FK to stores)
    # PK updated to (store_id, product_id)
    'stocks':      """CREATE TABLE IF NOT EXISTS stocks (
                        store_id   INT NOT NULL,
                        product_id INT NOT NULL,
                        quantity   INT DEFAULT 0,
                        PRIMARY KEY (store_id, product_id),
                        FOREIGN KEY (store_id)   REFERENCES stores(store_id),
                        FOREIGN KEY (product_id) REFERENCES products(product_id)
                      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

    # store / staff_name → store_id / staff_id  (FK to stores / staffs)
    'orders':      """CREATE TABLE IF NOT EXISTS orders (
                        order_id          INT PRIMARY KEY,
                        customer_id       INT NOT NULL,
                        order_status      INT NOT NULL,
                        order_status_name VARCHAR(50),
                        order_date        DATE,
                        required_date     DATE,
                        shipped_date      DATE,
                        staff_id          INT,
                        store_id          INT,
                        FOREIGN KEY (staff_id) REFERENCES staffs(staff_id),
                        FOREIGN KEY (store_id) REFERENCES stores(store_id),
                        INDEX idx_customer_id  (customer_id),
                        INDEX idx_order_date   (order_date),
                        INDEX idx_order_status (order_status),
                        INDEX idx_store_id     (store_id)
                      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

    # Added FK declarations for order_id and product_id
    'order_items': """CREATE TABLE IF NOT EXISTS order_items (
                        item_id    INT            PRIMARY KEY,
                        order_id   INT            NOT NULL,
                        product_id INT            NOT NULL,
                        quantity   INT            NOT NULL DEFAULT 1,
                        list_price DECIMAL(10, 2) NOT NULL,
                        discount   DECIMAL(4,  2) DEFAULT 0.00,
                        FOREIGN KEY (order_id)   REFERENCES orders(order_id),
                        FOREIGN KEY (product_id) REFERENCES products(product_id),
                        INDEX idx_order_id   (order_id),
                        INDEX idx_product_id (product_id)
                      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

    'customers':   """CREATE TABLE IF NOT EXISTS customers (
                        customer_id INT          PRIMARY KEY,
                        first_name  VARCHAR(100) NOT NULL,
                        last_name   VARCHAR(100) NOT NULL,
                        email       VARCHAR(255) UNIQUE,
                        phone       VARCHAR(20),
                        street      VARCHAR(255),
                        city        VARCHAR(100),
                        state       VARCHAR(50),
                        zip_code    VARCHAR(20),
                        INDEX idx_email (email),
                        INDEX idx_state (state),
                        INDEX idx_city  (city)
                      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

    'users':       """CREATE TABLE IF NOT EXISTS users (
                        user_id                INT      PRIMARY KEY AUTO_INCREMENT,
                        staff_id               INT      UNIQUE,
                        username               VARCHAR(50)  UNIQUE NOT NULL,
                        password_hash          VARCHAR(255) NOT NULL,
                        role                   ENUM('Employee', 'Manager', 'Administrator') NOT NULL DEFAULT 'Employee',
                        last_login             DATETIME,
                        created_at             DATETIME DEFAULT CURRENT_TIMESTAMP,
                        active                 BOOLEAN  DEFAULT TRUE,
                        failed_login_attempts  INT      DEFAULT 0,
                        last_failed_login      DATETIME,
                        account_locked_until   DATETIME,
                        password_last_changed  DATETIME DEFAULT CURRENT_TIMESTAMP,
                        must_change_password   BOOLEAN  DEFAULT FALSE,
                        two_factor_enabled     BOOLEAN  DEFAULT FALSE,
                        two_factor_secret      VARCHAR(32),
                        backup_codes           TEXT,
                        FOREIGN KEY (staff_id) REFERENCES staffs(staff_id) ON DELETE SET NULL,
                        INDEX idx_username (username),
                        INDEX idx_role     (role)
                      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
}

TABLE_COLUMNS: Dict[str, List[str]] = {
    'brands':      ['brand_id', 'brand_name'],
    'categories':  ['category_id', 'category_name'],
    'stores':      ['store_id', 'name', 'phone', 'email', 'street', 'city', 'state', 'zip_code'],
    'staffs':      ['staff_id', 'name', 'last_name', 'email', 'phone', 'active', 'store_id', 'street', 'manager_id'],
    'products':    ['product_id', 'product_name', 'brand_id', 'category_id', 'model_year', 'list_price'],
    'stocks':      ['store_id', 'product_id', 'quantity'],
    'orders':      ['order_id', 'customer_id', 'order_status', 'order_status_name',
                    'order_date', 'required_date', 'shipped_date', 'staff_id', 'store_id'],
    'order_items': ['item_id', 'order_id', 'product_id', 'quantity', 'list_price', 'discount'],
    'customers':   ['customer_id', 'first_name', 'last_name', 'email', 'phone',
                    'street', 'city', 'state', 'zip_code'],
    'users':       ['user_id', 'staff_id', 'username', 'password_hash', 'role',
                    'last_login', 'created_at', 'active', 'failed_login_attempts',
                    'last_failed_login', 'account_locked_until', 'password_last_changed',
                    'must_change_password', 'two_factor_enabled', 'two_factor_secret', 'backup_codes'],
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


_validate_column_registry()


class SchemaManager:
    """Manages database table schemas using data-driven definitions."""

    def __init__(self, db_connection):
        self.db_connection = db_connection

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

        Returns True on success, False otherwise.
        """
        schema = self.get_schema(table_name)
        if not schema:
            logger.error("No schema defined for table: %s", table_name)
            return False

        try:
            with self.db_connection.get_connection() as conn:
                if conn is None:
                    logger.error("Database connection failed")
                    return False

                with conn.cursor() as cursor:
                    cursor.execute(schema)
                conn.commit()
                logger.info("Table '%s' created successfully", table_name)
                return True

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
        successes = sum(1 for t in order if self.create_table(t))
        logger.info("Created %d/%d tables", successes, len(order))
        return successes == len(order)

    # ------------------------------------------------------------------
    # Live database checks
    # ------------------------------------------------------------------

    def table_exists(self, table_name: str) -> bool:
        """Return True if *table_name* exists in the connected database."""
        try:
            with self.db_connection.get_connection() as conn:
                if conn is None:
                    return False

                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT 1 FROM information_schema.tables "
                        "WHERE table_name = %s",
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