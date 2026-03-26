r"""
C:\Economy\Invest\TrendMaster\src\database\utilities\schema_sync.py
python -m src.database.utilities.schema_sync
Utility to sync schema_manager.py with the live MySQL database.
"""

import re
import sys
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Also add src to sys.path (some modules might expect it)
SRC_PATH = PROJECT_ROOT / 'src'
sys.path.insert(0, str(SRC_PATH))

try:
    from src.database.connection_manager import DatabaseConnection
    from src.database import config
except ImportError:
    # Fallback for different directory structures if needed
    try:
        from database.connection_manager import DatabaseConnection
        from database import config
    except ImportError as e:
        print(f"Error: Could not import connection modules: {e}")
        sys.exit(1)


def fetch_live_schema():
    """Fetch all table structures and columns from the live database."""
    print("Connecting to database...")
    conn_mgr = DatabaseConnection(config)

    schema_definitions = {}
    table_columns = {}

    with conn_mgr.get_connection() as conn:
        if conn is None:
            print("Failed to get database connection")
            return None, None

        cursor = conn.cursor()

        # Get all tables
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        print(f"Found {len(tables)} tables: {', '.join(tables)}")

        for table in tables:
            # Get CREATE TABLE SQL
            cursor.execute(f"SHOW CREATE TABLE `{table}`")
            create_sql = cursor.fetchone()[1]
            # Clean up SQL slightly (MySQL adds auto_increment value which we don't want in template)
            create_sql = re.sub(r'AUTO_INCREMENT=\d+\s+', '', create_sql)
            schema_definitions[table] = create_sql

            # Get Columns
            cursor.execute(f"SHOW COLUMNS FROM `{table}`")
            columns = [row[0] for row in cursor.fetchall()]
            table_columns[table] = columns

    return schema_definitions, table_columns


def update_schema_manager(schema_definitions, table_columns):
    """Update src/database/schema_manager.py with new definitions."""
    schema_manager_path = Path(PROJECT_ROOT) / 'src' / 'database' / 'schema_manager.py'
    if not schema_manager_path.exists():
        schema_manager_path = Path(PROJECT_ROOT) / 'database' / 'schema_manager.py'

    if not schema_manager_path.exists():
        print(f"Error: Could not find schema_manager.py at {schema_manager_path}")
        return False

    print(f"Updating {schema_manager_path}...")
    content = schema_manager_path.read_text(encoding='utf-8')

    # Update SCHEMA_DEFINITIONS
    # We find the start of the dict and the matching closing brace
    new_schema_dict = "SCHEMA_DEFINITIONS = {\n"
    for table, sql in schema_definitions.items():
        # Escape single quotes for SQL string
        #escaped_sql = sql.replace("'", "\\'") ????
        if "\n" in sql:
            new_schema_dict += f"    '{table}': \"\"\"{sql}\"\"\",\n\n"
        else:
            new_schema_dict += f"    '{table}': '{sql}',\n"
    new_schema_dict += "}"

    content = re.sub(
        r'SCHEMA_DEFINITIONS\s*=\s*\{.*?\}\n\}' if 'order_items' in content else r'SCHEMA_DEFINITIONS\s*=\s*\{.*?\}',
        new_schema_dict,
        content,
        flags=re.DOTALL
    )

    # Actually, a more precise way to find the dict
    content = re.sub(
        r'SCHEMA_DEFINITIONS\s*=\s*\{.*?\n\}',
        new_schema_dict,
        content,
        flags=re.DOTALL
    )

    # Update TABLE_COLUMNS
    new_columns_dict = "TABLE_COLUMNS: Dict[str, List[str]] = {\n"
    for table, columns in table_columns.items():
        cols_str = ", ".join([f"'{c}'" for c in columns])
        new_columns_dict += f"    '{table}': [{cols_str}],\n"
    new_columns_dict += "}"

    content = re.sub(
        r'TABLE_COLUMNS.*?\s*=\s*\{.*?\n\}',
        new_columns_dict,
        content,
        flags=re.DOTALL
    )

    schema_manager_path.write_text(content, encoding='utf-8')
    print("Unlocking file...")
    print("Schema Manager updated successfully!")
    return True


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Sync schema manager with live database")
    parser.add_argument("--dry-run", action="store_true", help="Print changes without writing to file")
    args = parser.parse_args()

    defs, cols = fetch_live_schema()
    if defs and cols:
        if args.dry_run:
            print("\nPROPOSED SCHEMA_DEFINITIONS:")
            for t, s in defs.items():
                print(f"  {t}: {s[:100]}...")
            print("\nPROPOSED TABLE_COLUMNS:")
            for t, c in cols.items():
                print(f"  {t}: {c}")
        else:
            update_schema_manager(defs, cols)
