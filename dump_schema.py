import sys
import os
from pathlib import Path

# Add src to sys.path
src_path = Path(__file__).parent / 'src'
sys.path.insert(0, str(src_path))

try:
    from database.connection_manager import DatabaseConnection
    from connect import config
    
    conn_mgr = DatabaseConnection(config)
    with conn_mgr.get_connection() as conn:
        if conn is None:
            print("Failed to connect to database")
            sys.exit(1)
            
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        
        print(f"Found {len(tables)} tables: {', '.join(tables)}")
        print("-" * 50)
        
        schema_info = {}
        for table in tables:
            cursor.execute(f"SHOW CREATE TABLE `{table}`")
            create_sql = cursor.fetchone()[1]
            
            cursor.execute(f"DESCRIBE `{table}`")
            columns = [row[0] for row in cursor.fetchall()]
            
            schema_info[table] = {
                'sql': create_sql,
                'columns': columns
            }
            
            print(f"Table: {table}")
            print(f"Columns: {columns}")
            print(f"SQL: {create_sql[:100]}...") # Just show beginning
            print("-" * 50)
            
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
