import os
import csv
from datetime import datetime
from sqlalchemy import create_engine, text
from .schema_manager import SchemaManager
from .config import load_config


def export_table_to_csv(engine, table, path):
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM `{table}`"))
        rows = result.fetchall()
        columns = result.keys()

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)


def run_backup():
    cfg = load_config()
    engine = create_engine(cfg["db_url"])
    schema_manager = SchemaManager(None)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    backup_dir = os.path.join(cfg["backup_dir"], timestamp)
    os.makedirs(backup_dir, exist_ok=True)

    tables_dir = os.path.join(backup_dir, "tables")
    os.makedirs(tables_dir, exist_ok=True)

    for table in schema_manager.get_all_table_names():
        export_table_to_csv(engine, table, os.path.join(tables_dir, f"{table}.csv"))

    return backup_dir