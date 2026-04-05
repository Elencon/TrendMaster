"""
test_integrity_db.py
Run a visual database integrity check using Rich.
"""

import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

from rich.console import Console
from rich.table import Table
from rich.progress import track

from sqlalchemy import create_engine

from src.database.integrity_check import IntegrityCheck
from src.database.schema_manager import SchemaManager


# ---------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------
load_dotenv()


def build_connection_url() -> str:
    """Build MySQL connection URL from environment variables."""

    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "3306")
    dbname = os.getenv("DB_NAME")

    if not all([user, password, dbname]):
        raise ValueError("Missing required database environment variables")

    # Encode password in case it contains special characters
    password = quote_plus(password)

    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{dbname}"


def run_integrity_test():
    console = Console()

    console.print("[bold cyan]Running Database Integrity Check...[/bold cyan]\n")

    steps = [
        "Creating engine",
        "Initializing SchemaManager",
        "Initializing IntegrityCheck",
        "Running checks",
        "Preparing report",
    ]

    report = {}

    for step in track(steps, description="Processing..."):
        if step == "Creating engine":
            connection_url = build_connection_url()
            engine = create_engine(connection_url)

        elif step == "Initializing SchemaManager":
            schema_manager = SchemaManager(db_connection=None)

        elif step == "Initializing IntegrityCheck":
            checker = IntegrityCheck(engine, schema_manager)

        elif step == "Running checks":
            report = checker.run()

        elif step == "Preparing report":
            pass

    # -----------------------------------------------------
    # Display results table
    # -----------------------------------------------------
    table = Table(title="Database Integrity Check Results")

    table.add_column("Check", style="cyan", no_wrap=True)
    table.add_column("Status", style="magenta")

    for key, value in report.items():
        status = "[green]OK[/green]" if value else "[red]FAIL[/red]"
        table.add_row(key, status)

    console.print("\n")
    console.print(table)
    console.print("\n")

    # Final summary
    if all(report.values()):
        console.print("[bold green]Integrity Check PASSED[/bold green]")
    else:
        console.print("[bold red]Integrity Check FAILED[/bold red]")


if __name__ == "__main__":
    run_integrity_test()