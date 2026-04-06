from sqlalchemy import create_engine

from .integrity_check import IntegrityCheck
from .schema_manager import SchemaManager
from .config import load_config
from .alerts import send_alert
from .backup import run_backup
from .state import save_last_run, save_last_backup


def run_integrity_only() -> dict:
    """
    Run only the database integrity check.
    Returns the full integrity report.
    """
    cfg = load_config()

    engine = create_engine(
        cfg["db_url"],
        pool_pre_ping=True,
    )

    schema_manager = SchemaManager(db_connection=None)

    checker = IntegrityCheck(
        engine=engine,
        schema_manager=schema_manager,
        # schema="public",      # Uncomment and set if you use a specific schema
        fail_fast=False,
    )

    report = checker.run()

    # Save result
    save_last_run("integrity", report)

    # Send alert only if failed
    if not report.get("success", False):
        error_summary = _build_error_summary(report)
        send_alert(
            title="Database Integrity Check Failed",
            message=f"Integrity check failed.\n\n{error_summary}"
        )
    # else:  # Optional - you can remove this empty else completely

    return report


def run_integrity_and_backup() -> dict:
    """
    Run integrity check first, then backup only if it passes.
    """
    report = run_integrity_only()

    if report.get("success", False):
        try:
            backup_path = run_backup()
            state.save_last_backup(backup_path)
        except Exception as e:
            send_alert(
                title="Backup Failed",
                message=f"Integrity passed but backup failed.\nError: {e}"
            )
    else:
        send_alert(
            title="Backup Skipped",
            message="Integrity check failed → backup was aborted for safety."
        )

    return report


def _build_error_summary(report: dict) -> str:
    """Create a clean summary of what went wrong."""
    failures = []

    if not report.get("connection_ok"):
        failures.append("• Database connection failed")
    if not report.get("tables_ok"):
        details = report.get("details", {}).get("tables", {})
        missing = details.get("missing_tables", []) if isinstance(details, dict) else []
        if missing:
            failures.append(f"• Missing tables: {missing}")
        else:
            failures.append("• Tables check failed")
    if not report.get("columns_ok"):
        failures.append("• Column issues detected")
    if not report.get("constraints_ok"):
        failures.append("• Primary key constraints missing")
    if not report.get("foreign_keys_ok"):
        failures.append("• Foreign key issues")
    if not report.get("indexes_ok"):
        failures.append("• Index issues")

    return "\n".join(failures) if failures else "Unknown integrity failure"