from __future__ import annotations
from typing import Dict, Tuple, Optional, Any, List
import logging

from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine, Inspector

from schema_manager import SchemaManager


class IntegrityCheck:
    """
    Performs comprehensive database integrity checks using SchemaManager.

    Checks performed:
    - Database connection
    - All required tables exist
    - All required columns exist in each table
    - Primary key constraints exist on all tables

    Returns a detailed report with success status and error details.
    """

    def __init__(self, engine: Engine, schema_manager: SchemaManager):
        """
        :param engine: SQLAlchemy engine instance
        :param schema_manager: SchemaManager instance providing schema metadata
        """
        self.engine = engine
        self.schema_manager = schema_manager
        self.logger = logging.getLogger("etl.integrity")

    # ---------------------------------------------------------
    # Public API
    # ---------------------------------------------------------
    def run(self) -> Dict[str, Any]:
        """
        Run all integrity checks and return a detailed structured report.

        Returns:
            {
                "connection_ok": bool,
                "tables_ok": bool,
                "columns_ok": bool,
                "constraints_ok": bool,
                "success": bool,                    # Overall success
                "details": {
                    "connection": str | None,
                    "tables": str | None,
                    "columns": str | None,
                    "constraints": str | None,
                }
            }
        """
        report: Dict[str, Any] = {
            "connection_ok": False,
            "tables_ok": False,
            "columns_ok": False,
            "constraints_ok": False,
            "success": False,
            "details": {
                "connection": None,
                "tables": None,
                "columns": None,
                "constraints": None,
            }
        }

        # 1. Connection check (critical - early exit)
        ok, msg = self.check_connection()
        report["connection_ok"] = ok
        report["details"]["connection"] = msg

        if not ok:
            self.logger.error("Integrity check aborted due to connection failure")
            return report

        # Create inspector once and reuse for efficiency
        inspector: Inspector = inspect(self.engine)

        # 2. Tables check
        ok, msg = self.check_tables(inspector)
        report["tables_ok"] = ok
        report["details"]["tables"] = msg

        # 3. Columns check
        ok, msg = self.check_columns(inspector)
        report["columns_ok"] = ok
        report["details"]["columns"] = msg

        # 4. Constraints check
        ok, msg = self.check_constraints(inspector)
        report["constraints_ok"] = ok
        report["details"]["constraints"] = msg

        # Overall success
        report["success"] = all([
            report["connection_ok"],
            report["tables_ok"],
            report["columns_ok"],
            report["constraints_ok"]
        ])

        if report["success"]:
            self.logger.info("✅ All integrity checks passed successfully")
        else:
            self.logger.warning("⚠️ Integrity check completed with one or more failures")

        return report

    # ---------------------------------------------------------
    # Individual checks
    # ---------------------------------------------------------
    def check_connection(self) -> Tuple[bool, Optional[str]]:
        """Verify basic database connectivity."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self.logger.info("Database connection OK")
            return True, None
        except SQLAlchemyError as e:
            msg = f"Connection failed: {e}"
            self.logger.exception("Database connection failed")
            return False, msg

    def check_tables(self, inspector: Inspector) -> Tuple[bool, Optional[str]]:
        """Check that all required tables exist in the database."""
        existing = set(inspector.get_table_names())
        required = set(self.schema_manager.get_all_table_names())

        if not required:
            self.logger.info("No tables required by SchemaManager")
            return True, None

        missing = required - existing
        if missing:
            msg = f"Missing tables: {sorted(missing)}"
            self.logger.error(msg)
            return False, msg

        self.logger.info(f"All {len(required)} required tables exist")
        return True, None

    def check_columns(self, inspector: Inspector) -> Tuple[bool, Optional[str]]:
        """Check that all required columns exist in their respective tables."""
        all_ok = True
        errors: List[str] = []

        for table in self.schema_manager.get_all_table_names():
            required_cols = set(self.schema_manager.get_table_columns(table))

            try:
                existing_cols = {col["name"] for col in inspector.get_columns(table)}
            except SQLAlchemyError as e:
                msg = f"Cannot inspect columns for table '{table}': {e}"
                self.logger.error(msg)
                errors.append(msg)
                all_ok = False
                continue

            missing = required_cols - existing_cols
            if missing:
                msg = f"Table '{table}' missing columns: {sorted(missing)}"
                self.logger.error(msg)
                errors.append(msg)
                all_ok = False

        if all_ok:
            self.logger.info("All required columns exist in all tables")
            return True, None

        return False, " | ".join(errors)

    def check_constraints(self, inspector: Inspector) -> Tuple[bool, Optional[str]]:
        """Check that primary key constraints exist on all required tables."""
        all_ok = True
        errors: List[str] = []

        for table in self.schema_manager.get_all_table_names():
            try:
                pk = inspector.get_pk_constraint(table)
            except SQLAlchemyError as e:
                msg = f"Cannot inspect constraints for table '{table}': {e}"
                self.logger.error(msg)
                errors.append(msg)
                all_ok = False
                continue

            if not pk or not pk.get("constrained_columns"):
                msg = f"Table '{table}' missing primary key"
                self.logger.error(msg)
                errors.append(msg)
                all_ok = False

        if all_ok:
            self.logger.info("All primary key constraints are present")
            return True, None

        return False, " | ".join(errors)