from __future__ import annotations

import logging
import time
from typing import Dict, Tuple, Optional, Any, List, Set

from sqlalchemy import inspect
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine, Inspector

from .schema_manager import SchemaManager


class IntegrityCheck:
    """
    Production-grade database integrity checker.

    Checks:
    - Connection
    - Tables existence
    - Columns existence
    - Primary keys
    - Foreign keys (inspection-level)
    - Indexes (inspection-level)
    """

    def __init__(
        self,
        engine: Engine,
        schema_manager: SchemaManager,
        schema: Optional[str] = None,
        fail_fast: bool = False,
    ):
        self.engine = engine
        self.schema_manager = schema_manager
        self.schema = schema
        self.fail_fast = fail_fast
        self.logger = logging.getLogger("etl.integrity")

    # ---------------------------------------------------------
    # Public API
    # ---------------------------------------------------------
    def run(self) -> Dict[str, Any]:
        start_time = time.time()

        report: Dict[str, Any] = {
            "connection_ok": False,
            "tables_ok": False,
            "columns_ok": False,
            "constraints_ok": False,
            "foreign_keys_ok": False,
            "indexes_ok": False,
            "success": False,
            "duration_sec": None,
            "skipped_checks": [],
            "details": {
                "connection": None,
                "tables": None,
                "columns": None,
                "constraints": None,
                "foreign_keys": None,
                "indexes": None,
            },
        }

        # 1. Connection
        ok, msg = self.check_connection()
        report["connection_ok"] = ok
        report["details"]["connection"] = msg

        if not ok:
            self.logger.error("Integrity check aborted: connection failure")
            return self._finalize(report, start_time)

        inspector: Inspector = inspect(self.engine)
        tables = list(self.schema_manager.get_all_table_names())

        # 2. Tables
        ok, msg, existing_tables = self.check_tables(inspector, tables)
        report["tables_ok"] = ok
        report["details"]["tables"] = msg

        if not ok:
            self.logger.warning("Skipping deeper checks due to missing tables")
            report["skipped_checks"] = ["columns", "constraints", "foreign_keys", "indexes"]
            return self._finalize(report, start_time)

        # Cache schema_manager calls (micro-optimization)
        table_columns_map = {
            t: set(self.schema_manager.get_table_columns(t))
            for t in tables
        }

        # 3–6. Deeper checks
        checks = [
            ("columns_ok", "columns", self.check_columns),
            ("constraints_ok", "constraints", self.check_constraints),
            ("foreign_keys_ok", "foreign_keys", self.check_foreign_keys),
            ("indexes_ok", "indexes", self.check_indexes),
        ]

        for key, name, func in checks:
            ok, msg = func(inspector, tables, existing_tables, table_columns_map)
            report[key] = ok
            report["details"][name] = msg

            if not ok and self.fail_fast:
                remaining = [n for _, n, _ in checks[checks.index((key, name, func)) + 1:]]
                report["skipped_checks"].extend(remaining)
                self.logger.warning(f"Fail-fast triggered, skipping checks: {remaining}")
                break

        return self._finalize(report, start_time)

    def _finalize(self, report: Dict[str, Any], start_time: float) -> Dict[str, Any]:
        report["duration_sec"] = round(time.time() - start_time, 3)

        report["success"] = all([
            report["connection_ok"],
            report["tables_ok"],
            report["columns_ok"],
            report["constraints_ok"],
            report["foreign_keys_ok"],
            report["indexes_ok"],
        ])

        if report["success"]:
            self.logger.info("✅ All integrity checks passed")
        else:
            self.logger.warning("⚠️ Integrity check completed with issues")

        return report

    # ---------------------------------------------------------
    # Checks
    # ---------------------------------------------------------
    def check_connection(self) -> Tuple[bool, Optional[str]]:
        try:
            with self.engine.connect() as conn:
                conn.exec_driver_sql("SELECT 1")
            self.logger.info("Database connection OK")
            return True, None
        except SQLAlchemyError as e:
            self.logger.exception("Connection failed")
            return False, f"Connection failed: {e}"

    def check_tables(
        self,
        inspector: Inspector,
        required_tables: List[str],
    ) -> Tuple[bool, Dict[str, Any], Set[str]]:

        existing = set(inspector.get_table_names(schema=self.schema))
        required = set(required_tables)

        missing = sorted(required - existing)
        extra = sorted(existing - required)

        msg = {
            "missing_tables": missing,
            "extra_tables": extra,
        }

        if missing:
            self.logger.error(f"Missing tables: {missing}")
            return False, msg, existing

        self.logger.info(f"All {len(required)} required tables exist")
        return True, msg, existing

    def check_columns(
        self,
        inspector: Inspector,
        tables: List[str],
        existing_tables: Set[str],
        table_columns_map: Dict[str, Set[str]],
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:

        errors: Dict[str, Any] = {}

        for table in tables:
            if table not in existing_tables:
                continue

            try:
                existing_cols = {
                    col["name"]
                    for col in inspector.get_columns(table, schema=self.schema)
                }
            except SQLAlchemyError as e:
                errors[table] = {"error": str(e)}
                continue

            missing = sorted(table_columns_map[table] - existing_cols)

            if missing:
                errors[table] = {"missing_columns": missing}
                if self.fail_fast:
                    return False, errors

        if errors:
            self.logger.error(f"Column issues in tables: {list(errors.keys())}")
            return False, errors

        self.logger.info("All required columns exist")
        return True, None

    def check_constraints(
        self,
        inspector: Inspector,
        tables: List[str],
        existing_tables: Set[str],
        *_,
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:

        errors: Dict[str, Any] = {}

        for table in tables:
            if table not in existing_tables:
                continue

            try:
                pk = inspector.get_pk_constraint(table, schema=self.schema)
            except SQLAlchemyError as e:
                errors[table] = {"error": str(e)}
                continue

            if not pk or not pk.get("constrained_columns"):
                errors[table] = {"missing_primary_key": True}
                if self.fail_fast:
                    return False, errors

        if errors:
            self.logger.error("Primary key issues detected")
            return False, errors

        self.logger.info("All primary keys present")
        return True, None

    def check_foreign_keys(
        self,
        inspector: Inspector,
        tables: List[str],
        existing_tables: Set[str],
        *_,
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:

        errors: Dict[str, Any] = {}

        for table in tables:
            if table not in existing_tables:
                continue

            try:
                fks = inspector.get_foreign_keys(table, schema=self.schema)
            except SQLAlchemyError as e:
                errors[table] = {"error": str(e)}
                continue

            if fks is None:
                errors[table] = {"inspection_failed": True}
                if self.fail_fast:
                    return False, errors

        if errors:
            self.logger.error("Foreign key inspection issues")
            return False, errors

        self.logger.info("Foreign key inspection OK")
        return True, None

    def check_indexes(
        self,
        inspector: Inspector,
        tables: List[str],
        existing_tables: Set[str],
        *_,
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:

        errors: Dict[str, Any] = {}

        for table in tables:
            if table not in existing_tables:
                continue

            try:
                indexes = inspector.get_indexes(table, schema=self.schema)
            except SQLAlchemyError as e:
                errors[table] = {"error": str(e)}
                continue

            if indexes is None:
                errors[table] = {"inspection_failed": True}
                if self.fail_fast:
                    return False, errors

        if errors:
            self.logger.error("Index inspection issues")
            return False, errors

        self.logger.info("Index inspection OK")
        return True, None