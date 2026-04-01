r"""
C:\Economy\Invest\TrendMaster\src\database\utilities\data_utils.py
Data processing and cleaning utilities.
    How to use it now:
    Bash# Run the demo
    python data_utils.py --demo

    # Run the tests
    python data_utils.py --test

    # Show help
    python data_utils.py
"""

import logging
import math
import re
import sys
import unittest
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class DataUtils:
    """Data processing and cleaning utilities for database operations."""

    @staticmethod
    def clean_dataframe(
        df: pd.DataFrame,
        null_replacements: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """
        Clean a DataFrame for database compatibility.

        Replaces NaN and ±inf with None (MySQL-compatible). Applies optional
        per-column null replacements after the global cleanup.
        """
        if df is None or df.empty:
            return df.copy() if isinstance(df, pd.DataFrame) else df

        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})

        if null_replacements:
            for column, replacement in null_replacements.items():
                if column in df.columns:
                    df[column] = df[column].fillna(replacement)

        return df

    @staticmethod
    def dataframe_to_records(
        df: pd.DataFrame,
        table_schema: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Convert a DataFrame to a clean list of dicts for database insertion.
        """
        if df is None or df.empty:
            return []

        df = DataUtils.clean_dataframe(df)

        if table_schema:
            available = [c for c in table_schema if c in df.columns]
            if available:
                df = df[available]

        records = df.to_dict("records")

        # Final guard against any remaining NaN or inf
        cleaned_records = []
        for record in records:
            cleaned = {}
            for k, v in record.items():
                if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                    cleaned[k] = None
                else:
                    cleaned[k] = v
            cleaned_records.append(cleaned)
        return cleaned_records

    @staticmethod
    def arrow_to_records(
        table: Any,
        table_schema: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Convert a PyArrow Table to a list of dicts suitable for DB insertion."""
        if table is None:
            return []
        df = table.to_pandas()
        return DataUtils.dataframe_to_records(df, table_schema)

    @staticmethod
    def validate_records(
        records: list[dict[str, Any]],
        required_fields: list[str] | None = None,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        """Validate records, separating valid ones from those with missing fields."""
        if not required_fields:
            return records[:], []

        valid: list[dict[str, Any]] = []
        errors: list[str] = []

        for i, record in enumerate(records):
            missing = [
                f for f in required_fields
                if f not in record or record.get(f) is None
            ]
            if missing:
                errors.append(f"Record {i}: missing required fields: {missing}")
            else:
                valid.append(record)

        return valid, errors

    @staticmethod
    def normalize_column_names(
        df: pd.DataFrame,
        naming_convention: str = "snake_case",
    ) -> pd.DataFrame:
        """Normalize DataFrame column names to a consistent format."""
        if df is None or df.empty:
            return df.copy() if isinstance(df, pd.DataFrame) else df

        def to_snake(col: str) -> str:
            col = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", col)
            col = re.sub(r"[^a-zA-Z0-9]", "_", col).lower().strip("_")
            return re.sub(r"_+", "_", col)

        def to_camel(col: str) -> str:
            words = [w for w in re.split(r"[^a-zA-Z0-9]", col) if w]
            if not words:
                return "col"
            return words[0].lower() + "".join(w.capitalize() for w in words[1:])

        def to_pascal(col: str) -> str:
            words = [w for w in re.split(r"[^a-zA-Z0-9]", col) if w]
            return "".join(w.capitalize() for w in words) or "Col"

        converters = {
            "snake_case": to_snake,
            "camelCase": to_camel,
            "PascalCase": to_pascal,
        }

        convert = converters.get(naming_convention, lambda c: str(c).strip())
        new_columns = [convert(str(c)) for c in df.columns]

        # Ensure column name uniqueness
        seen: dict[str, int] = {}
        for i, col in enumerate(new_columns):
            if col in seen:
                new_columns[i] = f"{col}_{seen[col]}"
                seen[col] += 1
            else:
                seen[col] = 1

        df = df.copy()
        df.columns = new_columns
        return df

    @staticmethod
    def detect_data_types(df: pd.DataFrame) -> dict[str, str]:
        """Suggest SQL data types for each column in a DataFrame."""
        type_mapping: dict[str, str] = {}

        for col in df.columns:
            series = df[col].dropna()

            if series.empty:
                type_mapping[col] = "TEXT"
                continue

            if pd.api.types.is_bool_dtype(series):
                type_mapping[col] = "BOOLEAN"
            elif pd.api.types.is_integer_dtype(series):
                lo, hi = series.min(), series.max()
                if lo >= -128 and hi <= 127:
                    type_mapping[col] = "TINYINT"
                elif lo >= -32768 and hi <= 32767:
                    type_mapping[col] = "SMALLINT"
                elif lo >= -2147483648 and hi <= 2147483647:
                    type_mapping[col] = "INT"
                else:
                    type_mapping[col] = "BIGINT"
            elif pd.api.types.is_numeric_dtype(series):
                type_mapping[col] = "DECIMAL(10,2)"
            elif pd.api.types.is_datetime64_any_dtype(series):
                type_mapping[col] = "DATETIME"
            else:
                max_len = int(series.astype(str).str.len().max() or 0)
                type_mapping[col] = (
                    f"VARCHAR({min(max_len * 2, 255)})" if max_len <= 255 else "TEXT"
                )

        return type_mapping

    @staticmethod
    def split_dataframe_chunks(
        df: pd.DataFrame,
        chunk_size: int = 1000,
    ) -> list[pd.DataFrame]:
        """Split a DataFrame into equal-sized chunks."""
        if df is None or df.empty:
            return []

        chunk_size = max(1, int(chunk_size))
        return [
            df.iloc[i : i + chunk_size].copy()
            for i in range(0, len(df), chunk_size)
        ]

    @staticmethod
    def merge_dataframes_safe(
        dfs: list[pd.DataFrame],
        how: str = "outer",
    ) -> pd.DataFrame:
        """Merge multiple DataFrames with error handling."""
        if not dfs:
            return pd.DataFrame()
        if len(dfs) == 1:
            return dfs[0].copy()

        try:
            result = dfs[0].copy()
            for df in dfs[1:]:
                if df.empty:
                    continue
                common = list(set(result.columns) & set(df.columns))
                if common:
                    result = pd.merge(
                        result, df, on=common, how=how, suffixes=("", "_dup")
                    )
                else:
                    result = pd.concat([result, df], ignore_index=True, sort=False)
            return result
        except Exception as e:
            logger.error("Error merging DataFrames: %s", e, exc_info=True)
            return pd.DataFrame()

    @staticmethod
    def remove_duplicate_records(
        records: list[dict[str, Any]],
        key_fields: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Remove duplicate records while preserving insertion order."""
        if not records:
            return []

        seen: set[tuple] = set()
        unique: list[dict[str, Any]] = []

        for record in records:
            if key_fields:
                key = tuple(record.get(f) for f in key_fields)
            else:
                try:
                    key = tuple(sorted((k, v) for k, v in record.items()))
                except TypeError:
                    key = tuple(sorted((k, str(v)) for k, v in record.items()))

            if key not in seen:
                seen.add(key)
                unique.append(record)

        return unique


# ========================= LOCAL TESTS =========================

class TestDataUtils(unittest.TestCase):
    """Local unit tests for DataUtils class."""

    def test_clean_dataframe(self):
        df = pd.DataFrame({
            "a": [1, np.nan, 3],
            "b": [np.inf, 2, -np.inf],
            "c": [None, "text", np.nan]
        })

        cleaned = DataUtils.clean_dataframe(df)
        self.assertIsNone(cleaned.iloc[0]["a"])
        self.assertIsNone(cleaned.iloc[0]["b"])
        self.assertIsNone(cleaned.iloc[2]["b"])

        with_repl = DataUtils.clean_dataframe(df, {"a": -999, "c": "MISSING"})
        self.assertEqual(with_repl.iloc[1]["a"], -999)
        self.assertEqual(with_repl.iloc[2]["c"], "MISSING")

    def test_dataframe_to_records(self):
        df = pd.DataFrame({
            "id": [1, 2],
            "value": [10.5, np.nan],
            "name": ["Alice", "Bob"]
        })

        records = DataUtils.dataframe_to_records(df)
        self.assertEqual(len(records), 2)
        self.assertIsNone(records[1]["value"])

    def test_normalize_column_names(self):
        df = pd.DataFrame(columns=["UserID", "First Name", "Last-Name", "emailAddress"])

        snake = DataUtils.normalize_column_names(df, "snake_case")
        self.assertEqual(list(snake.columns),
                        ["user_id", "first_name", "last_name", "email_address"])

    def test_detect_data_types(self):
        df = pd.DataFrame({
            "int_col": [1, 2, 3],
            "bool_col": [True, False, True],
            "str_col": ["a", "bb", "ccc"]
        })

        types = DataUtils.detect_data_types(df)
        self.assertEqual(types["int_col"], "INT")
        self.assertEqual(types["bool_col"], "BOOLEAN")

    def test_remove_duplicate_records(self):
        records = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 1, "name": "Alice"},
            {"id": 3, "name": "Charlie"}
        ]

        unique = DataUtils.remove_duplicate_records(records, key_fields=["id"])
        self.assertEqual(len(unique), 3)

    def test_split_dataframe_chunks(self):
        df = pd.DataFrame({"x": range(2500)})
        chunks = DataUtils.split_dataframe_chunks(df, chunk_size=1000)
        self.assertEqual(len(chunks), 3)

    def test_validate_records(self):
        records = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": None},
        ]
        valid, errors = DataUtils.validate_records(records, ["id", "name"])
        self.assertEqual(len(valid), 1)
        self.assertEqual(len(errors), 1)


# ========================= DEMO & CLI =========================

def run_demo():
    """Run a small demonstration of DataUtils functionality."""
    print("=== DataUtils Demo ===\n")

    # Sample data with messy values
    data = {
        "UserID": [1, 2, 3, 4],
        "First Name": ["Alice", "Bob", None, "Charlie"],
        "Last-Name": ["Smith", np.nan, "Johnson", "Brown"],
        "emailAddress": ["alice@example.com", "bob@example.com", None, "charlie@example.com"],
        "score": [95.5, np.inf, -np.inf, 87.0],
        "active": [True, False, True, None]
    }

    df = pd.DataFrame(data)
    print("Original DataFrame:")
    print(df)
    print("\n" + "="*60 + "\n")

    # 1. Clean DataFrame
    cleaned_df = DataUtils.clean_dataframe(df, null_replacements={"active": False})
    print("After clean_dataframe (with null replacement for 'active'):")
    print(cleaned_df)
    print("\n" + "="*60 + "\n")

    # 2. Normalize column names
    normalized_df = DataUtils.normalize_column_names(cleaned_df, "snake_case")
    print("After normalize_column_names (snake_case):")
    print(normalized_df)
    print("\n" + "="*60 + "\n")

    # 3. Convert to records
    records = DataUtils.dataframe_to_records(normalized_df)
    print(f"Converted to {len(records)} clean records:")
    for i, rec in enumerate(records[:2]):  # show first 2 only
        print(f"  {i+1}: {rec}")
    if len(records) > 2:
        print(f"  ... and {len(records)-2} more")
    print("\n" + "="*60 + "\n")

    # 4. Detect data types
    dtypes = DataUtils.detect_data_types(normalized_df)
    print("Suggested SQL data types:")
    for col, dtype in dtypes.items():
        print(f"  {col:15} → {dtype}")
    print("\nDemo completed successfully!")


if __name__ == "__main__":
    # Simple CLI argument handling
    if len(sys.argv) > 1 and sys.argv[1] in ("--demo", "-d", "demo"):
        run_demo()
    elif len(sys.argv) > 1 and sys.argv[1] in ("--test", "-t", "test"):
        print("Running DataUtils unit tests...\n")
        unittest.main(argv=[""], verbosity=2, exit=False)
    else:
        # Default behavior: show help
        print("DataUtils Utility Module")
        print("=" * 40)
        print("Usage:")
        print("  python data_utils.py --demo     Run demonstration")
        print("  python data_utils.py --test     Run unit tests")
        print("  python data_utils.py            Show this help")
        print("\nYou can also import and use DataUtils directly in your code.")