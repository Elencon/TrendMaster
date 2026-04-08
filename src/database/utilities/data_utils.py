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

from __future__ import annotations

import logging
import re
import sys
import unittest

import numpy as np
import pandas as pd
import pyarrow as pa

from typing import Any, Literal
from collections.abc import Iterable

logger = logging.getLogger(__name__)


class DataUtils:
    """Data processing and cleaning utilities for database operations."""
    @staticmethod
    def clean_dataframe(
        df: pd.DataFrame | None,
        null_replacements: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """
        Clean a DataFrame for safe database / JSON / Arrow export.

        - Replaces NaN and ±Inf with None
        - Applies optional per-column null replacements
        - Always returns a new DataFrame (never modifies input in place)
        - Returns an empty DataFrame if input is None or empty

        Args:
            df: Input DataFrame.
            null_replacements: Optional dict {column: replacement_value}
                               applied after global cleanup.

        Returns:
            Cleaned DataFrame copy.
        """
        if df is None or df.empty:
            return pd.DataFrame()

        # Work on a copy to avoid mutating the original
        df = df.copy()

        # Global replacement of NaN and Inf
        df = df.replace([np.nan, np.inf, -np.inf], None)

        # Per-column null replacements
        if null_replacements:
            for col, replacement in null_replacements.items():
                if col in df.columns:
                    # Safe assignment (avoids SettingWithCopy issues)
                    df[col] = df[col].where(df[col].notna(), replacement)

        return df

    @staticmethod
    def dataframe_to_records(
        df: pd.DataFrame | None,
        table_schema: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Convert a pandas DataFrame to a list of dictionaries suitable for database insertion.

        Features:
        - Cleans NaN / Inf using `clean_dataframe`
        - Optionally filters columns to match `table_schema`
        - Ensures all NaN/Inf values are converted to None
        - Returns clean, fully serializable records

        Args:
            df: Input DataFrame.
            table_schema: Optional whitelist of columns to keep.

        Returns:
            List of dict records ready for DB insertion.
        """
        if df is None or df.empty:
            return []

        # Clean DataFrame
        df = DataUtils.clean_dataframe(df)

        # Filter columns if schema is provided
        if table_schema:
            available_cols = [c for c in table_schema if c in df.columns]
            if available_cols:
                df = df[available_cols]

        # Convert to records
        records = df.to_dict("records")

        # Final safety pass to ensure no NaN/Inf remain
        cleaned_records: list[dict[str, Any]] = []
        for record in records:
            cleaned: dict[str, Any] = {}
            for k, v in record.items():
                if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                    cleaned[k] = None
                else:
                    cleaned[k] = v
            cleaned_records.append(cleaned)

        return cleaned_records

    @staticmethod
    def arrow_to_records(
        table: pa.Table | None,
        table_schema: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Convert a PyArrow Table to a list of dictionaries suitable for database insertion.

        Uses pandas as an intermediate format for consistency.
        """
        if table is None or len(table) == 0:
            return []

        # Convert Arrow → pandas with optimizations
        df: pd.DataFrame = table.to_pandas(
            self_destruct=True,     # frees Arrow memory after conversion
            date_as_object=False,   # keeps datetime types efficient
        )

        return DataUtils.dataframe_to_records(df, table_schema)

    @staticmethod
    def validate_records(
        records: list[dict[str, Any]],
        required_fields: list[str] | None = None,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        """
        Validate records against required fields.

        Args:
            records: List of record dictionaries.
            required_fields: Fields that must be present and non-None.

        Returns:
            Tuple of:
                - valid_records
                - error_messages
        """
        if not records:
            return [], []

        if not required_fields:
            return records[:], []

        valid: list[dict[str, Any]] = []
        errors: list[str] = []

        for i, record in enumerate(records):
            missing = [
                field for field in required_fields
                if field not in record or record.get(field) is None
            ]

            if missing:
                errors.append(
                    f"Record #{i}: missing required fields {missing}"
                )
            else:
                valid.append(record)

        return valid, errors

    # NOTE:
    # This method is assumed to exist in your codebase.
    # It is required by dataframe_to_records.

    @staticmethod
    def normalize_column_names(
        df: pd.DataFrame,
        naming_convention: Literal["snake_case", "camelCase", "PascalCase"] = "snake_case",
    ) -> pd.DataFrame:
        """Normalize DataFrame column names to a consistent naming format.

        Preserves original interface behavior:
        - Returns None if input is None
        - Returns a copy of the DataFrame if valid input
        """

        # Preserve File 1 behavior for None input
        if df is None:
            return pd.DataFrame()

        if df.empty:
            return df.copy()

        def to_snake_case(col: str) -> str:
            if not col:
                return "col"

            col = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", col)
            col = re.sub(r"[^a-zA-Z0-9]+", "_", col)
            col = col.lower().strip("_")
            return re.sub(r"_+", "_", col) or "col"

        def to_camel_case(col: str) -> str:
            if not col:
                return "col"

            words = [w for w in re.split(r"[^a-zA-Z0-9]+", col) if w]
            if not words:
                return "col"

            return words[0].lower() + "".join(w.capitalize() for w in words[1:])

        def to_pascal_case(col: str) -> str:
            if not col:
                return "Col"

            words = [w for w in re.split(r"[^a-zA-Z0-9]+", col) if w]
            return "".join(w.capitalize() for w in words) or "Col"

        converters = {
            "snake_case": to_snake_case,
            "camelCase": to_camel_case,
            "PascalCase": to_pascal_case,
        }

        convert_func = converters.get(naming_convention, lambda c: str(c).strip())

        # Apply conversion
        new_columns = [convert_func(str(col)) for col in df.columns]

        # Ensure uniqueness (clean, stable approach)
        seen: dict[str, int] = {}
        final_columns: list[str] = []

        for col in new_columns:
            if col in seen:
                seen[col] += 1
                final_columns.append(f"{col}_{seen[col]}")
            else:
                seen[col] = 1
                final_columns.append(col)

        # Return a copy with updated columns
        result = df.copy()
        result.columns = final_columns
        return result

    @staticmethod
    def detect_data_types(df: pd.DataFrame) -> dict[str, str]:
        """
        Analyze a pandas DataFrame and suggest appropriate SQL data types for each column.

        Supports:
        - Boolean → BOOLEAN
        - Integers → TINYINT / SMALLINT / INT / BIGINT (range-based)
        - Floats → DECIMAL(18,6)
        - Datetime → DATETIME
        - Strings / Object / Category → VARCHAR(n) or TEXT
        - Empty / all-null columns → TEXT

        Args:
            df: Input DataFrame.

        Returns:
            Dictionary mapping {column_name: sql_type}
        """

        if df is None or df.empty:
            return {}

        type_mapping: dict[str, str] = {}

        for col in df.columns:
            series = df[col].dropna()

            # Empty column after dropping NaNs
            if series.empty:
                type_mapping[col] = "TEXT"
                continue

            # Boolean
            if pd.api.types.is_bool_dtype(series):
                type_mapping[col] = "BOOLEAN"
                continue

            # Integer
            if pd.api.types.is_integer_dtype(series):
                lo, hi = series.min(), series.max()

                if lo >= -128 and hi <= 127:
                    type_mapping[col] = "TINYINT"
                elif lo >= -32768 and hi <= 32767:
                    type_mapping[col] = "SMALLINT"
                elif lo >= -2147483648 and hi <= 2147483647:
                    type_mapping[col] = "INT"
                else:
                    type_mapping[col] = "BIGINT"
                continue

            # Float
            if pd.api.types.is_float_dtype(series):
                type_mapping[col] = "DECIMAL(18,6)"
                continue

            # Datetime
            if pd.api.types.is_datetime64_any_dtype(series):
                type_mapping[col] = "DATETIME"
                continue

            # String / Object / Category
            str_series = series.astype(str)
            max_len = int(str_series.str.len().max() or 0)

            if max_len == 0:
                type_mapping[col] = "VARCHAR(255)"
            elif max_len <= 255:
                # Add buffer (~1.5x) but cap at 255
                suggested_len = min(int(max_len * 1.5), 255)
                type_mapping[col] = f"VARCHAR({suggested_len})"
            else:
                type_mapping[col] = "TEXT"

            # Category dtype refinement
            if pd.api.types.is_categorical_dtype(series.dtype):
                # For categorical data, prefer VARCHAR sized to actual content
                if max_len > 0 and max_len <= 255:
                    type_mapping[col] = f"VARCHAR({max_len})"

        return type_mapping

    @staticmethod
    def split_dataframe_chunks(
        df: pd.DataFrame,
        chunk_size: int = 1000,
    ) -> list[pd.DataFrame]:
        """
        Split a DataFrame into chunks of approximately equal size.

        This function preserves the original order and returns copies of each chunk
        to avoid SettingWithCopyWarning and unexpected modifications.

        Args:
            df: The DataFrame to split.
            chunk_size: Number of rows per chunk (must be >= 1).

        Returns:
            List of DataFrame chunks. Returns empty list if input is None or empty.
        """
        if df is None or df.empty:
            return []

        # Ensure chunk_size is valid
        chunk_size = max(1, int(chunk_size))

        # More efficient and cleaner implementation
        n = len(df)
        return [
            df.iloc[i : i + chunk_size].copy()
            for i in range(0, n, chunk_size)
        ]

    @staticmethod
    def merge_dataframes_safe(
        dfs: list[pd.DataFrame],
        how: str = "outer",
        *,
        validate: str | None = None,
        suffix: str = "_dup",
    ) -> pd.DataFrame:
        """
        Safely merge multiple DataFrames.

        Behavior:
        - Merges on ALL shared columns (SQL-style join).
        - Falls back to concatenation when no common columns exist.
        - Preserves column order.
        - Avoids modifying original DataFrames.
        - Returns empty DataFrame on failure.

        Args:
            dfs: List of DataFrames to merge.
            how: Join type ('outer', 'inner', 'left', 'right', 'cross', etc.).
            validate: Optional merge validation ('one_to_one', 'one_to_many', etc.).
            suffix: Suffix for duplicate column names.

        Returns:
            A merged DataFrame.
        """
        if not dfs:
            return pd.DataFrame()

        if len(dfs) == 1:
            return dfs[0].copy()

        # Basic validation (don't over-restrict future pandas features)
        if not isinstance(how, str):
            logger.warning("Invalid 'how' type (%s). Falling back to 'outer'.", type(how))
            how = "outer"

        try:
            result = dfs[0].copy()

            for i, df in enumerate(dfs[1:], start=2):
                if df is None or df.empty:
                    continue

                # Preserve column order (IMPORTANT FIX)
                common_cols = [c for c in result.columns if c in df.columns]

                if common_cols:
                    result = pd.merge(
                        result,
                        df,
                        on=common_cols,
                        how=how,
                        suffixes=("", suffix),
                        validate=validate,
                        copy=False,  # pandas 2.1+ optimization
                    )
                else:
                    result = pd.concat(
                        [result, df],
                        ignore_index=True,
                        sort=False,
                        copy=False,
                    )

            return result

        except Exception as e:
            logger.error(
                "merge_dataframes_safe failed | how=%s | dfs=%d | error=%s",
                how,
                len(dfs),
                e,
                exc_info=True,
            )
            return pd.DataFrame()

    @staticmethod
    def remove_duplicate_records(
        records: list[dict[str, Any]],
        key_fields: Iterable[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Remove duplicate records while preserving insertion order.

        Args:
            records: List of dictionaries to deduplicate.
            key_fields: Iterable of field names to use as deduplication key.
                        If None or empty, the entire record is used.

        Returns:
            List of unique records in first-seen order.
        """
        if not records:
            return []

        seen: set[tuple] = set()
        unique: list[dict[str, Any]] = []

        # Normalize key_fields to avoid empty iterable bug
        key_fields = tuple(key_fields) if key_fields else None

        for record in records:
            if key_fields:
                key = tuple(record.get(field) for field in key_fields)
            else:
                try:
                    key = tuple(sorted(record.items()))
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
