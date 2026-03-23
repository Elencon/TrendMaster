"""
Data processing and cleaning utilities.
"""

import logging
import math
import re
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class DataUtils:
    """Data processing and cleaning utilities for database operations."""

    @staticmethod
    def clean_dataframe(
        df: pd.DataFrame,
        null_replacements: dict | None = None,
    ) -> pd.DataFrame:
        """
        Clean a DataFrame for database compatibility.

        Replaces NaN and ±inf with None (MySQL-compatible). Applies optional
        per-column null replacements after the global cleanup.

        Args:
            df: DataFrame to clean.
            null_replacements: Mapping of column name → replacement value for nulls.

        Returns:
            Cleaned DataFrame.
        """
        if df is None or df.empty:
            return df

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
    ) -> list[dict]:
        """
        Convert a DataFrame to a clean list of dicts for database insertion.

        Cleans NaN/inf values and optionally filters to schema columns.

        Args:
            df: DataFrame to convert.
            table_schema: Optional list of column names to include.

        Returns:
            List of clean record dictionaries.
        """
        if df is None or df.empty:
            return []

        df = DataUtils.clean_dataframe(df)

        if table_schema:
            available = [c for c in table_schema if c in df.columns]
            df = df[available]

        records = df.to_dict("records")

        # Guard against any remaining float NaN that slipped through
        return [
            {
                k: (None if isinstance(v, float) and math.isnan(v) else v)
                for k, v in record.items()
            }
            for record in records
        ]

    @staticmethod
    def arrow_to_records(
        table: Any,
        table_schema: list[str] | None = None,
    ) -> list[dict]:
        """
        Convert a PyArrow Table to a list of dicts suitable for DB insertion.

        Args:
            table: pyarrow.Table instance.
            table_schema: Optional list of column names to include.

        Returns:
            List of record dictionaries.
        """
        df = table.to_pandas()
        return DataUtils.dataframe_to_records(df, table_schema)

    @staticmethod
    def validate_records(
        records: list[dict],
        required_fields: list[str] | None = None,
    ) -> tuple[list[dict], list[str]]:
        """
        Validate records, separating valid ones from those with missing fields.

        Args:
            records: List of record dictionaries to validate.
            required_fields: Fields that must be present and non-None.

        Returns:
            Tuple of (valid_records, error_messages).
        """
        if not required_fields:
            return records, []

        valid: list[dict] = []
        errors: list[str] = []

        for i, record in enumerate(records):
            missing = [
                f for f in required_fields
                if f not in record or record[f] is None
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
        """
        Normalize DataFrame column names to a consistent format.

        Args:
            df: DataFrame to normalize.
            naming_convention: One of 'snake_case', 'camelCase', 'PascalCase'.

        Returns:
            DataFrame with renamed columns.
        """
        if df is None or df.empty:
            return df

        def to_snake(col: str) -> str:
            col = re.sub(r"([A-Z])", r"_\1", col).lower().strip("_")
            col = re.sub(r"[^\w]", "_", col)
            return re.sub(r"_+", "_", col)

        def to_camel(col: str) -> str:
            words = re.split(r"[^\w]", col)
            return words[0].lower() + "".join(w.capitalize() for w in words[1:])

        def to_pascal(col: str) -> str:
            return "".join(w.capitalize() for w in re.split(r"[^\w]", col))

        converters = {
            "snake_case": to_snake,
            "camelCase":  to_camel,
            "PascalCase": to_pascal,
        }
        convert = converters.get(naming_convention, lambda c: c)
        df.columns = [convert(c) for c in df.columns]
        return df

    @staticmethod
    def detect_data_types(df: pd.DataFrame) -> dict[str, str]:
        """
        Suggest SQL data types for each column in a DataFrame.

        Args:
            df: DataFrame to analyse.

        Returns:
            Mapping of column name → suggested SQL type string.
        """
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
                elif lo >= -32_768 and hi <= 32_767:
                    type_mapping[col] = "SMALLINT"
                elif lo >= -2_147_483_648 and hi <= 2_147_483_647:
                    type_mapping[col] = "INT"
                else:
                    type_mapping[col] = "BIGINT"
            elif pd.api.types.is_numeric_dtype(series):
                type_mapping[col] = "DECIMAL(10,2)"
            elif pd.api.types.is_datetime64_any_dtype(series):
                type_mapping[col] = "DATETIME"
            else:
                max_len = int(series.astype(str).str.len().max())
                type_mapping[col] = (
                    f"VARCHAR({min(max_len * 2, 255)})" if max_len <= 255 else "TEXT"
                )

        return type_mapping

    @staticmethod
    def split_dataframe_chunks(
        df: pd.DataFrame,
        chunk_size: int = 1000,
    ) -> list[pd.DataFrame]:
        """
        Split a DataFrame into equal-sized chunks.

        Args:
            df: DataFrame to split.
            chunk_size: Maximum rows per chunk.

        Returns:
            List of DataFrame chunks.
        """
        if df is None or df.empty:
            return []

        return [
            df.iloc[i:i + chunk_size].copy()
            for i in range(0, len(df), chunk_size)
        ]

    @staticmethod
    def merge_dataframes_safe(
        dfs: list[pd.DataFrame],
        how: str = "outer",
    ) -> pd.DataFrame:
        """
        Merge multiple DataFrames with error handling.

        Merges on common columns when they exist; concatenates otherwise.

        Args:
            dfs: DataFrames to merge.
            how: Merge strategy — 'outer', 'inner', 'left', or 'right'.

        Returns:
            Merged DataFrame, or an empty DataFrame on error.
        """
        if not dfs:
            return pd.DataFrame()
        if len(dfs) == 1:
            return dfs[0].copy()

        try:
            result = dfs[0].copy()
            for df in dfs[1:]:
                common = list(set(result.columns) & set(df.columns))
                if common:
                    result = pd.merge(
                        result, df, on=common, how=how, suffixes=("", "_dup")
                    )
                else:
                    result = pd.concat([result, df], ignore_index=True, sort=False)
            return result
        except Exception as e:
            logger.error("Error merging DataFrames: %s", e)
            return pd.DataFrame()

    @staticmethod
    def remove_duplicate_records(
        records: list[dict],
        key_fields: list[str] | None = None,
    ) -> list[dict]:
        """
        Remove duplicate records while preserving insertion order.

        Args:
            records: List of record dictionaries.
            key_fields: Fields to use for duplicate detection.
                If None, all fields are used.

        Returns:
            Deduplicated list of records.
        """
        if not records:
            return records

        seen: set = set()
        unique: list[dict] = []

        for record in records:
            key = (
                tuple(record.get(f) for f in key_fields)
                if key_fields
                else tuple(sorted(record.items()))
            )
            if key not in seen:
                seen.add(key)
                unique.append(record)

        return unique