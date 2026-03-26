r"""
C:\Economy\Invest\TrendMaster\src\database\pandas_optimizer.py
Compact pandas optimization utilities for memory-efficient ETL operations.
python -m src.database.pandas_optimizer
"""

import pandas as pd
import numpy as np

from typing import Dict, List, Any, Union, Iterator, Callable
from pathlib import Path
import logging
import gc
import psutil

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data Utilities
# ---------------------------------------------------------------------------

class DataUtils:
    @staticmethod
    def get_dataframe_memory_mb(df: pd.DataFrame) -> float:
        try:
            return df.memory_usage(deep=True).sum() / (1024 * 1024)
        except Exception:
            return 0.0

    @staticmethod
    def should_be_categorical(series: pd.Series, threshold: float = 0.5) -> bool:
        if not isinstance(series, pd.Series):
            return False

        n = len(series)
        if n == 0:
            return False

        try:
            unique_ratio = series.nunique(dropna=True) / n
            return unique_ratio < threshold
        except Exception:
            return False

    @staticmethod
    def create_stats_tracker() -> Dict[str, Any]:
        return {
            "memory_optimized": 0,
            "chunks_processed": 0,
            "rows_processed": 0,
            "memory_saved_mb": 0.0,
        }

    @staticmethod
    def update_stats(stats: Dict[str, Any], key: str, value: Any) -> None:
        if key in stats and isinstance(stats[key], (int, float)) and isinstance(value, (int, float)):
            stats[key] += value
        else:
            stats[key] = value

    @staticmethod
    def force_cleanup() -> None:
        gc.collect()

    @staticmethod
    def profile_dataframe(df: pd.DataFrame) -> Dict[str, Any]:
        return {
            "rows": len(df),
            "columns": list(df.columns),
            "memory_usage_mb": DataUtils.get_dataframe_memory_mb(df),
            "dtypes": df.dtypes.astype(str).to_dict(),
        }


# ---------------------------------------------------------------------------
# Pandas Optimizer
# ---------------------------------------------------------------------------

class PandasOptimizer:
    """Memory-efficient pandas operations with automatic optimization."""

    def __init__(self, max_memory_mb: int = 512, chunk_size: int = 10000, auto_optimize: bool = True):
        self.max_memory_mb = max_memory_mb
        self.chunk_size = chunk_size
        self.auto_optimize = auto_optimize
        self.stats = DataUtils.create_stats_tracker()

    def get_memory_usage_mb(self) -> float:
        try:
            return psutil.Process().memory_info().rss / (1024 * 1024)
        except Exception:
            return 0.0

    def optimize_dtypes(
        self,
        df: pd.DataFrame,
        categorical_threshold: float = 0.5,
        inplace: bool = True
    ) -> pd.DataFrame:
        """Optimize DataFrame dtypes. Supports inplace or copy."""
        if not self.auto_optimize or df.empty:
            return df

        if not inplace:
            df = df.copy()

        original_memory = DataUtils.get_dataframe_memory_mb(df)

        # Numeric downcasting
        for col in df.select_dtypes(include=["int64", "int32", "int16"]).columns:
            df[col] = pd.to_numeric(df[col], downcast="integer")

        for col in df.select_dtypes(include=["float64", "float32"]).columns:
            df[col] = pd.to_numeric(df[col], downcast="float")

        # Categorical conversion
        for col in df.select_dtypes(include=["object"]).columns:
            if DataUtils.should_be_categorical(df[col], categorical_threshold):
                df[col] = df[col].astype("category")

        optimized_memory = DataUtils.get_dataframe_memory_mb(df)
        memory_saved = original_memory - optimized_memory

        DataUtils.update_stats(self.stats, "memory_optimized", 1)
        DataUtils.update_stats(self.stats, "memory_saved_mb", memory_saved)

        logger.info(
            f"Memory: {original_memory:.2f}MB → {optimized_memory:.2f}MB "
            f"(saved {memory_saved:.2f}MB)"
        )

        return df

    def process_in_chunks(
        self,
        file_path: Union[str, Path],
        processor: Callable[[pd.DataFrame], Any],
        **read_kwargs
    ) -> Iterator[Any]:
        read_params = {
            "chunksize": self.chunk_size,
            "low_memory": True,
            **read_kwargs,
        }

        total_chunks, total_rows = 0, 0

        try:
            for chunk_num, chunk in enumerate(pd.read_csv(file_path, **read_params)):
                if self.get_memory_usage_mb() > self.max_memory_mb:
                    logger.warning("Memory limit exceeded, forcing cleanup")
                    DataUtils.force_cleanup()

                if self.auto_optimize:
                    chunk = self.optimize_dtypes(chunk, inplace=True)

                result = processor(chunk)

                total_chunks += 1
                total_rows += len(chunk)

                if chunk_num % 10 == 0:
                    logger.info(f"Processed {chunk_num} chunks...")

                yield result

            DataUtils.update_stats(self.stats, "chunks_processed", total_chunks)
            DataUtils.update_stats(self.stats, "rows_processed", total_rows)

        except Exception as e:
            logger.error(f"Chunk processing failed: {e}")
            raise

    def efficient_groupby(
        self,
        df: pd.DataFrame,
        groupby_cols: List[str],
        agg_funcs: Dict[str, Any],
        sort_result: bool = False
    ) -> pd.DataFrame:
        if self.auto_optimize:
            df = self.optimize_dtypes(df, inplace=True)

        result = df.groupby(groupby_cols, sort=sort_result, observed=True).agg(agg_funcs)

        return result.reset_index() if isinstance(result.index, pd.MultiIndex) or len(groupby_cols) == 1 else result

    def efficient_merge(
        self,
        left: pd.DataFrame,
        right: pd.DataFrame,
        on: Union[str, List[str]] = None,
        how: str = "inner",
        **kwargs
    ) -> pd.DataFrame:
        if self.auto_optimize:
            left = self.optimize_dtypes(left, inplace=False)
            right = self.optimize_dtypes(right, inplace=False)

        # Optimize join keys
        if on:
            keys = [on] if isinstance(on, str) else on
            for col in keys:
                if col in left.columns and col in right.columns:
                    left[col] = left[col].astype("category")
                    right[col] = right[col].astype("category")

        result = pd.merge(left, right, on=on, how=how, **kwargs)

        logger.info(f"Merge complete: {len(result)} rows")
        return result

    def suggest_optimizations(self, df: pd.DataFrame) -> List[str]:
        suggestions = []

        memory_mb = DataUtils.get_dataframe_memory_mb(df)
        if memory_mb > self.max_memory_mb:
            suggestions.append(
                f"Memory usage ({memory_mb:.1f}MB) exceeds limit ({self.max_memory_mb}MB)"
            )

        for col in df.columns:
            col_type = df[col].dtype
            unique_count = df[col].nunique(dropna=True)
            total_count = len(df)

            if total_count == 0:
                continue

            if pd.api.types.is_object_dtype(col_type):
                if unique_count / total_count < 0.5:
                    suggestions.append(
                        f"'{col}' could be categorical ({unique_count}/{total_count} unique)"
                    )

            if pd.api.types.is_integer_dtype(col_type) or pd.api.types.is_float_dtype(col_type):
                suggestions.append(f"'{col}' ({col_type}) could be downcast")

        return suggestions

    def get_data_profile(self, df: pd.DataFrame) -> Dict[str, Any]:
        return DataUtils.profile_dataframe(df)

    def get_stats(self) -> Dict[str, Any]:
        return self.stats.copy()


# ---------------------------------------------------------------------------
# Chunker
# ---------------------------------------------------------------------------

class DataFrameChunker:
    def __init__(self, chunk_size: int = 10000):
        self.chunk_size = chunk_size

    def chunk_dataframe(self, df: pd.DataFrame) -> Iterator[pd.DataFrame]:
        for i in range(0, len(df), self.chunk_size):
            yield df.iloc[i:i + self.chunk_size]

    def process_chunks(
        self,
        df: pd.DataFrame,
        processor: Callable[[pd.DataFrame], Any],
        combine_results: bool = True
    ) -> Union[List[Any], pd.DataFrame]:
        results = [processor(chunk) for chunk in self.chunk_dataframe(df)]

        if combine_results and results and isinstance(results[0], pd.DataFrame):
            return pd.concat(results, ignore_index=True)

        return results


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def create_pandas_optimizer(max_memory_mb=512, chunk_size=10000, auto_optimize=True):
    return PandasOptimizer(max_memory_mb, chunk_size, auto_optimize)


def optimize_csv_reading(file_path: Union[str, Path], **kwargs) -> pd.DataFrame:
    optimizer = PandasOptimizer(auto_optimize=True)

    try:
        df = pd.read_csv(file_path, **kwargs)
        return optimizer.optimize_dtypes(df)
    except MemoryError:
        chunks = list(optimizer.process_in_chunks(file_path, lambda x: x, **kwargs))
        return pd.concat(chunks, ignore_index=True)


def get_memory_efficient_dtypes(df: pd.DataFrame) -> Dict[str, str]:
    return {col: str(df[col].dtype) for col in df.columns}


# ---------------------------------------------------------------------------
# Local Test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import itertools

    print("Testing compact pandas optimizer...")

    # Create test data with consistent column lengths
    df = pd.DataFrame({
        "id": range(1000),
        "category": list(itertools.islice(itertools.cycle(["A", "B", "C"]), 1000)),
        "value": np.random.randn(1000),
        "score": np.random.randint(0, 100, 1000),
    })

    # Initialize optimizer
    optimizer = PandasOptimizer()

    # Print original memory usage
    original_mem = DataUtils.get_dataframe_memory_mb(df)
    print(f"Original DataFrame: {df.shape}, {original_mem:.2f} MB")

    # Optimize DataFrame dtypes
    optimized_df = optimizer.optimize_dtypes(df)
    optimized_mem = DataUtils.get_dataframe_memory_mb(optimized_df)
    print(f"Optimized DataFrame: {optimized_df.shape}, {optimized_mem:.2f} MB")

    # Print optimization stats
    print("Optimizer stats:", optimizer.get_stats())

    # Optional: demonstrate chunked processing
    chunker = DataFrameChunker(chunk_size=250)
    combined_df = chunker.process_chunks(optimized_df, lambda x: x)
    print(f"Chunked processing result: {combined_df.shape}")
