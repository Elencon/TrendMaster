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
    """Static helper methods for DataFrame memory and profiling."""

    @staticmethod
    def get_dataframe_memory_mb(df: pd.DataFrame) -> float:
        """Return memory usage in MB."""
        if df is None or df.empty:
            return 0.0
        try:
            return df.memory_usage(deep=True).sum() / (1024 * 1024)
        except Exception:
            return 0.0

    @staticmethod
    def should_be_categorical(series: pd.Series, threshold: float = 0.5) -> bool:
        """Return True if column is a good candidate for categorical dtype."""
        if not isinstance(series, pd.Series) or len(series) == 0:
            return False
        if pd.api.types.is_categorical_dtype(series):
            return False
        try:
            unique_ratio = series.nunique(dropna=True) / len(series)
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
    """Memory-efficient pandas operations with automatic optimization.
    
    Combines best parts of both versions:
    - Full interface and features from File 1
    - Modern pandas syntax, robustness, and smarter logic from File 2
    """

    def __init__(self, max_memory_mb: int = 512, chunk_size: int = 10000, auto_optimize: bool = True):
        self.max_memory_mb = max_memory_mb
        self.chunk_size = chunk_size
        self.auto_optimize = auto_optimize
        self.stats = DataUtils.create_stats_tracker()

    def get_memory_usage_mb(self) -> float:
        """Return current process RSS memory in MB."""
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
        """Optimize DataFrame dtypes for lower memory usage."""
        if not self.auto_optimize or df is None or df.empty:
            return df

        if not inplace:
            df = df.copy()

        original_memory = DataUtils.get_dataframe_memory_mb(df)

        # Modern numeric downcasting (best from File 2)
        for col in df.select_dtypes(include=["integer"]).columns:
            df[col] = pd.to_numeric(df[col], downcast="integer", errors="ignore")

        for col in df.select_dtypes(include=["floating"]).columns:
            df[col] = pd.to_numeric(df[col], downcast="float", errors="ignore")

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
        """Process large CSV file in chunks with memory safeguards."""
        read_params = {
            "chunksize": self.chunk_size,
            "low_memory": True,   # Conservative default from File 1
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
        """
        Memory-efficient groupby with reliable and consistent output.
        
        Always returns a flat DataFrame with groupby columns as regular columns.
        This fixes the unreliable reset_index logic from the original File 1.
        """
        if self.auto_optimize:
            df = self.optimize_dtypes(df, inplace=True)

        # Use as_index=False for predictable, consistent schema
        result = df.groupby(
            groupby_cols,
            sort=sort_result,
            observed=True,
            as_index=False
        ).agg(agg_funcs)

        return result

    def efficient_merge(
        self,
        left: pd.DataFrame,
        right: pd.DataFrame,
        on: Union[str, List[str]] = None,
        how: str = "inner",
        **kwargs
    ) -> pd.DataFrame:
        """Memory-friendly merge with smart categorical key optimization."""
        if self.auto_optimize:
            left = self.optimize_dtypes(left, inplace=False)
            right = self.optimize_dtypes(right, inplace=False)

        # Smarter join key optimization (from File 2)
        if on:
            keys = [on] if isinstance(on, str) else on
            for col in keys:
                if col in left.columns and col in right.columns:
                    if (DataUtils.should_be_categorical(left[col]) and
                        DataUtils.should_be_categorical(right[col])):
                        left[col] = left[col].astype("category")
                        right[col] = right[col].astype("category")

        result = pd.merge(left, right, on=on, how=how, **kwargs)

        logger.info(f"Merge complete: {len(result)} rows")
        return result

    def suggest_optimizations(self, df: pd.DataFrame) -> List[str]:
        """Suggest dtype optimizations for the given DataFrame."""
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
        """Return basic profile of the DataFrame."""
        return DataUtils.profile_dataframe(df)

    def get_stats(self) -> Dict[str, Any]:
        return self.stats.copy()


# ---------------------------------------------------------------------------
# Chunker
# ---------------------------------------------------------------------------

class DataFrameChunker:
    """Utility for chunking DataFrames in memory."""

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
    """Factory function to create PandasOptimizer."""
    return PandasOptimizer(max_memory_mb, chunk_size, auto_optimize)


def optimize_csv_reading(file_path: Union[str, Path], **kwargs) -> pd.DataFrame:
    """Read CSV with automatic dtype optimization and chunk fallback."""
    optimizer = PandasOptimizer(auto_optimize=True)

    try:
        df = pd.read_csv(file_path, **kwargs)
        return optimizer.optimize_dtypes(df)
    except MemoryError:
        logger.warning("MemoryError on direct read. Falling back to chunked mode.")
        chunks = list(optimizer.process_in_chunks(file_path, lambda x: x, **kwargs))
        return pd.concat(chunks, ignore_index=True)


def get_memory_efficient_dtypes(df: pd.DataFrame) -> Dict[str, str]:
    """Return current dtype mapping of the DataFrame."""
    return {col: str(df[col].dtype) for col in df.columns}


# ---------------------------------------------------------------------------
# Local Test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import itertools

    print("Testing merged pandas optimizer (with fixed efficient_groupby)...")

    # Create test data
    df = pd.DataFrame({
        "id": range(5000),
        "category": list(itertools.islice(itertools.cycle(["A", "B", "C"]), 5000)),
        "value": np.random.randn(5000),
        "score": np.random.randint(0, 100, 5000),
        "group": np.random.choice(["X", "Y", "Z"], 5000),
    })

    optimizer = PandasOptimizer()

    # Original memory
    original_mem = DataUtils.get_dataframe_memory_mb(df)
    print(f"Original DataFrame: {df.shape} | {original_mem:.2f} MB")

    # Optimize
    optimized_df = optimizer.optimize_dtypes(df)
    optimized_mem = DataUtils.get_dataframe_memory_mb(optimized_df)
    print(f"Optimized DataFrame: {optimized_df.shape} | {optimized_mem:.2f} MB")

    print("Optimizer stats:", optimizer.get_stats())

    # Test fixed efficient_groupby
    print("\n--- Testing efficient_groupby ---")
    agg_result = optimizer.efficient_groupby(
        df=optimized_df,
        groupby_cols=["category", "group"],
        agg_funcs={"value": "mean", "score": ["min", "max"], "id": "count"}
    )
    print(f"Groupby result shape: {agg_result.shape}")
    print("Groupby columns:", list(agg_result.columns))

    # Test other File 1 features
    suggestions = optimizer.suggest_optimizations(df)
    print("\nOptimization suggestions:", suggestions)

    profile = optimizer.get_data_profile(optimized_df)
    print("Data profile keys:", list(profile.keys()))

    # Test chunker
    chunker = DataFrameChunker(chunk_size=1000)
    combined = chunker.process_chunks(optimized_df, lambda x: x)
    print(f"Chunked processing result: {combined.shape}")