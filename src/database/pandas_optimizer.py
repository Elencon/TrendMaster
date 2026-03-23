"""
Compact pandas optimization utilities for memory-efficient ETL operations.
"""

import gc
import logging
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Union

import numpy as np
import pandas as pd

try:
    import psutil
    _PSUTIL_AVAILABLE = True
except ImportError:
    _PSUTIL_AVAILABLE = False

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal utilities (not part of public interface)
# ---------------------------------------------------------------------------

class DataUtils:
    @staticmethod
    def get_dataframe_memory_mb(df: pd.DataFrame) -> float:
        try:
            return df.memory_usage(deep=True).sum() / 1024 / 1024
        except Exception:
            return 0.0

    @staticmethod
    def should_be_categorical(series: pd.Series, threshold: float = 0.5) -> bool:
        try:
            if len(series) == 0:
                return False
            return series.nunique() / len(series) < threshold
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
        stats[key] = stats.get(key, 0) + value

    @staticmethod
    def force_cleanup() -> None:
        gc.collect()


# ---------------------------------------------------------------------------
# Public classes
# ---------------------------------------------------------------------------

class PandasOptimizer:
    """Memory-efficient pandas operations with automatic optimization."""

    def __init__(
        self,
        max_memory_mb: float = 512,
        chunk_size: int = 10_000,
        auto_optimize: bool = True,
    ) -> None:
        self.max_memory_mb = max_memory_mb
        self.chunk_size = chunk_size
        self.auto_optimize = auto_optimize
        self.stats = DataUtils.create_stats_tracker()

    # ------------------------------------------------------------------
    # Memory helpers
    # ------------------------------------------------------------------

    def get_memory_usage_mb(self) -> float:
        """Return current process RSS memory in MB."""
        if _PSUTIL_AVAILABLE:
            try:
                return psutil.Process().memory_info().rss / 1024 / 1024
            except Exception:
                pass
        try:
            import resource
            return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        except Exception:
            return 0.0

    # ------------------------------------------------------------------
    # Core optimizations
    # ------------------------------------------------------------------

    def optimize_dtypes(
        self, df: pd.DataFrame, categorical_threshold: float = 0.5
    ) -> pd.DataFrame:
        """Optimize DataFrame dtypes for memory efficiency."""
        if not self.auto_optimize:
            return df

        original_memory = DataUtils.get_dataframe_memory_mb(df)
        optimized_df = df.copy()

        for col in optimized_df.columns:
            dtype = optimized_df[col].dtype

            if pd.api.types.is_integer_dtype(dtype):
                optimized_df[col] = pd.to_numeric(optimized_df[col], downcast="integer")
            elif pd.api.types.is_float_dtype(dtype):
                optimized_df[col] = pd.to_numeric(optimized_df[col], downcast="float")
            elif dtype == object:
                if DataUtils.should_be_categorical(optimized_df[col], categorical_threshold):
                    optimized_df[col] = optimized_df[col].astype("category")

        optimized_memory = DataUtils.get_dataframe_memory_mb(optimized_df)
        memory_saved = original_memory - optimized_memory

        DataUtils.update_stats(self.stats, "memory_optimized", 1)
        DataUtils.update_stats(self.stats, "memory_saved_mb", memory_saved)

        logger.info(
            "Memory: %.2fMB → %.2fMB (saved %.2fMB)",
            original_memory, optimized_memory, memory_saved,
        )
        return optimized_df

    def process_in_chunks(
        self,
        file_path: Union[str, Path],
        processor: Callable,
        **read_kwargs,
    ) -> Iterator[Any]:
        """Process a large CSV in memory-efficient chunks."""
        read_params = {
            "chunksize": self.chunk_size,
            "low_memory": True,
            "engine": "c",
            **read_kwargs,
        }

        logger.info("Processing %s in chunks of %d", file_path, self.chunk_size)
        total_chunks = total_rows = 0

        try:
            for chunk in pd.read_csv(file_path, **read_params):
                if self.get_memory_usage_mb() > self.max_memory_mb:
                    logger.warning("Memory limit exceeded, forcing cleanup")
                    DataUtils.force_cleanup()

                if self.auto_optimize:
                    chunk = self.optimize_dtypes(chunk)

                result = processor(chunk)
                total_chunks += 1
                total_rows += len(chunk)
                yield result

            DataUtils.update_stats(self.stats, "chunks_processed", total_chunks)
            DataUtils.update_stats(self.stats, "rows_processed", total_rows)
            logger.info("Processed %d chunks, %d total rows", total_chunks, total_rows)

        except Exception:
            logger.error("Chunk processing failed for %s", file_path, exc_info=True)
            raise

    def efficient_groupby(
        self,
        df: pd.DataFrame,
        groupby_cols: List[str],
        agg_funcs: Dict[str, Union[str, List[str]]],
        sort_result: bool = False,
    ) -> pd.DataFrame:
        """Memory-efficient groupby."""
        original_memory = DataUtils.get_dataframe_memory_mb(df)

        if self.auto_optimize:
            df = self.optimize_dtypes(df)

        result = (
            df.groupby(groupby_cols, sort=sort_result, observed=True)
            .agg(agg_funcs)
            .reset_index()
        )

        logger.info(
            "Groupby: %d → %d rows, %.2fMB → %.2fMB",
            len(df), len(result),
            original_memory, DataUtils.get_dataframe_memory_mb(result),
        )
        return result

    def efficient_merge(
        self,
        left: pd.DataFrame,
        right: pd.DataFrame,
        on: Union[str, List[str], None] = None,
        how: str = "inner",
        **kwargs,
    ) -> pd.DataFrame:
        """Memory-efficient merge."""
        logger.debug(
            "Merging: left(%d rows, %.2fMB) %s right(%d rows, %.2fMB)",
            len(left), DataUtils.get_dataframe_memory_mb(left),
            how,
            len(right), DataUtils.get_dataframe_memory_mb(right),
        )

        if self.auto_optimize:
            left = self.optimize_dtypes(left)
            right = self.optimize_dtypes(right)

        result = pd.merge(left, right, on=on, how=how, **kwargs)
        logger.info(
            "Merge complete: %d rows, %.2fMB",
            len(result), DataUtils.get_dataframe_memory_mb(result),
        )
        return result

    # ------------------------------------------------------------------
    # Profiling & suggestions
    # ------------------------------------------------------------------

    def get_data_profile(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Return a comprehensive profile of a DataFrame."""
        return {
            "shape": df.shape,
            "memory_usage_mb": DataUtils.get_dataframe_memory_mb(df),
            "dtypes": df.dtypes.astype(str).to_dict(),
            "null_counts": df.isnull().sum().to_dict(),
            "unique_counts": {col: int(df[col].nunique()) for col in df.columns},
        }

    def suggest_optimizations(self, df: pd.DataFrame) -> List[str]:
        """Return human-readable optimisation suggestions for a DataFrame."""
        suggestions: List[str] = []
        profile = self.get_data_profile(df)

        if profile["memory_usage_mb"] > self.max_memory_mb:
            suggestions.append(
                f"Memory usage ({profile['memory_usage_mb']:.1f}MB) "
                f"exceeds limit ({self.max_memory_mb}MB)"
            )

        total = len(df)
        for col in df.columns:
            dtype = df[col].dtype
            n_unique = profile["unique_counts"][col]

            if dtype == object and total and n_unique / total < 0.5:
                suggestions.append(
                    f"'{col}' could be categorical ({n_unique}/{total} unique)"
                )
            elif pd.api.types.is_integer_dtype(dtype) or pd.api.types.is_float_dtype(dtype):
                suggestions.append(f"'{col}' ({dtype}) could be downcast")

        return suggestions

    def get_stats(self) -> Dict[str, Any]:
        """Return a snapshot of optimizer performance statistics."""
        return self.stats.copy()


class DataFrameChunker:
    """Utility for processing large DataFrames in chunks."""

    def __init__(self, chunk_size: int = 10_000) -> None:
        self.chunk_size = chunk_size

    def chunk_dataframe(self, df: pd.DataFrame) -> Iterator[pd.DataFrame]:
        """Yield successive slices of *df*."""
        total_rows = len(df)
        total_chunks = (total_rows + self.chunk_size - 1) // self.chunk_size
        logger.info("Chunking: %d rows → %d chunks", total_rows, total_chunks)
        for i in range(0, total_rows, self.chunk_size):
            yield df.iloc[i : i + self.chunk_size]

    def process_chunks(
        self,
        df: pd.DataFrame,
        processor: Callable,
        combine_results: bool = True,
    ) -> Union[List[Any], pd.DataFrame]:
        """Apply *processor* to each chunk, optionally concatenating results."""
        results = [processor(chunk) for chunk in self.chunk_dataframe(df)]

        if combine_results and all(isinstance(r, pd.DataFrame) for r in results):
            combined = pd.concat(results, ignore_index=True)
            logger.info("Combined %d chunks → %d rows", len(results), len(combined))
            return combined

        return results


# ---------------------------------------------------------------------------
# Convenience / backward-compat functions
# ---------------------------------------------------------------------------

def optimize_csv_reading(
    file_path: Union[str, Path],
    chunk_size: int = 10_000,
    auto_optimize: bool = True,
    **kwargs,
) -> pd.DataFrame:
    """Read a CSV with automatic dtype optimisation; falls back to chunked read on MemoryError."""
    optimizer = PandasOptimizer(chunk_size=chunk_size, auto_optimize=auto_optimize)

    try:
        df = pd.read_csv(file_path, **kwargs)
        if auto_optimize:
            df = optimizer.optimize_dtypes(df)
        logger.info("Read and optimized %s: %s", file_path, df.shape)
        return df

    except MemoryError:
        logger.warning("MemoryError reading %s, switching to chunked processing", file_path)
        result = pd.concat(
            optimizer.process_in_chunks(file_path, lambda x: x, **kwargs),
            ignore_index=True,
        )
        logger.info("Chunked read complete: %s", result.shape)
        return result


def get_memory_efficient_dtypes(df: pd.DataFrame) -> Dict[str, str]:
    """Return suggested memory-efficient dtype names for each column."""
    suggestions: Dict[str, str] = {}
    for col in df.columns:
        dtype = df[col].dtype
        if pd.api.types.is_integer_dtype(dtype):
            suggestions[col] = str(pd.to_numeric(df[col], downcast="integer").dtype)
        elif pd.api.types.is_float_dtype(dtype):
            suggestions[col] = str(pd.to_numeric(df[col], downcast="float").dtype)
        elif dtype == object and DataUtils.should_be_categorical(df[col]):
            suggestions[col] = "category"
        else:
            suggestions[col] = str(dtype)
    return suggestions


def create_pandas_optimizer(
    max_memory_mb: float = 512,
    chunk_size: int = 10_000,
    auto_optimize: bool = True,
) -> PandasOptimizer:
    """Factory for backward compatibility."""
    return PandasOptimizer(max_memory_mb, chunk_size, auto_optimize)


# ---------------------------------------------------------------------------
# Quick smoke-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Testing compact pandas optimizer...")

    rng = np.random.default_rng(42)
    data = {
        "id": range(1000),
        "category": np.random.choice(["A", "B", "C"], size=1000),
        "value": rng.standard_normal(1000),
        "score": rng.integers(0, 100, 1000),
    }

    df = pd.DataFrame(data)
    optimizer = PandasOptimizer()

    print(f"Original:  {df.shape}, {DataUtils.get_dataframe_memory_mb(df):.2f}MB")
    optimized_df = optimizer.optimize_dtypes(df)
    print(f"Optimized: {optimized_df.shape}, {DataUtils.get_dataframe_memory_mb(optimized_df):.2f}MB")
    print(f"Stats: {optimizer.get_stats()}")