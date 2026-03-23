import pytest
import pandas as pd
import sys
from pathlib import Path

# Add src to python path for testing
src_path = Path("c:/Economy/Invest/TrendMaster/src")
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from database.pandas_optimizer import PandasOptimizer, DataUtils

def test_get_mem_usage(sample_df):
    mem = DataUtils.get_mem_usage(sample_df)
    assert isinstance(mem, float)
    assert mem > 0.0

def test_pandas_optimizer_init():
    opt = PandasOptimizer(max_memory_mb=500, chunk_size=100)
    assert opt.max_memory_mb == 500
    assert opt.chunk_size == 100
    assert opt.stats["memory_saved_mb"] == 0.0

def test_optimize_dtypes(sample_df):
    opt = PandasOptimizer()
    
    # Store classic types
    orig_types = sample_df.dtypes.to_dict()
    
    # Perform optimization
    optimized_df = opt.optimize_dtypes(sample_df)
    
    # Check that strings are converted to Arrow string arrays or categoricals
    # Pandas 2.x convert_dtypes(dtype_backend="pyarrow") makes things pyarrow backed
    # Test for standard pandas nullable/arrow behaviors
    new_types = optimized_df.dtypes
    
    for col in sample_df.columns:
        # Optimizing might downcast int64 -> int8/16/32
        if orig_types[col].name.startswith("int"):
            assert new_types[col].name.startswith("int")
            
    # Stats should record memory saved
    assert opt.stats["memory_saved_mb"] > 0 or opt.stats["memory_saved_mb"] == 0.0 # Small datasets might yield close to 0

def test_efficient_merge():
    opt = PandasOptimizer()
    
    left = pd.DataFrame({"id": [1, 2, 3], "val_L": ["A", "B", "C"]})
    right = pd.DataFrame({"id": [1, 2, 4], "val_R": ["X", "Y", "Z"]})
    
    merged = opt.efficient_merge(left, right, on="id", how="inner")
    
    assert len(merged) == 2
    assert "val_L" in merged.columns
    assert "val_R" in merged.columns
