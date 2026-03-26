r"""
C:\Economy\Invest\TrendMaster\src\database\batch_operations\__init__.py
Batch operations package - Modular batch processing classes.
Each operation type has its own specialized class for better maintainability.

Architecture:
- BatchProcessor: Main coordinator that delegates to specialized processors
- InsertProcessor: Handles INSERT operations with validation and retry logic
- UpdateProcessor: Handles UPDATE operations with key-based matching
- UpsertProcessor: Handles INSERT...ON DUPLICATE KEY UPDATE and REPLACE operations
- DeleteProcessor: Handles DELETE operations with various criteria options
- BaseBatchProcessor: Common functionality shared by all processors
"""

from .base_processor import BaseBatchProcessor
from .batch_processor import BatchProcessor
from .insert_processor import InsertProcessor
from .update_processor import UpdateProcessor
from .upsert_processor import UpsertProcessor
from .delete_processor import DeleteProcessor

__all__ = [
    'BatchProcessor',       # Main coordinator
    'BaseBatchProcessor',   # Base class with common functionality
    'InsertProcessor',      # Specialized insert operations
    'UpdateProcessor',      # Specialized update operations
    'UpsertProcessor',      # Specialized upsert/replace operations
    'DeleteProcessor',      # Specialized delete operations
]
