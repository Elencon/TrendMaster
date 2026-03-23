"""
Database utilities package - Modular utility classes.
Each utility type has its own focused class for better maintainability.

Architecture:
- DatabaseUtils: Core database operations (SQL generation, batch execution)
- DataUtils: Data processing and cleaning utilities
- ConfigUtils: Configuration management and validation
- OperationStats: Statistics tracking and reporting
- context_managers: Safe operation and transaction context managers
"""

from .database_utils import DatabaseUtils
from .data_utils import DataUtils
from .config_utils import ConfigUtils
from .operation_stats import OperationStats
from .context_managers import safe_operation, db_transaction, managed_cursor

__all__ = [
    'DatabaseUtils',     # Core database operations
    'DataUtils',         # Data processing and cleaning
    'ConfigUtils',       # Configuration management
    'OperationStats',    # Statistics tracking
    'safe_operation',    # Safe operation context manager
    'db_transaction',    # Database transaction context manager
    'managed_cursor',    # Cursor lifecycle context manager
]