r"""
C:\Economy\Invest\TrendMaster\src\database\__init__.py
Database package - modular components for ETL operations
"""

# Database package - modular components for ETL operations
from .db_manager import DatabaseManager
from .schema_manager import SchemaManager, SCHEMA_DEFINITIONS, TABLE_COLUMNS

# Import modular components
from .utilities import ConfigUtils, DataUtils, DatabaseUtils, OperationStats, safe_operation
from .batch_operations import BatchProcessor
from .csv_operations import CSVImporter

# Import other modules with fallback handling
try:
    from .connection_manager import DatabaseConnection, ConnectionPool
    CONNECTION_MANAGER_AVAILABLE = True
except ImportError:
    CONNECTION_MANAGER_AVAILABLE = False

try:
    from .data_validator import DataValidator, ValidationRule, ValidationResult, ValidationSeverity
    DATA_VALIDATOR_AVAILABLE = True
except ImportError:
    DATA_VALIDATOR_AVAILABLE = False

try:
    from .data_from_api import APIDataFetcher, DataProcessor
    API_DATA_AVAILABLE = True
except ImportError:
    API_DATA_AVAILABLE = False

try:
    from .pandas_optimizer import PandasOptimizer, DataFrameChunker
    PANDAS_OPTIMIZER_AVAILABLE = True
except ImportError:
    PANDAS_OPTIMIZER_AVAILABLE = False

__all__ = [
    # Core modular database management
    'DatabaseManager',
    
    # Modular utilities (from utilities package)
    'ConfigUtils',
    'DataUtils', 
    'DatabaseUtils',
    'OperationStats',
    'safe_operation',
    
    # Specialized processors
    'BatchProcessor',
    'CSVImporter',
    
    # Schema management
    'SchemaManager',
    'SCHEMA_DEFINITIONS',
    'TABLE_COLUMNS',
]

# Add optional components if available
if CONNECTION_MANAGER_AVAILABLE:
    __all__.extend(['DatabaseConnection', 'ConnectionPool'])
    
if DATA_VALIDATOR_AVAILABLE:
    __all__.extend(['DataValidator', 'ValidationRule', 'ValidationResult', 'ValidationSeverity'])
    
if API_DATA_AVAILABLE:
    __all__.extend(['APIDataFetcher', 'DataProcessor'])
    
if PANDAS_OPTIMIZER_AVAILABLE:
    __all__.extend(['PandasOptimizer', 'DataFrameChunker'])