"""
Test suite for the exceptions package.

Covers:
    - ErrorSeverity / ErrorCategory enums
    - ErrorContext dataclass
    - ETLException base class
    - DatabaseError / ConnectionError / QueryError
    - ValidationError / SchemaValidationError / DataQualityError
    - APIError
    - ProcessingError
    - ConfigurationError / FileSystemError / MemoryError
    - create_database_error / create_validation_error / create_api_error factories
    - handle_etl_exceptions decorator

Run with:
    pytest tests/exceptions/test_exceptions.py -v
"""

import pytest
from datetime import datetime


# ===========================================================================
# ErrorSeverity
# ===========================================================================

class TestErrorSeverity:

    def test_values(self):
        from exceptions.base_exceptions import ErrorSeverity
        assert ErrorSeverity.LOW.value      == "low"
        assert ErrorSeverity.MEDIUM.value   == "medium"
        assert ErrorSeverity.HIGH.value     == "high"
        assert ErrorSeverity.CRITICAL.value == "critical"

    def test_all_members(self):
        from exceptions.base_exceptions import ErrorSeverity
        assert len(ErrorSeverity) == 4


# ===========================================================================
# ErrorCategory
# ===========================================================================

class TestErrorCategory:

    def test_values(self):
        from exceptions.base_exceptions import ErrorCategory
        assert ErrorCategory.DATABASE.value      == "database"
        assert ErrorCategory.API.value           == "api"
        assert ErrorCategory.VALIDATION.value    == "validation"
        assert ErrorCategory.PROCESSING.value    == "processing"
        assert ErrorCategory.CONFIGURATION.value == "configuration"
        assert ErrorCategory.FILE_SYSTEM.value   == "file_system"
        assert ErrorCategory.MEMORY.value        == "memory"
        assert ErrorCategory.NETWORK.value       == "network"
        assert ErrorCategory.AUTHENTICATION.value == "authentication"

    def test_all_members(self):
        from exceptions.base_exceptions import ErrorCategory
        assert len(ErrorCategory) == 9


# ===========================================================================
# ErrorContext
# ===========================================================================

class TestErrorContext:

    def test_defaults(self):
        from exceptions.base_exceptions import ErrorContext
        ctx = ErrorContext()
        assert ctx.operation       == ""
        assert ctx.component       == ""
        assert ctx.table_name      is None
        assert ctx.file_path       is None
        assert ctx.record_count    is None
        assert ctx.additional_data == {}
        assert isinstance(ctx.timestamp, datetime)

    def test_to_dict_keys(self):
        from exceptions.base_exceptions import ErrorContext
        d = ErrorContext().to_dict()
        assert set(d.keys()) == {
            "operation", "component", "table_name", "file_path",
            "record_count", "additional_data", "timestamp",
        }

    def test_to_dict_timestamp_is_iso(self):
        from exceptions.base_exceptions import ErrorContext
        d = ErrorContext().to_dict()
        # Should be parseable as ISO 8601
        datetime.fromisoformat(d["timestamp"])

    def test_additional_data_not_shared(self):
        from exceptions.base_exceptions import ErrorContext
        a = ErrorContext()
        b = ErrorContext()
        a.additional_data["key"] = "value"
        assert "key" not in b.additional_data

    def test_custom_values(self):
        from exceptions.base_exceptions import ErrorContext
        ctx = ErrorContext(
            operation="load",
            component="db_writer",
            table_name="orders",
            file_path="/tmp/data.csv",
            record_count=100,
        )
        d = ctx.to_dict()
        assert d["operation"]    == "load"
        assert d["component"]    == "db_writer"
        assert d["table_name"]   == "orders"
        assert d["file_path"]    == "/tmp/data.csv"
        assert d["record_count"] == 100


# ===========================================================================
# ETLException
# ===========================================================================

class TestETLException:

    def _make(self, **kwargs):
        from exceptions.base_exceptions import ETLException
        return ETLException("test message", **kwargs)

    def test_is_exception(self):
        from exceptions.base_exceptions import ETLException
        assert issubclass(ETLException, Exception)

    def test_default_attributes(self):
        from exceptions.base_exceptions import ETLException, ErrorSeverity, ErrorCategory
        exc = ETLException("oops")
        assert exc.message    == "oops"
        assert exc.error_code == "ETL_UNKNOWN"
        assert exc.severity   == ErrorSeverity.MEDIUM
        assert exc.category   == ErrorCategory.PROCESSING
        assert exc.recovery_suggestions == []
        assert exc.original_exception   is None
        assert exc.traceback_info       is None

    def test_context_defaults_to_empty(self):
        from exceptions.base_exceptions import ETLException, ErrorContext
        exc = ETLException("oops")
        assert isinstance(exc.context, ErrorContext)

    def test_str_contains_error_code_and_message(self):
        exc = self._make()
        s = str(exc)
        assert "[ETL_UNKNOWN]" in s
        assert "test message" in s

    def test_str_includes_operation_and_component(self):
        from exceptions.base_exceptions import ETLException, ErrorContext
        ctx = ErrorContext(operation="load", component="writer")
        exc = ETLException("msg", context=ctx)
        s = str(exc)
        assert "load" in s
        assert "writer" in s

    def test_str_includes_severity_for_high(self):
        from exceptions.base_exceptions import ETLException, ErrorSeverity
        exc = ETLException("msg", severity=ErrorSeverity.HIGH)
        assert "HIGH" in str(exc)

    def test_str_includes_severity_for_critical(self):
        from exceptions.base_exceptions import ETLException, ErrorSeverity
        exc = ETLException("msg", severity=ErrorSeverity.CRITICAL)
        assert "CRITICAL" in str(exc)

    def test_str_no_severity_for_medium(self):
        from exceptions.base_exceptions import ETLException, ErrorSeverity
        exc = ETLException("msg", severity=ErrorSeverity.MEDIUM)
        assert "SEVERITY" not in str(exc)

    def test_to_dict_keys(self):
        exc = self._make()
        d = exc.to_dict()
        assert set(d.keys()) == {
            "error_type", "message", "error_code", "severity",
            "category", "context", "recovery_suggestions",
            "original_exception", "traceback",
        }

    def test_to_dict_error_type_is_class_name(self):
        exc = self._make()
        assert exc.to_dict()["error_type"] == "ETLException"

    def test_to_dict_severity_is_string(self):
        exc = self._make()
        assert isinstance(exc.to_dict()["severity"], str)

    def test_to_dict_original_exception_as_string(self):
        from exceptions.base_exceptions import ETLException
        original = ValueError("root cause")
        exc = ETLException("msg", original_exception=original)
        assert exc.to_dict()["original_exception"] == str(original)

    def test_to_dict_no_original_exception(self):
        exc = self._make()
        assert exc.to_dict()["original_exception"] is None

    def test_recovery_suggestions_stored(self):
        from exceptions.base_exceptions import ETLException
        suggestions = ["Try A", "Try B"]
        exc = ETLException("msg", recovery_suggestions=suggestions)
        assert exc.recovery_suggestions == suggestions

    def test_original_exception_stored(self):
        from exceptions.base_exceptions import ETLException
        original = RuntimeError("boom")
        exc = ETLException("msg", original_exception=original)
        assert exc.original_exception is original


# ===========================================================================
# DatabaseError
# ===========================================================================

class TestDatabaseError:

    def test_default_error_code(self):
        from exceptions.database_exceptions import DatabaseError
        assert DatabaseError("msg").error_code == "DB_ERROR"

    def test_default_category(self):
        from exceptions.database_exceptions import DatabaseError
        from exceptions.base_exceptions import ErrorCategory
        assert DatabaseError("msg").category == ErrorCategory.DATABASE

    def test_default_severity_high(self):
        from exceptions.database_exceptions import DatabaseError
        from exceptions.base_exceptions import ErrorSeverity
        assert DatabaseError("msg").severity == ErrorSeverity.HIGH

    def test_has_default_suggestions(self):
        from exceptions.database_exceptions import DatabaseError
        assert len(DatabaseError("msg").recovery_suggestions) > 0

    def test_connection_info_stored_in_context(self):
        from exceptions.database_exceptions import DatabaseError
        info = {"host": "localhost", "port": 3306}
        exc = DatabaseError("msg", connection_info=info)
        assert exc.context.additional_data["connection_info"] == info

    def test_sql_query_stored_in_context(self):
        from exceptions.database_exceptions import DatabaseError
        exc = DatabaseError("msg", sql_query="SELECT * FROM users")
        assert exc.context.additional_data["sql_query"] == "SELECT * FROM users"

    def test_is_etl_exception(self):
        from exceptions.database_exceptions import DatabaseError
        from exceptions.base_exceptions import ETLException
        assert issubclass(DatabaseError, ETLException)


class TestConnectionError:

    def test_default_error_code(self):
        from exceptions.database_exceptions import ConnectionError
        assert ConnectionError("msg").error_code == "DB_CONNECTION_ERROR"

    def test_severity_is_critical(self):
        from exceptions.database_exceptions import ConnectionError
        from exceptions.base_exceptions import ErrorSeverity
        assert ConnectionError("msg").severity == ErrorSeverity.CRITICAL

    def test_is_database_error(self):
        from exceptions.database_exceptions import ConnectionError, DatabaseError
        assert issubclass(ConnectionError, DatabaseError)

    def test_has_suggestions(self):
        from exceptions.database_exceptions import ConnectionError
        assert len(ConnectionError("msg").recovery_suggestions) > 0


class TestQueryError:

    def test_default_error_code(self):
        from exceptions.database_exceptions import QueryError
        assert QueryError("msg").error_code == "DB_QUERY_ERROR"

    def test_is_database_error(self):
        from exceptions.database_exceptions import QueryError, DatabaseError
        assert issubclass(QueryError, DatabaseError)

    def test_has_suggestions(self):
        from exceptions.database_exceptions import QueryError
        assert len(QueryError("msg").recovery_suggestions) > 0


# ===========================================================================
# ValidationError
# ===========================================================================

class TestValidationError:

    def test_default_error_code(self):
        from exceptions.validation_exceptions import ValidationError
        assert ValidationError("msg").error_code == "VALIDATION_ERROR"

    def test_default_category(self):
        from exceptions.validation_exceptions import ValidationError
        from exceptions.base_exceptions import ErrorCategory
        assert ValidationError("msg").category == ErrorCategory.VALIDATION

    def test_default_severity_medium(self):
        from exceptions.validation_exceptions import ValidationError
        from exceptions.base_exceptions import ErrorSeverity
        assert ValidationError("msg").severity == ErrorSeverity.MEDIUM

    def test_failed_records_stored(self):
        from exceptions.validation_exceptions import ValidationError
        records = [{"id": 1, "name": None}]
        exc = ValidationError("msg", failed_records=records)
        assert exc.context.additional_data["failed_records"] == records

    def test_validation_rules_stored(self):
        from exceptions.validation_exceptions import ValidationError
        rules = ["not_null", "min_length"]
        exc = ValidationError("msg", validation_rules=rules)
        assert exc.context.additional_data["validation_rules"] == rules

    def test_has_suggestions(self):
        from exceptions.validation_exceptions import ValidationError
        assert len(ValidationError("msg").recovery_suggestions) > 0


class TestSchemaValidationError:

    def test_default_error_code(self):
        from exceptions.validation_exceptions import SchemaValidationError
        assert SchemaValidationError("msg").error_code == "SCHEMA_VALIDATION_ERROR"

    def test_is_validation_error(self):
        from exceptions.validation_exceptions import SchemaValidationError, ValidationError
        assert issubclass(SchemaValidationError, ValidationError)

    def test_has_suggestions(self):
        from exceptions.validation_exceptions import SchemaValidationError
        assert len(SchemaValidationError("msg").recovery_suggestions) > 0


class TestDataQualityError:

    def test_default_error_code(self):
        from exceptions.validation_exceptions import DataQualityError
        assert DataQualityError("msg").error_code == "DATA_QUALITY_ERROR"

    def test_is_validation_error(self):
        from exceptions.validation_exceptions import DataQualityError, ValidationError
        assert issubclass(DataQualityError, ValidationError)

    def test_has_suggestions(self):
        from exceptions.validation_exceptions import DataQualityError
        assert len(DataQualityError("msg").recovery_suggestions) > 0


# ===========================================================================
# APIError
# ===========================================================================

class TestAPIError:

    def test_default_error_code(self):
        from exceptions.api_exceptions import APIError
        assert APIError("msg").error_code == "API_ERROR"

    def test_default_category(self):
        from exceptions.api_exceptions import APIError
        from exceptions.base_exceptions import ErrorCategory
        assert APIError("msg").category == ErrorCategory.API

    def test_status_500_gives_high_severity(self):
        from exceptions.api_exceptions import APIError
        from exceptions.base_exceptions import ErrorSeverity
        assert APIError("msg", status_code=500).severity == ErrorSeverity.HIGH

    def test_status_400_gives_medium_severity(self):
        from exceptions.api_exceptions import APIError
        from exceptions.base_exceptions import ErrorSeverity
        assert APIError("msg", status_code=400).severity == ErrorSeverity.MEDIUM

    def test_status_200_gives_low_severity(self):
        from exceptions.api_exceptions import APIError
        from exceptions.base_exceptions import ErrorSeverity
        assert APIError("msg", status_code=200).severity == ErrorSeverity.LOW

    def test_no_status_gives_medium_severity(self):
        from exceptions.api_exceptions import APIError
        from exceptions.base_exceptions import ErrorSeverity
        assert APIError("msg").severity == ErrorSeverity.MEDIUM

    def test_status_code_stored_in_context(self):
        from exceptions.api_exceptions import APIError
        exc = APIError("msg", status_code=404)
        assert exc.context.additional_data["status_code"] == 404

    def test_endpoint_stored_in_context(self):
        from exceptions.api_exceptions import APIError
        exc = APIError("msg", endpoint="/api/customers")
        assert exc.context.additional_data["endpoint"] == "/api/customers"

    def test_response_data_stored_in_context(self):
        from exceptions.api_exceptions import APIError
        exc = APIError("msg", response_data={"error": "not found"})
        assert exc.context.additional_data["response_data"] == {"error": "not found"}

    def test_401_suggestion_prepended(self):
        from exceptions.api_exceptions import APIError
        exc = APIError("msg", status_code=401)
        assert "token" in exc.recovery_suggestions[0].lower()

    def test_403_suggestion_prepended(self):
        from exceptions.api_exceptions import APIError
        exc = APIError("msg", status_code=403)
        assert "permission" in exc.recovery_suggestions[0].lower()

    def test_404_suggestion_prepended(self):
        from exceptions.api_exceptions import APIError
        exc = APIError("msg", status_code=404)
        assert "endpoint" in exc.recovery_suggestions[0].lower()

    def test_429_suggestion_prepended(self):
        from exceptions.api_exceptions import APIError
        exc = APIError("msg", status_code=429)
        assert "rate" in exc.recovery_suggestions[0].lower()

    def test_500_suggestion_prepended(self):
        from exceptions.api_exceptions import APIError
        exc = APIError("msg", status_code=503)
        assert "server" in exc.recovery_suggestions[0].lower() or "retry" in exc.recovery_suggestions[0].lower()

    def test_has_base_suggestions(self):
        from exceptions.api_exceptions import APIError
        assert len(APIError("msg").recovery_suggestions) > 0

    def test_is_etl_exception(self):
        from exceptions.api_exceptions import APIError
        from exceptions.base_exceptions import ETLException
        assert issubclass(APIError, ETLException)


# ===========================================================================
# ProcessingError
# ===========================================================================

class TestProcessingError:

    def test_default_error_code(self):
        from exceptions.processing_exceptions import ProcessingError
        assert ProcessingError("msg").error_code == "PROCESSING_ERROR"

    def test_default_category(self):
        from exceptions.processing_exceptions import ProcessingError
        from exceptions.base_exceptions import ErrorCategory
        assert ProcessingError("msg").category == ErrorCategory.PROCESSING

    def test_default_severity_medium(self):
        from exceptions.processing_exceptions import ProcessingError
        from exceptions.base_exceptions import ErrorSeverity
        assert ProcessingError("msg").severity == ErrorSeverity.MEDIUM

    def test_processing_stage_stored(self):
        from exceptions.processing_exceptions import ProcessingError
        exc = ProcessingError("msg", processing_stage="transform")
        assert exc.context.additional_data["processing_stage"] == "transform"

    def test_no_processing_stage_no_key(self):
        from exceptions.processing_exceptions import ProcessingError
        exc = ProcessingError("msg")
        assert "processing_stage" not in exc.context.additional_data

    def test_has_suggestions(self):
        from exceptions.processing_exceptions import ProcessingError
        assert len(ProcessingError("msg").recovery_suggestions) > 0


# ===========================================================================
# ConfigurationError
# ===========================================================================

class TestConfigurationError:

    def test_default_error_code(self):
        from exceptions.system_exceptions import ConfigurationError
        assert ConfigurationError("msg").error_code == "CONFIG_ERROR"

    def test_default_category(self):
        from exceptions.system_exceptions import ConfigurationError
        from exceptions.base_exceptions import ErrorCategory
        assert ConfigurationError("msg").category == ErrorCategory.CONFIGURATION

    def test_default_severity_high(self):
        from exceptions.system_exceptions import ConfigurationError
        from exceptions.base_exceptions import ErrorSeverity
        assert ConfigurationError("msg").severity == ErrorSeverity.HIGH

    def test_config_section_stored(self):
        from exceptions.system_exceptions import ConfigurationError
        exc = ConfigurationError("msg", config_section="database")
        assert exc.context.additional_data["config_section"] == "database"

    def test_invalid_keys_stored(self):
        from exceptions.system_exceptions import ConfigurationError
        exc = ConfigurationError("msg", invalid_keys=["host", "port"])
        assert exc.context.additional_data["invalid_keys"] == ["host", "port"]

    def test_has_suggestions(self):
        from exceptions.system_exceptions import ConfigurationError
        assert len(ConfigurationError("msg").recovery_suggestions) > 0


# ===========================================================================
# FileSystemError
# ===========================================================================

class TestFileSystemError:

    def test_default_error_code(self):
        from exceptions.system_exceptions import FileSystemError
        assert FileSystemError("msg").error_code == "FILE_SYSTEM_ERROR"

    def test_default_category(self):
        from exceptions.system_exceptions import FileSystemError
        from exceptions.base_exceptions import ErrorCategory
        assert FileSystemError("msg").category == ErrorCategory.FILE_SYSTEM

    def test_default_severity_medium(self):
        from exceptions.system_exceptions import FileSystemError
        from exceptions.base_exceptions import ErrorSeverity
        assert FileSystemError("msg").severity == ErrorSeverity.MEDIUM

    def test_file_path_stored_in_context(self):
        from exceptions.system_exceptions import FileSystemError
        exc = FileSystemError("msg", file_path="/data/file.csv")
        assert exc.context.file_path == "/data/file.csv"

    def test_no_file_path_context_file_path_none(self):
        from exceptions.system_exceptions import FileSystemError
        assert FileSystemError("msg").context.file_path is None

    def test_has_suggestions(self):
        from exceptions.system_exceptions import FileSystemError
        assert len(FileSystemError("msg").recovery_suggestions) > 0


# ===========================================================================
# MemoryError
# ===========================================================================

class TestMemoryError:

    def test_default_error_code(self):
        from exceptions.system_exceptions import MemoryError
        assert MemoryError("msg").error_code == "MEMORY_ERROR"

    def test_default_category(self):
        from exceptions.system_exceptions import MemoryError
        from exceptions.base_exceptions import ErrorCategory
        assert MemoryError("msg").category == ErrorCategory.MEMORY

    def test_default_severity_high(self):
        from exceptions.system_exceptions import MemoryError
        from exceptions.base_exceptions import ErrorSeverity
        assert MemoryError("msg").severity == ErrorSeverity.HIGH

    def test_memory_usage_stored(self):
        from exceptions.system_exceptions import MemoryError
        exc = MemoryError("msg", memory_usage_mb=512.0)
        assert exc.context.additional_data["memory_usage_mb"] == 512.0

    def test_zero_memory_usage_stored(self):
        """Ensure 0.0 is stored — not skipped by a falsy check."""
        from exceptions.system_exceptions import MemoryError
        exc = MemoryError("msg", memory_usage_mb=0.0)
        assert exc.context.additional_data["memory_usage_mb"] == 0.0

    def test_no_memory_usage_no_key(self):
        from exceptions.system_exceptions import MemoryError
        assert "memory_usage_mb" not in MemoryError("msg").context.additional_data

    def test_has_suggestions(self):
        from exceptions.system_exceptions import MemoryError
        assert len(MemoryError("msg").recovery_suggestions) > 0


# ===========================================================================
# create_database_error
# ===========================================================================

class TestCreateDatabaseError:

    def test_returns_database_error_by_default(self):
        from exceptions.exception_factories import create_database_error
        from exceptions.database_exceptions import DatabaseError
        assert isinstance(create_database_error("msg"), DatabaseError)

    def test_connection_keyword_returns_connection_error(self):
        from exceptions.exception_factories import create_database_error
        from exceptions.database_exceptions import ConnectionError
        exc = create_database_error("msg", original_exception=Exception("connection refused"))
        assert isinstance(exc, ConnectionError)

    def test_connect_keyword_returns_connection_error(self):
        from exceptions.exception_factories import create_database_error
        from exceptions.database_exceptions import ConnectionError
        exc = create_database_error("msg", original_exception=Exception("failed to connect"))
        assert isinstance(exc, ConnectionError)

    def test_syntax_keyword_returns_query_error(self):
        from exceptions.exception_factories import create_database_error
        from exceptions.database_exceptions import QueryError
        exc = create_database_error("msg", original_exception=Exception("SQL syntax error"))
        assert isinstance(exc, QueryError)

    def test_query_keyword_returns_query_error(self):
        from exceptions.exception_factories import create_database_error
        from exceptions.database_exceptions import QueryError
        exc = create_database_error("msg", original_exception=Exception("query failed"))
        assert isinstance(exc, QueryError)

    def test_original_exception_attached(self):
        from exceptions.exception_factories import create_database_error
        original = RuntimeError("boom")
        exc = create_database_error("msg", original_exception=original)
        assert exc.original_exception is original

    def test_no_original_exception_returns_database_error(self):
        from exceptions.exception_factories import create_database_error
        from exceptions.database_exceptions import DatabaseError
        exc = create_database_error("msg")
        assert type(exc) is DatabaseError


# ===========================================================================
# create_validation_error
# ===========================================================================

class TestCreateValidationError:

    def test_general_returns_validation_error(self):
        from exceptions.exception_factories import create_validation_error
        from exceptions.validation_exceptions import ValidationError
        assert type(create_validation_error("msg")) is ValidationError

    def test_schema_returns_schema_error(self):
        from exceptions.exception_factories import create_validation_error
        from exceptions.validation_exceptions import SchemaValidationError
        assert isinstance(create_validation_error("msg", "schema"), SchemaValidationError)

    def test_data_quality_returns_data_quality_error(self):
        from exceptions.exception_factories import create_validation_error
        from exceptions.validation_exceptions import DataQualityError
        assert isinstance(create_validation_error("msg", "data_quality"), DataQualityError)

    def test_unknown_type_returns_validation_error(self):
        from exceptions.exception_factories import create_validation_error
        from exceptions.validation_exceptions import ValidationError
        assert type(create_validation_error("msg", "unknown")) is ValidationError


# ===========================================================================
# create_api_error
# ===========================================================================

class TestCreateAPIError:

    def test_returns_api_error(self):
        from exceptions.exception_factories import create_api_error
        from exceptions.api_exceptions import APIError
        assert isinstance(create_api_error("msg"), APIError)

    def test_401_gets_unauthorized_code(self):
        from exceptions.exception_factories import create_api_error
        assert create_api_error("msg", status_code=401).error_code == "API_UNAUTHORIZED"

    def test_403_gets_forbidden_code(self):
        from exceptions.exception_factories import create_api_error
        assert create_api_error("msg", status_code=403).error_code == "API_FORBIDDEN"

    def test_404_gets_not_found_code(self):
        from exceptions.exception_factories import create_api_error
        assert create_api_error("msg", status_code=404).error_code == "API_NOT_FOUND"

    def test_429_gets_rate_limited_code(self):
        from exceptions.exception_factories import create_api_error
        assert create_api_error("msg", status_code=429).error_code == "API_RATE_LIMITED"

    def test_500_gets_server_error_code(self):
        from exceptions.exception_factories import create_api_error
        assert create_api_error("msg", status_code=500).error_code == "API_SERVER_ERROR"

    def test_503_gets_server_error_code(self):
        from exceptions.exception_factories import create_api_error
        assert create_api_error("msg", status_code=503).error_code == "API_SERVER_ERROR"

    def test_unknown_4xx_gets_http_code(self):
        from exceptions.exception_factories import create_api_error
        assert create_api_error("msg", status_code=418).error_code == "API_HTTP_418"

    def test_no_status_code_default_error_code(self):
        from exceptions.exception_factories import create_api_error
        assert create_api_error("msg").error_code == "API_ERROR"

    def test_explicit_error_code_not_overridden(self):
        from exceptions.exception_factories import create_api_error
        exc = create_api_error("msg", status_code=404, error_code="MY_CODE")
        assert exc.error_code == "MY_CODE"


# ===========================================================================
# handle_etl_exceptions decorator
# ===========================================================================

class TestHandleETLExceptions:

    def _wrap(self, func, operation="op", component="comp"):
        from exceptions.decorators import handle_etl_exceptions
        return handle_etl_exceptions(operation, component)(func)

    def test_returns_value_on_success(self):
        wrapped = self._wrap(lambda: 42)
        assert wrapped() == 42

    def test_reraises_etl_exception_unchanged(self):
        from exceptions.base_exceptions import ETLException
        original = ETLException("already etl")

        @self._wrap
        def func():
            raise original

        with pytest.raises(ETLException) as exc_info:
            func()
        assert exc_info.value is original

    def test_db_keyword_raises_database_error(self):
        from exceptions.database_exceptions import DatabaseError

        wrapped = self._wrap(lambda: (_ for _ in ()).throw(Exception("mysql connection failed")))

        with pytest.raises(DatabaseError):
            wrapped()

    def test_api_keyword_raises_api_error(self):
        from exceptions.api_exceptions import APIError

        wrapped = self._wrap(lambda: (_ for _ in ()).throw(Exception("http request failed")))

        with pytest.raises(APIError):
            wrapped()

    def test_file_keyword_raises_file_system_error(self):
        from exceptions.system_exceptions import FileSystemError

        wrapped = self._wrap(lambda: (_ for _ in ()).throw(Exception("file not found")))

        with pytest.raises(FileSystemError):
            wrapped()

    def test_memory_keyword_raises_memory_error(self):
        from exceptions.system_exceptions import MemoryError

        wrapped = self._wrap(lambda: (_ for _ in ()).throw(Exception("memory allocation failed")))

        with pytest.raises(MemoryError):
            wrapped()

    def test_generic_exception_raises_processing_error(self):
        from exceptions.processing_exceptions import ProcessingError

        wrapped = self._wrap(lambda: (_ for _ in ()).throw(Exception("something went wrong")))

        with pytest.raises(ProcessingError):
            wrapped()

    def test_context_operation_set(self):
        from exceptions.processing_exceptions import ProcessingError

        @self._wrap
        def func():
            raise Exception("boom")

        with pytest.raises(ProcessingError) as exc_info:
            func()
        assert exc_info.value.context.operation == "op"

    def test_context_component_set(self):
        from exceptions.processing_exceptions import ProcessingError

        @self._wrap
        def func():
            raise Exception("boom")

        with pytest.raises(ProcessingError) as exc_info:
            func()
        assert exc_info.value.context.component == "comp"

    def test_preserves_function_name(self):
        from exceptions.decorators import handle_etl_exceptions

        @handle_etl_exceptions("op", "comp")
        def my_function():
            pass

        assert my_function.__name__ == "my_function"

    def test_preserves_function_docstring(self):
        from exceptions.decorators import handle_etl_exceptions

        @handle_etl_exceptions("op", "comp")
        def my_function():
            """My docstring."""
            pass

        assert my_function.__doc__ == "My docstring."

    def test_passes_args_and_kwargs(self):
        from exceptions.decorators import handle_etl_exceptions

        @handle_etl_exceptions("op")
        def add(a, b, *, factor=1):
            return (a + b) * factor

        assert add(2, 3, factor=2) == 10

    def test_original_exception_chained(self):
        from exceptions.processing_exceptions import ProcessingError

        original = RuntimeError("root cause")

        @self._wrap
        def func():
            raise original

        with pytest.raises(ProcessingError) as exc_info:
            func()
        assert exc_info.value.original_exception is original


# ===========================================================================
# Exception hierarchy
# ===========================================================================

class TestExceptionHierarchy:
    """Ensure the inheritance tree is correct throughout the package."""

    def test_all_exceptions_inherit_from_etl_exception(self):
        from exceptions.base_exceptions import ETLException
        from exceptions.database_exceptions import DatabaseError, ConnectionError, QueryError
        from exceptions.validation_exceptions import ValidationError, SchemaValidationError, DataQualityError
        from exceptions.api_exceptions import APIError
        from exceptions.processing_exceptions import ProcessingError
        from exceptions.system_exceptions import ConfigurationError, FileSystemError, MemoryError

        for cls in (
            DatabaseError, ConnectionError, QueryError,
            ValidationError, SchemaValidationError, DataQualityError,
            APIError, ProcessingError,
            ConfigurationError, FileSystemError, MemoryError,
        ):
            assert issubclass(cls, ETLException), f"{cls.__name__} does not inherit from ETLException"

    def test_connection_error_is_database_error(self):
        from exceptions.database_exceptions import ConnectionError, DatabaseError
        assert issubclass(ConnectionError, DatabaseError)

    def test_query_error_is_database_error(self):
        from exceptions.database_exceptions import QueryError, DatabaseError
        assert issubclass(QueryError, DatabaseError)

    def test_schema_error_is_validation_error(self):
        from exceptions.validation_exceptions import SchemaValidationError, ValidationError
        assert issubclass(SchemaValidationError, ValidationError)

    def test_data_quality_error_is_validation_error(self):
        from exceptions.validation_exceptions import DataQualityError, ValidationError
        assert issubclass(DataQualityError, ValidationError)


# ===========================================================================
# __init__ public API
# ===========================================================================

class TestPackagePublicAPI:
    """Ensure everything listed in __all__ is importable from the package root."""

    def test_all_exports_importable(self):
        import exceptions
        for name in exceptions.__all__:
            assert hasattr(exceptions, name), f"{name} missing from exceptions package"

    def test_can_catch_by_base_class(self):
        from exceptions import ETLException, DatabaseError
        with pytest.raises(ETLException):
            raise DatabaseError("test")


# ===========================================================================
# Entry point
# ===========================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])