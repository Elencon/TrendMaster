"""
Example usage for the ETL exception package.
"""

from .base_exceptions import ErrorContext
from .database_exceptions import DatabaseError
from .decorators import handle_etl_exceptions
from .exception_factories import create_api_error, create_database_error


def run_examples() -> None:
    """Demonstrate the exception hierarchy, factories, and decorator."""
    print("Testing ETL exception system...")

    # --- Basic exception ---
    try:
        raise DatabaseError(
            "Connection failed to MySQL server",
            error_code="DB_CONNECTION_FAILED",
            context=ErrorContext(
                operation="test_operation",
                component="test_component",
                table_name="test_table",
            ),
        )
    except DatabaseError as exc:
        print(f"Caught:              {exc}")
        print(f"Error code:          {exc.error_code}")
        print(f"Severity:            {exc.severity.value}")
        print(f"Category:            {exc.category.value}")
        print(f"Recovery suggestions:{exc.recovery_suggestions}")

    # --- Factory-created exception ---
    try:
        raise create_database_error(
            "Database connection lost during operation",
            original_exception=Exception("MySQL server has gone away"),
        )
    except DatabaseError as exc:
        print(f"\nFactory exception:   {exc}")
        print(f"Original exception:  {exc.original_exception}")

    # --- Decorator-wrapped function ---
    @handle_etl_exceptions("csv_processing", "file_reader")
    def process_csv_file(filename: str) -> None:
        raise FileNotFoundError(f"Could not find file: {filename}")

    try:
        process_csv_file("nonexistent.csv")
    except Exception as exc:
        print(f"\nDecorator exception: {exc}")
        print(f"Context:             {exc.context.to_dict()}")

    # --- API error factory ---
    try:
        raise create_api_error(
            "API request failed",
            status_code=404,
            endpoint="/api/customers",
        )
    except Exception as exc:
        print(f"\nAPI error:           {exc}")
        print(f"Status code context: {exc.context.additional_data}")

    print("\nETL exception system test complete!")


if __name__ == "__main__":
    run_examples()
