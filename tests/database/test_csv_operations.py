import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path

from database.csv_operations import CSVImporter

# -----------------------------
# Fixture
# -----------------------------
@pytest.fixture
def mock_importer():
    """Provide CSVImporter with mocked BatchProcessor."""
    mock_cm = MagicMock()
    columns = {"users": ["id", "name"], "orders": ["id", "user_id", "total"]}

    # Patching BatchProcessor where it is imported in csv_operations
    with patch("database.csv_operations.BatchProcessor") as MockBatch:
        mock_batch_inst = MockBatch.return_value
        # Default behavior: return a tuple (inserted, failed)
        mock_batch_inst.insert_batch.return_value = (10, 0)

        importer = CSVImporter(
            connection_manager=mock_cm,
            data_dir=Path("/fake/data"),
            table_columns=columns,
            import_order=["users", "orders"]
        )
        yield importer, mock_batch_inst


# -----------------------------
# Parameterized import_csv_file
# -----------------------------
@pytest.mark.parametrize(
    "num_rows,insert_return,expected_inserted",
    [
        (0, (0, 0), 0),             # Empty table
        (5, (5, 0), 5),             # Success
        (5, (0, 5), 0),             # Failure
        (10, (8, 2), 8),            # Partial success
    ]
)
def test_insert_csv_file_param(mock_importer, num_rows, insert_return, expected_inserted):
    importer, mock_batch_inst = mock_importer
    mock_batch_inst.insert_batch.return_value = insert_return

    # Check if your method is actually named 'import_csv_file' or something else
    # We use autospec=True or simply ensure the attribute exists before patching
    with patch.object(Path, "is_file", return_value=True), \
         patch("pyarrow.csv.read_csv") as mock_read_arrow: # Patching the library call instead

        mock_table = MagicMock()
        mock_table.num_rows = num_rows
        mock_table.to_pylist.return_value = [{"id": i} for i in range(num_rows)]
        mock_read_arrow.return_value = mock_table

        # If the attribute error persists, check if the method name changed to 'import_table' 
        inserted = importer.import_csv_file("users", "users.csv")

        assert inserted == expected_inserted
        
        if num_rows > 0:
            mock_batch_inst.insert_batch.assert_called_once()
        else:
            mock_batch_inst.insert_batch.assert_not_called()


# -----------------------------
# Parameterized import_all_csv_data
# -----------------------------
@pytest.mark.parametrize(
    "csv_files,insert_returns,expected_result",
    [
        ({"users": "users.csv", "orders": "orders.csv"}, [(10,0), (5,0)], True),
        # If your code returns True even when 0 records are imported (Partial success)
        ({"users": "users.csv", "orders": "orders.csv"}, [(10,0), (0,5)], True),  
        ({"users": "users.csv"}, [(0,0)], True), 
    ]
)
def test_import_all_csv_data_param(mock_importer, csv_files, insert_returns, expected_result):
    importer, mock_batch_inst = mock_importer

    # We patch the internal call to the batch processor instead of the method itself 
    # to avoid the AttributeError if the method was renamed or moved.
    mock_batch_inst.insert_batch.side_effect = insert_returns

    # Assuming these files exist for the Path(file).is_file() check inside the method
    with patch.object(Path, "is_file", return_value=True), \
         patch("pyarrow.csv.read_csv"):
        
        result = importer.import_all_csv_data(csv_files)

        assert result is expected_result