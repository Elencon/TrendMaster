import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
import pandas as pd

from database.csv_operations import CSVImporter

# -----------------------------
# Fixture
# -----------------------------
@pytest.fixture
def mock_importer():
    """Provide CSVImporter with mocked BatchProcessor."""
    mock_cm = MagicMock()
    # Note these columns
    columns = {"users": ["id", "name"], "orders": ["id", "user_id", "total"]}

    with patch("database.csv_operations.BatchProcessor") as MockBatch:
        mock_batch_inst = MockBatch.return_value
        mock_batch_inst.insert_batch.return_value = (10, 0)
        mock_batch_inst.get_stats.return_value = {"inserted": 0}

        importer = CSVImporter(
            connection_manager=mock_cm,
            data_dir=Path("/fake/data"),
            table_columns=columns,
        )
        # Ensure instance uses the mock
        importer._batch_processor = mock_batch_inst
        
        yield importer, mock_batch_inst

# -----------------------------
# Parameterized import_csv_file
# -----------------------------
@pytest.mark.parametrize(
    "num_rows,insert_return,expected_inserted",
    [
        (0, (0, 0), 0),             
        (5, (5, 0), 5),             
        (5, (0, 5), 0),             
        (10, (8, 2), 8),            
    ]
)
def test_insert_csv_file_param(mock_importer, num_rows, insert_return, expected_inserted):
    importer, mock_batch_inst = mock_importer
    mock_batch_inst.insert_batch.return_value = insert_return

    # Fix: Ensure the DF has columns matching the schema ['id', 'name']
    mock_df = pd.DataFrame([{"id": i, "name": f"user_{i}"} for i in range(num_rows)])

    with patch.object(Path, "exists", return_value=True), \
         patch("pandas.read_csv", return_value=mock_df):

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
        ({"users": "users.csv", "orders": "orders.csv"}, [(10, 0), (5, 0)], True),
        ({"users": "users.csv", "orders": "orders.csv"}, [(10, 0), (0, 5)], False),
        ({"users": "users.csv"}, [(0, 0)], False),
    ]
)
def test_import_all_csv_data_param(mock_importer, csv_files, insert_returns, expected_result):
    importer, mock_batch_inst = mock_importer
    mock_batch_inst.insert_batch.side_effect = insert_returns

    # Fix: Use "id" as a common column so DataUtils doesn't return an empty list
    dummy_df = pd.DataFrame([{"id": 1}])

    with patch.object(Path, "exists", return_value=True), \
         patch("pandas.read_csv", return_value=dummy_df):

        result = importer.import_all_csv_data(csv_files, import_order=list(csv_files.keys()))

        assert result is expected_result