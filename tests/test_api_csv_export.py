"""
Test API data export to CSV files.
"""

import sys
from pathlib import Path
import pandas as pd

# Ensure the src package is discoverable when running tests from project root
ROOT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT_DIR / 'src'
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from database.data_from_api import APIDataFetcher
from database.db_manager import DatabaseManager

def test_api_csv_export(tmp_path):
    """Test the API CSV export functionality using a stubbed API fetcher."""
    print("🧪 Testing API CSV Export Functionality")
    print("="*50)

    output_dir = tmp_path / 'API'
    output_dir.mkdir(parents=True, exist_ok=True)

    class StubAPIDataFetcher(APIDataFetcher):
        def fetch_all_data(self):
            # deterministic data so test does not depend on external endpoints
            return {
                'orders': pd.DataFrame([{'order_id': 1, 'customer_id': 1, 'order_status': 2}]),
                'order_items': pd.DataFrame([{'item_id': 1, 'order_id': 1, 'product_id': 100}]),
                'customers': pd.DataFrame([{'customer_id': 1, 'first_name': 'John'}])
            }

    api_fetcher = StubAPIDataFetcher()
    success = api_fetcher.save_all_api_data_to_csv(str(output_dir))
    api_fetcher.close()

    assert success is True

    files = sorted([p.name for p in output_dir.glob('*.csv')])
    assert 'orders.csv' in files
    assert 'order_items.csv' in files
    assert 'customers.csv' in files

    # Monkeypatch APIDataFetcher to use our stub for db_manager route
    import database.data_from_api as data_from_api_module
    original_fetcher = data_from_api_module.APIDataFetcher
    data_from_api_module.APIDataFetcher = StubAPIDataFetcher

    try:
        db_manager = DatabaseManager(data_dir=tmp_path)
        db_success = db_manager.export_api_data_to_csv()
        assert db_success is True

        files2 = sorted([p.name for p in output_dir.glob('*.csv')])
        assert files2
    finally:
        data_from_api_module.APIDataFetcher = original_fetcher


if __name__ == "__main__":
    test_api_csv_export(Path('test_output'))
