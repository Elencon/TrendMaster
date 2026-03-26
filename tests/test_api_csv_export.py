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
        async def fetch_all_async(self):
            # deterministic data so test does not depend on external endpoints
            return {
                'tickers': pd.DataFrame([{'ticker_symbol': 'AAPL', 'close_price': 170.0}]),
                'daily_prices': pd.DataFrame([{'ticker_symbol': 'AAPL', 'close_price': 170.0}])
            }

    api_fetcher = StubAPIDataFetcher()
    success = api_fetcher.save_all_api_data_to_csv(str(output_dir))
    api_fetcher.close()

    assert success is True

    files = sorted([p.name for p in output_dir.glob('*.csv')])
    assert 'tickers.csv' in files
    assert 'daily_prices.csv' in files

    # Monkeypatch APIDataFetcher to use our stub for db_manager route
    import database.data_from_api as data_from_api_module
    original_fetcher = data_from_api_module.APIDataFetcher
    data_from_api_module.APIDataFetcher = StubAPIDataFetcher

    try:
        db_manager = DatabaseManager()
        db_success = db_manager.export_api_data_to_csv(str(output_dir))
        assert db_success is True

        files2 = sorted([p.name for p in output_dir.glob('*.csv')])
        assert files2
    finally:
        data_from_api_module.APIDataFetcher = original_fetcher


if __name__ == "__main__":
    test_api_csv_export(Path('test_output'))