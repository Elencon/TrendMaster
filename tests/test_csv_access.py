"""Test CSV file access after moving to CSV subfolder."""

import sys
from pathlib import Path

# Ensure src package is discoverable when running tests from project root
ROOT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT_DIR / 'src'
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from database.db_manager import DatabaseManager

def test_csv_access():
    print("🔍 Testing CSV file access after reorganization...")
    
    db_manager = DatabaseManager()
    print(f"📁 Data directory: {db_manager.data_dir}")
    
    # Test reading each CSV file
    csv_files_map = getattr(db_manager, 'csv_files', None)
    if csv_files_map is None:
        csv_path = Path(db_manager.data_dir) / 'CSV'
        csv_files_map = {p.stem: p.name for p in csv_path.glob('*.csv')}

    for table_name, csv_file in csv_files_map.items():
        try:
            file_path = Path(db_manager.data_dir) / csv_file
            df = db_manager.read_csv_file(csv_file)
            
            if df is not None:
                print(f"✅ {table_name:12}: {len(df):4} rows, {len(df.columns):2} columns - {csv_file}")
            else:
                print(f"❌ {table_name:12}: Failed to read {csv_file}")
                
        except Exception as e:
            print(f"❌ {table_name:12}: Error - {e}")
    
    print("\n🎉 CSV file reorganization test completed!")

if __name__ == "__main__":
    test_csv_access()