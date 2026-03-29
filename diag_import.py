import sys
import os
from unittest.mock import patch, MagicMock

# Add src to sys.path
sys.path.append(os.path.join(os.getcwd(), 'src'))

print("Diagnostic start")

mock_db_cfg = MagicMock()
mock_db_cfg.to_dict.return_value = {}
mock_cfg = MagicMock()
mock_cfg.database = mock_db_cfg

with patch("src.config.get_config", return_value=mock_cfg):
    print("Patch applied, importing connect...")
    try:
        print("Import successful!")
    except Exception as e:
        print(f"Import failed: {e}")

print("Diagnostic end")
