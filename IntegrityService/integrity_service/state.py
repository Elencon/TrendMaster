# integrity_service/state.py

import json
from pathlib import Path
from datetime import datetime

# State directory (will be created next to your code)
STATE_DIR = Path(__file__).parent / "state"
STATE_DIR.mkdir(exist_ok=True)


def save_last_run(check_type: str, report: dict):
    """Save integrity check result"""
    data = {
        "timestamp": datetime.now().isoformat(),
        "check_type": check_type,
        "success": report.get("success", False),
        "duration_sec": report.get("duration_sec"),
        "details": report.get("details", {})
    }

    file_path = STATE_DIR / f"last_{check_type}_run.json"

    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Warning: Could not save last run state: {e}")


def save_last_backup(backup_path: str):
    """Save last backup information"""
    data = {
        "timestamp": datetime.now().isoformat(),
        "backup_path": str(backup_path)
    }

    file_path = STATE_DIR / "last_backup.json"

    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Warning: Could not save backup state: {e}")