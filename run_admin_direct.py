"""
DEVELOPMENT ONLY - Direct Admin Access (BYPASSES AUTHENTICATION)
WARNING: This script provides direct access to the admin window without login.
For production use, run: python run_app.py

Run:
    python -m run_admin_direct
"""

import os
import sys
from pathlib import Path

# Prevent Python cache files from being created
sys.dont_write_bytecode = True
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"

# ---------------------------------------------------------
# Locate project root (TrendMaster folder)
# ---------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent

# Ensure project root is importable
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Now imports work cleanly
from src.bootstrap import initialize
from src.gui.admin_window import main as gui_main


# ---------------------------------------------------------
# Banner / Demo Info
# ---------------------------------------------------------
def print_banner() -> None:
    print("=" * 60)
    print("WARNING: DEVELOPMENT MODE - AUTHENTICATION BYPASSED")
    print("=" * 60)
    print("\nThis script provides DIRECT ACCESS to the admin window")
    print("without requiring authentication.")
    print("\nFor production use with login, run:")
    print("  python run_app.py")
    print("\n" + "=" * 60)

    print("\nETL Pipeline GUI Demo")
    print("=" * 60)

    print("\nStarting GUI Interface...")
    print("The ETL Pipeline Manager GUI should now be open.")

    print("\nAvailable Features:")
    print("  API Configuration:")
    print("    - Enter an API URL (e.g., https://etl-server.fly.dev)")
    print("    - Click 'Load' to test the API connection")
    print("    - Click 'Load API Data' to download data as CSV")

    print("\n  Database Operations:")
    print("    - Click 'Test DB Connection' to verify MySQL connection")
    print("    - Click 'Create Tables' to set up database structure")

    print("\n  File Management:")
    print("    - Click 'Select CSV Files' to choose files from your computer")
    print("    - Selected files will be copied to the data/CSV folder")
    print("    - Click 'Load CSV Data' to import them into database")

    print("\n  Test Operations:")
    print("    - Click 'Test CSV Access' to verify file reading")
    print("    - Click 'Test API Export' to test API data export")

    print("\nUsage Tips:")
    print("  1. Start by testing your database connection")
    print("  2. Create tables if they don't exist")
    print("  3. Use either CSV files OR API data (or both)")
    print("  4. Monitor the output window for real-time feedback")
    print("  5. All operations run in background threads (non-blocking)")

    print("\nExample API URLs to try:")
    print("  - https://etl-server.fly.dev (default ETL server)")
    print("  - https://jsonplaceholder.typicode.com (demo REST API)")

    print("\nSupported Data Sources:")
    print("  CSV Files: brands, categories, products, staffs, stocks, stores")
    print("  API Endpoints: orders, order_items, customers")

    print("\nTechnical Features:")
    print("  - Multi-threaded operations (UI never freezes)")
    print("  - Real-time progress feedback")
    print("  - Comprehensive error handling")
    print("  - File dialog integration")
    print("  - Database connection testing")
    print("  - Automatic CSV file management")

    print("\nProject Structure:")
    print("  data/CSV/  - Your CSV files go here")
    print("  data/API/  - API downloads saved here")
    print("  gui/       - PySide6 interface code")
    print("  src/       - Core ETL functionality")

    print("\nThe GUI is now running - check your screen!")
    print("Close this terminal or press Ctrl+C to stop the demo.")


# ---------------------------------------------------------
# Main launcher
# ---------------------------------------------------------
def main():
    print_banner()

    print("\n" + "=" * 60)
    print("LAUNCHING GUI INTERFACE...")
    print("=" * 60)

    gui_main()


# ---------------------------------------------------------
# Entry point
# ---------------------------------------------------------
if __name__ == "__main__":
    try:
        # Initialize logging + directories
        initialize()

        print("Attempting to launch GUI interface...")
        main()

    except ImportError as e:
        print_banner()
        print(f"\nERROR: Could not launch GUI: {e}")
        print("\nTo launch manually, run:")
        print("   python gui/admin_window.py")

    except Exception as e:
        print_banner()
        print(f"\nERROR: {e}")
        print("\nMake sure all dependencies are installed:")
        print("   python -m pip install PySide6 pandas PyMySQL requests python-dotenv qt-material cryptography")