"""
Cache Cleaner Utility
Automatically clears application cache when run
"""

import shutil
from pathlib import Path
import logging


class CacheCleaner:
    def __init__(self):
        # Get the root project directory (parent of gui folder)
        self.project_root = Path(__file__).parent.parent

        # Cache directories to clean
        self.cache_dirs = [
            self.project_root / "__pycache__",
            self.project_root / "gui" / "__pycache__",
            self.project_root / "src" / "__pycache__",
            self.project_root / "src" / "database" / "__pycache__",
            self.project_root / "tests" / "__pycache__",
        ]

        # Cache file patterns to remove
        self.cache_patterns = [
            "*.pyc",
            "*.pyo",
            "*.pyd",
            ".pytest_cache",
            ".coverage"
        ]

        # Log file patterns (handled separately with better error handling)
        self.log_patterns = ["*.log"]

    def clear_pycache_dirs(self):
        """Remove __pycache__ directories"""
        removed_dirs = []

        for cache_dir in self.cache_dirs:
            if cache_dir.exists():
                try:
                    shutil.rmtree(cache_dir)
                    removed_dirs.append(str(cache_dir))
                except Exception as e:
                    print(f"Warning: Could not remove {cache_dir}: {e}")

        return removed_dirs

    def clear_cache_files(self):
        """Remove cache files by pattern"""
        removed_files = []

        # Search recursively through project
        for pattern in self.cache_patterns:
            for file_path in self.project_root.rglob(pattern):
                try:
                    if file_path.is_file():
                        file_path.unlink()
                        removed_files.append(str(file_path))
                    elif file_path.is_dir():
                        shutil.rmtree(file_path)
                        removed_files.append(str(file_path))
                except Exception as e:
                    print(f"Warning: Could not remove {file_path}: {e}")

        return removed_files

    def clear_log_files(self, force=False):
        """Remove log files with better error handling"""
        removed_files = []
        locked_files = []

        # Try to close any active logging handlers first
        if force:
            try:
                # Get all loggers and close their handlers
                for name in logging.Logger.manager.loggerDict:
                    logger = logging.getLogger(name)
                    for handler in logger.handlers[:]:
                        try:
                            handler.close()
                            logger.removeHandler(handler)
                        except Exception:
                            pass

                # Also close root logger handlers
                root_logger = logging.getLogger()
                for handler in root_logger.handlers[:]:
                    try:
                        handler.close()
                        root_logger.removeHandler(handler)
                    except Exception:
                        pass
            except Exception:
                pass

        # Now try to remove log files
        for pattern in self.log_patterns:
            for file_path in self.project_root.rglob(pattern):
                if file_path.is_file():
                    try:
                        file_path.unlink()
                        removed_files.append(str(file_path))
                    except OSError as e:
                        # Check for 'File in use' error (WinError 32)
                        if hasattr(e, 'winerror') and e.winerror == 32:
                            locked_files.append(str(file_path))
                        else:
                            print(f"Warning: Could not remove {file_path}: {e}")

        return removed_files, locked_files

    def clean_all(self, verbose=True, clean_logs=False, force_logs=False):
        """Clean all cache files and directories"""
        if verbose:
            print("Starting cache cleanup...")

        # Clear __pycache__ directories
        removed_dirs = self.clear_pycache_dirs()

        # Clear cache files
        removed_files = self.clear_cache_files()

        # Handle log files separately
        removed_logs = []
        locked_logs = []
        if clean_logs:
            removed_logs, locked_logs = self.clear_log_files(force=force_logs)

        total_removed = len(removed_dirs) + len(removed_files) + len(removed_logs)

        if verbose:
            if total_removed > 0:
                print(f"Cache cleanup completed: {total_removed} items removed")
                if removed_dirs:
                    print(f"Directories removed: {len(removed_dirs)}")
                if removed_files:
                    print(f"Cache files removed: {len(removed_files)}")
                if removed_logs:
                    print(f"Log files removed: {len(removed_logs)}")
            else:
                print("Cache cleanup completed: No cache files found")

            if locked_logs:
                print(f"Note: {len(locked_logs)} log files are in use and couldn't be removed")
                if verbose and len(locked_logs) <= 5:  # Only show details for a few files
                    for log_file in locked_logs:
                        print(f"  - {Path(log_file).name}")

        return total_removed

    @staticmethod
    def close_logging_handlers():
        """Close all active logging handlers to release log files"""
        try:
            # Get all loggers and close their handlers
            logger_dict = logging.Logger.manager.loggerDict.copy()
            for name in logger_dict:
                # Filter out objects that aren't actual Loggers (like Placeholders)
                if not isinstance(logging.Logger.manager.loggerDict[name], logging.Logger):
                    continue
                
                logger = logging.getLogger(name)
                handlers = logger.handlers[:]
                for handler in handlers:
                    try:
                        handler.close()
                        logger.removeHandler(handler)
                    except Exception:
                        pass

            # Also close root logger handlers
            root_logger = logging.getLogger()
            handlers = root_logger.handlers[:]
            for handler in handlers:
                try:
                    handler.close()
                    root_logger.removeHandler(handler)
                except Exception:
                    pass

            return True
        except Exception:
            return False


def main():
    """Run cache cleaner from command line"""
    import argparse

    parser = argparse.ArgumentParser(description='Clean ETL project cache files')
    parser.add_argument('--logs', action='store_true', help='Also attempt to clean log files')
    parser.add_argument('--force-logs', action='store_true', help='Force close logging handlers before cleaning log files')
    parser.add_argument('--close-loggers', action='store_true', help='Just close logging handlers without cleaning')

    args = parser.parse_args()

    cleaner = CacheCleaner()

    if args.close_loggers:
        print("Closing all logging handlers...")
        success = CacheCleaner.close_logging_handlers()
        print(f"Logging handlers closed: {'Success' if success else 'Failed'}")
        return

    # Regular cleanup
    total_removed = cleaner.clean_all(
        verbose=True,
        clean_logs=args.logs or args.force_logs,
        force_logs=args.force_logs
    )

    if total_removed == 0:
        print("No cache files found to remove.")


if __name__ == "__main__":
    main()