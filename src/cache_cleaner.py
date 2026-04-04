"""
Cache Cleaner Utility
Automatically clears Python project cache files safely.
Now removes __pycache__ recursively at all levels.
"""

import shutil
import logging
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table
from rich.progress import track

app = typer.Typer(add_completion=False, help="Clean Python project cache files safely.")
console = Console()

class CacheCleaner:
    def __init__(self):
        # Auto-detect project root reliably
        self.project_root = self._find_project_root(Path(__file__).resolve())

        # Cache file patterns (non-directory)
        self.cache_patterns = [
            "*.pyc",
            "*.pyo",
            "*.pyd",
            ".pytest_cache",
            ".coverage",
        ]

        # Log file patterns
        self.log_patterns = ["*.log"]

    # ---------------------------------------------------------
    # PROJECT ROOT DETECTION
    # ---------------------------------------------------------
    def _find_project_root(self, start: Path) -> Path:
        """Walk upward until we find the project root."""
        required = {"src", "tests", "testers"}

        for parent in [start] + list(start.parents):
            try:
                entries = {p.name for p in parent.iterdir() if p.is_dir()}
                if required.issubset(entries):
                    return parent
            except (PermissionError, OSError):
                continue

        # Fallback
        for parent in [start] + list(start.parents):
            if (parent / "src").exists():
                return parent

        return start

    # ---------------------------------------------------------
    # CLEANING METHODS - FIXED RECURSIVE __pycache__ REMOVAL
    # ---------------------------------------------------------
    def clear_all_pycache_dirs(self):
        """Remove ALL __pycache__ directories recursively at every level"""
        removed_dirs = []

        # Find every __pycache__ directory in the entire project tree
        for cache_dir in track(list(self.project_root.rglob("__pycache__")), 
                              description="Removing all __pycache__ directories..."):
            if cache_dir.exists() and cache_dir.is_dir():
                try:
                    shutil.rmtree(cache_dir)
                    removed_dirs.append(str(cache_dir.relative_to(self.project_root)))
                except Exception as e:
                    console.print(f"[yellow]Warning:[/] Could not remove {cache_dir}: {e}")

        return removed_dirs

    def clear_cache_files(self):
        """Remove cache files and special cache directories (like .pytest_cache)"""
        removed = []

        for pattern in track(self.cache_patterns, description="Removing cache files..."):
            for path in self.project_root.rglob(pattern):
                try:
                    if path.is_file():
                        path.unlink()
                        removed.append(str(path.relative_to(self.project_root)))
                    elif path.is_dir():
                        shutil.rmtree(path)
                        removed.append(str(path.relative_to(self.project_root)))
                except Exception as e:
                    console.print(f"[yellow]Warning:[/] Could not remove {path}: {e}")

        return removed

    def clear_log_files(self, force: bool = False):
        """Remove log files"""
        removed_files = []
        locked_files = []

        if force:
            self._force_close_loggers()

        for pattern in track(self.log_patterns, description="Removing log files..."):
            for file_path in self.project_root.rglob(pattern):
                if file_path.is_file():
                    try:
                        file_path.unlink()
                        removed_files.append(str(file_path.relative_to(self.project_root)))
                    except OSError as e:
                        if hasattr(e, "winerror") and e.winerror == 32:  # File in use
                            locked_files.append(str(file_path.relative_to(self.project_root)))
                        else:
                            console.print(f"[yellow]Warning:[/] Could not remove {file_path}: {e}")

        return removed_files, locked_files

    @staticmethod
    def _force_close_loggers():
        """Close all active logging handlers"""
        try:
            for name in logging.Logger.manager.loggerDict:
                logger = logging.getLogger(name)
                for handler in logger.handlers[:]:
                    try:
                        handler.close()
                        logger.removeHandler(handler)
                    except Exception:
                        pass

            root_logger = logging.getLogger()
            for handler in root_logger.handlers[:]:
                try:
                    handler.close()
                    root_logger.removeHandler(handler)
                except Exception:
                    pass
        except Exception:
            pass

    # ---------------------------------------------------------
    # REPORTING
    # ---------------------------------------------------------
    def report_results(self, removed_pycache: list, removed_files: list, 
                       removed_logs: list, locked_logs: list):
        table = Table(title="✅ Cache Cleaner Results")

        table.add_column("Category", style="cyan", no_wrap=True)
        table.add_column("Count", style="green", justify="right")

        table.add_row("__pycache__ Directories Removed", str(len(removed_pycache)))
        table.add_row("Other Cache Files Removed", str(len(removed_files)))
        table.add_row("Log Files Removed", str(len(removed_logs)))

        if locked_logs:
            table.add_row("Locked Log Files", str(len(locked_logs)))

        console.print(table)

    # ---------------------------------------------------------
    # MAIN CLEAN METHOD
    # ---------------------------------------------------------
    def clean_all(self, verbose: bool = True, clean_logs: bool = False, force_logs: bool = False):
        if verbose:
            console.print(f"[bold blue]Project root:[/] {self.project_root}")

        # Now using recursive removal for __pycache__
        removed_pycache = self.clear_all_pycache_dirs()
        removed_files = self.clear_cache_files()

        removed_logs: list = []
        locked_logs: list = []

        if clean_logs:
            removed_logs, locked_logs = self.clear_log_files(force=force_logs)

        total_removed = len(removed_pycache) + len(removed_files) + len(removed_logs)

        if verbose:
            self.report_results(removed_pycache, removed_files, removed_logs, locked_logs)

            if total_removed == 0:
                console.print("[yellow]No cache files or directories found to remove.[/]")

        return total_removed


# ---------------------------------------------------------
# CLI COMMAND
# ---------------------------------------------------------
@app.command()
def clean(
    logs: bool = typer.Option(False, "--logs", help="Also clean log files"),
    force_logs: bool = typer.Option(False, "--force-logs", help="Force close logging handlers before cleaning logs"),
    close_loggers: bool = typer.Option(False, "--close-loggers", help="Only close logging handlers (no cleanup)"),
):
    cleaner = CacheCleaner()

    if close_loggers:
        console.print("[yellow]Closing all logging handlers...[/]")
        cleaner._force_close_loggers()
        console.print("[green]Logging handlers closed.[/]")
        raise typer.Exit()

    cleaner.clean_all(
        verbose=True,
        clean_logs=logs or force_logs,
        force_logs=force_logs,
    )


if __name__ == "__main__":
    app()