r"""
C:\Economy\Invest\TrendMaster\src\cache_cleaner.py
Modern cache cleaner for TrendMaster.
Uses Typer for CLI and Rich for beautiful terminal feedback.
"""

import shutil
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table
from rich.progress import track

app = typer.Typer(help="Clean Python project cache files safely.")
console = Console()

CACHE_PATTERNS = {
    "dirs": {"__pycache__", ".pytest_cache", ".ipynb_checkpoints", ".mypy_cache", ".ruff_cache"},
    "files": {"*.pyc", "*.pyo", "*.pyd", ".coverage", ".DS_Store"},
    "logs": {"*.log", "*.log.*"}
}

PROTECTED = {".git", "venv", ".venv", "env", "node_modules", "dist", "build"}


def find_project_root(start: Optional[Path] = None) -> Path:
    current = (start or Path.cwd()).resolve()
    markers = {".git", "pyproject.toml", "setup.py", "README.md"}

    for _ in range(12):
        if any((current / m).exists() for m in markers):
            return current
        if current.parent == current:
            break
        current = current.parent

    return Path.cwd().resolve()


def is_protected(path: Path) -> bool:
    return any(part in PROTECTED for part in path.parts)


@app.command()
def main(
    root: Optional[Path] = typer.Option(None, help="Custom project root path"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Only show what would be deleted"),
    include_logs: bool = typer.Option(False, "--logs", help="Include log files in cleaning"),
    quiet: bool = typer.Option(False, "--quiet", help="Minimal output"),
):
    target_root = find_project_root(root)
    stats = {"dirs": 0, "files": 0, "logs": 0}

    if not quiet:
        console.print(f"[bold blue]Scanning:[/bold blue] {target_root}")
        if dry_run:
            console.print("[bold yellow]DRY-RUN MODE:[/bold yellow] No files will be deleted.\n")

    all_items = list(target_root.rglob("*"))

    for item in track(all_items, description="Cleaning...", disable=quiet):
        if is_protected(item):
            continue

        # Skip symlinked directories (B)
        if item.is_dir() and item.is_symlink():
            continue

        # Directory deletion with safe handling (C)
        if item.is_dir() and item.name in CACHE_PATTERNS["dirs"]:
            try:
                if not dry_run:
                    shutil.rmtree(item)
                stats["dirs"] += 1
            except Exception as e:
                if not quiet:
                    console.print(f"[red]Failed to remove directory:[/red] {item} ({e})")
            continue

        # File deletion with missing_ok=True (A)
        if item.is_file():
            if any(item.match(p) for p in CACHE_PATTERNS["files"]):
                if not dry_run:
                    item.unlink(missing_ok=True)
                stats["files"] += 1

            elif include_logs and any(item.match(p) for p in CACHE_PATTERNS["logs"]):
                try:
                    if not dry_run:
                        item.unlink(missing_ok=True)
                    stats["logs"] += 1
                except PermissionError:
                    if not quiet:
                        console.print(f"[dim red]Locked:[/dim red] {item.name}")

    if not quiet:
        show_summary(stats, dry_run)


def show_summary(stats: dict, dry_run: bool):
    table = Table(title="Cleaning Summary", show_header=True, header_style="bold magenta")
    table.add_column("Category", style="dim")
    table.add_column("Count", justify="right")

    table.add_row("Directories", str(stats["dirs"]))
    table.add_row("Cache Files", str(stats["files"]))
    if stats["logs"] > 0:
        table.add_row("Log Files", str(stats["logs"]))

    console.print(table)
    total = sum(stats.values())
    status = "would be deleted" if dry_run else "deleted"
    console.print(f"[bold green]Success![/bold green] {total} items {status}.")


if __name__ == "__main__":
    app()
