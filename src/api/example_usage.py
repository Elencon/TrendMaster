r"""
Path: C:\Economy\Invest\TrendMaster\src\api\example_usage.py
python -m src.api.example_usage
"""
import anyio
import logging
import sys
from pathlib import Path

# Rich imports for terminal UI
from rich.console import Console
from rich.table import Table
from rich.logging import RichHandler
from rich import box

# ────────────────────────────────────────────────
# Path Setup
# ────────────────────────────────────────────────
current_file = Path(__file__).resolve()
src_path = str(current_file.parents[1]) 
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from .api_models import APIRequest, RequestMethod
from .api_client import AsyncAPIClient

# ────────────────────────────────────────────────
# Configuration & Logging
# ────────────────────────────────────────────────
console = Console()

# Using RichHandler to make standard logs look great alongside the table
logging.basicConfig(
    level="INFO",
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True, console=console)]
)
logger = logging.getLogger("TrendMaster")

# ────────────────────────────────────────────────
# Task Logic
# ────────────────────────────────────────────────

async def fetch_item(client: AsyncAPIClient, item_id: int):
    """Worker task to perform a concurrent API request."""
    request = APIRequest(url=f"posts/{item_id}")
    response = await client.request(request)
    
    status_label = "Success" if response.http_success else "Failed"
    color = "green" if response.http_success else "red"
    
    logger.info(
        f"ID {item_id:02} | Status: [{color}]{status_label}[/{color}] | "
        f"Latency: {response.latency_ms}ms"
    )

async def main():
    # client.base_url is private (_base_url) but accessible via property
    async with AsyncAPIClient(base_url="https://jsonplaceholder.typicode.com/") as client:
        logger.info(f"Starting Batch Fetch to: {client.base_url}")
        
        # AnyIO Task Groups manage concurrent requests safely
        async with anyio.create_task_group() as tg:
            for i in range(1, 6):
                tg.start_soon(fetch_item, client, i)

        # ────────────────────────────────────────────────
        # Final Stats Table (Full Grid Style)
        # ────────────────────────────────────────────────
        stats = await client.get_stats()

        table = Table(
            title="\n[bold cyan]TrendMaster Execution Summary[/bold cyan]", 
            show_header=True, 
            header_style="bold magenta",
            box=box.ROUNDED,      # Full vertical and horizontal outer borders
            show_lines=True,      # Full grid lines between all cells
            padding=(0, 1)
        )

        table.add_column("Category", style="dim", width=15)
        table.add_column("Metric", style="white")
        table.add_column("Value", justify="right")

        # Volumes Section
        table.add_row("Volumes", "Total Requests", str(stats['total_requests']))
        table.add_row("", "Successful", str(stats['successful_requests']), style="green")
        
        # Highlight failures in red if they exist
        fail_style = "bold red" if stats['failed_requests'] > 0 else "white"
        table.add_row("", "Failed", str(stats['failed_requests']), style=fail_style)
        table.add_row("", "Retried", str(stats['retried_requests']))
        
        # Distinct Section Break
        table.add_section()

        # Performance Section
        table.add_row("Performance", "Total Time", f"{stats['total_response_time']}s", style="cyan")
        table.add_row("", "Avg Latency", f"{stats['avg_latency']}s")
        
        # Output the table to the console
        console.print(table)
        console.print("\n")

if __name__ == "__main__":
    try:
        # anyio.run is the modern entry point for async apps
        anyio.run(main)
    except KeyboardInterrupt:
        pass