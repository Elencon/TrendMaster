r"""
When to use which logger
✔ Use logging.getLogger(__name__) in most modules
This is the standard Python pattern and works perfectly with your structlog setup.
✔ Use get_logger() from logging_system only when you want a named subsystem
for example

from src.logging_system import get_api_logger
logger = get_api_logger()
grouping logs by subsystem
• 	filtering logs by namespace
• 	giving ETL components consistent names
"""

from __future__ import annotations

import logging
import logging.handlers
import sys
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import structlog
from structlog import processors as sp
from structlog import stdlib

# Prevent double initialization
_logging_configured = False


# ────────────────────────────────────────────────
# Structlog Setup
# ────────────────────────────────────────────────

def setup_logging(log_dir: Path = Path("logs")) -> None:
    """Configure structlog + standard logging handlers (idempotent)."""
    global _logging_configured
    if _logging_configured:
        return

    _logging_configured = True
    log_dir.mkdir(exist_ok=True)

    use_json = "json" in sys.argv

    processors_chain = [
        structlog.contextvars.merge_contextvars,
        sp.add_log_level,
        sp.StackInfoRenderer(),
        sp.format_exc_info,
        sp.TimeStamper(fmt="iso", utc=True),
        sp.JSONRenderer() if use_json else structlog.dev.ConsoleRenderer(),
    ]

    structlog.configure(
        processors=processors_chain,
        logger_factory=stdlib.LoggerFactory(),
        wrapper_class=stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    root = logging.getLogger()
    if root.handlers:
        return

    root.setLevel(logging.INFO)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processor=sp.JSONRenderer() if use_json else structlog.dev.ConsoleRenderer(),
            foreign_pre_chain=[
                sp.add_log_level,
                sp.TimeStamper(fmt="iso", utc=True),
            ],
        )
    )
    root.addHandler(console_handler)

    # Rotating file handler (always JSON)
    file_handler = logging.handlers.RotatingFileHandler(
        log_dir / "etl.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
    )
    file_handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processor=sp.JSONRenderer(),
            foreign_pre_chain=[
                sp.add_log_level,
                sp.TimeStamper(fmt="iso", utc=True),
            ],
        )
    )
    root.addHandler(file_handler)


# ────────────────────────────────────────────────
# Public API
# ────────────────────────────────────────────────

def get_logger(name: str = "etl") -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)


def get_database_logger() -> structlog.stdlib.BoundLogger:
    return get_logger("etl.database")


def get_api_logger() -> structlog.stdlib.BoundLogger:
    return get_logger("etl.api")


def get_processing_logger() -> structlog.stdlib.BoundLogger:
    return get_logger("etl.processing")


def get_validation_logger() -> structlog.stdlib.BoundLogger:
    return get_logger("etl.validation")


# ────────────────────────────────────────────────
# Correlation ID context manager
# ────────────────────────────────────────────────

@contextmanager
def correlation_context(correlation_id: str | None = None) -> Generator[str, None, None]:
    new_id = correlation_id or str(uuid.uuid4())[:8]
    structlog.contextvars.bind_contextvars(correlation_id=new_id)
    try:
        yield new_id
    finally:
        structlog.contextvars.unbind_contextvars("correlation_id")


# ────────────────────────────────────────────────
# Performance measurement context manager
# ────────────────────────────────────────────────

@contextmanager
def performance_context(operation: str, logger: structlog.stdlib.BoundLogger | None = None) -> Generator[None, None, None]:
    logger = logger or get_logger("etl.performance")

    start = time.perf_counter()
    logger.info("operation_started", operation=operation)

    try:
        yield
        duration = time.perf_counter() - start
        logger.info(
            "operation_completed",
            operation=operation,
            duration_seconds=round(duration, 3),
            status="success",
        )
    except Exception as e:
        duration = time.perf_counter() - start
        logger.error(
            "operation_failed",
            operation=operation,
            duration_seconds=round(duration, 3),
            status="error",
            error=str(e),
            exc_info=True,
        )
        raise


# ────────────────────────────────────────────────
# Runtime configuration overrides
# ────────────────────────────────────────────────

def configure_logging(config: dict[str, object] | None = None) -> None:
    if not config:
        return

    # Update log level
    level_name = str(config.get("level", "INFO")).upper()
    logging.getLogger().setLevel(getattr(logging, level_name, logging.INFO))

    # Update log directory
    if log_dir := config.get("directory"):
        root = logging.getLogger()
        root.handlers.clear()
        global _logging_configured
        _logging_configured = False
        setup_logging(Path(log_dir))