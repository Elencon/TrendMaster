r"""
C:\Economy\Invest\TrendMaster\src\logging_system.py
Modern structured logging for ETL operations.
Uses structlog for clean, fast, structured logs with correlation IDs and performance tracking.
Public interface unchanged.
"""

from __future__ import annotations

import logging
import logging.handlers
import sys
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

import structlog
from structlog import processors as sp
from structlog import stdlib

# ────────────────────────────────────────────────
# Global flag to ensure idempotent configuration
# ────────────────────────────────────────────────

_logging_configured = False


# ────────────────────────────────────────────────
# Structlog Setup (explicit call required)
# ────────────────────────────────────────────────

def setup_logging(log_dir: Path = Path("logs")) -> None:
    """
    Configure structlog + standard logging handlers.

    Safe to call multiple times — subsequent calls are no-ops.
    Call this explicitly from your application entry point.
    """
    global _logging_configured
    if _logging_configured:
        return

    _logging_configured = True
    log_dir.mkdir(exist_ok=True)

    use_json = "json" in sys.argv

    # ────────────────────────────────────────────────
    # Structlog processor chain (latest structlog style)
    # ────────────────────────────────────────────────
    processors_chain = [
        structlog.contextvars.merge_contextvars,  # picks up bind_contextvars()
        sp.add_log_level,
        sp.StackInfoRenderer(),
        sp.format_exc_info,                      # modern replacement for set_exc_info
        sp.TimeStamper(fmt="iso", utc=True),
        sp.JSONRenderer() if use_json else structlog.dev.ConsoleRenderer(),
    ]

    structlog.configure(
        processors=processors_chain,
        logger_factory=stdlib.LoggerFactory(),
        wrapper_class=stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # ────────────────────────────────────────────────
    # Standard logging bridge
    # Avoid duplicate handlers
    # ────────────────────────────────────────────────
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
# Public API (unchanged)
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
    """
    Bind a correlation ID into structlog's context for the duration of this block.
    """
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
def performance_context(
    operation: str,
    logger: structlog.stdlib.BoundLogger | None = None,
) -> Generator[None, None, None]:
    """
    Measure and log the duration of a block.
    """
    if logger is None:
        logger = get_logger("etl.performance")

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

def configure_logging(config: dict[str, Any] | None = None) -> None:
    """
    Apply optional runtime overrides to logging configuration.
    """
    if config is None:
        return

    # Update log level
    level_name = config.get("level", "INFO").upper()
    logging.getLogger().setLevel(getattr(logging, level_name, logging.INFO))

    # Update log directory
    if log_dir := config.get("directory"):
        root = logging.getLogger()
        root.handlers.clear()  # allow reinitialization
        global _logging_configured
        _logging_configured = False
        setup_logging(Path(log_dir))
