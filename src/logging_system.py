"""
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
from structlog import configure
from structlog import processors as sp
from structlog import stdlib

# ────────────────────────────────────────────────
# Setup structlog (idempotent, not called on import)
# ────────────────────────────────────────────────

_logging_configured = False


def setup_logging(log_dir: Path = Path("logs")) -> None:
    """
    Configure structlog + standard logging handlers.

    Safe to call multiple times — subsequent calls are no-ops.
    Call this explicitly from your application entry point, not from
    library code or at module import time.
    """
    global _logging_configured
    if _logging_configured:
        return
    _logging_configured = True

    log_dir.mkdir(exist_ok=True)

    use_json = "json" in sys.argv

    # Shared processor chain for structlog
    processors_chain = [
        structlog.contextvars.merge_contextvars,    # picks up bind_contextvars() calls
        sp.add_log_level,
        sp.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        sp.TimeStamper(fmt="iso", utc=True),
        sp.JSONRenderer() if use_json else structlog.dev.ConsoleRenderer(),
    ]

    configure(
        processors=processors_chain,
        logger_factory=stdlib.LoggerFactory(),
        wrapper_class=stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Standard logging bridge — avoid adding duplicate handlers
    root = logging.getLogger()
    if root.handlers:
        return

    root.setLevel(logging.INFO)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(structlog.stdlib.ProcessorFormatter(
        processor=sp.JSONRenderer() if use_json else structlog.dev.ConsoleRenderer()
    ))
    root.addHandler(console_handler)

    # Rotating file handler (always JSON for machine readability)
    file_handler = logging.handlers.RotatingFileHandler(
        log_dir / "etl.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
    )
    file_handler.setFormatter(structlog.stdlib.ProcessorFormatter(
        processor=sp.JSONRenderer()
    ))
    root.addHandler(file_handler)


# ────────────────────────────────────────────────
# Public API
# ────────────────────────────────────────────────

def get_logger(name: str = "etl") -> structlog.stdlib.BoundLogger:
    """Get a structured logger bound to the given name."""
    return structlog.get_logger(name)


def get_database_logger() -> structlog.stdlib.BoundLogger:
    return get_logger("etl.database")


def get_api_logger() -> structlog.stdlib.BoundLogger:
    return get_logger("etl.api")


def get_processing_logger() -> structlog.stdlib.BoundLogger:
    return get_logger("etl.processing")


def get_validation_logger() -> structlog.stdlib.BoundLogger:
    return get_logger("etl.validation")


@contextmanager
def correlation_context(correlation_id: str | None = None) -> Generator[str, None, None]:
    """
    Bind a correlation ID into structlog's context for the duration of this block.

    The ID is automatically included in all log records via merge_contextvars.
    Safe for use in both threaded and async code.

    Args:
        correlation_id: Optional explicit ID. If omitted, a short UUID is generated.

    Yields:
        The correlation ID that was bound.
    """
    new_id = correlation_id or str(uuid.uuid4())[:8]
    structlog.contextvars.bind_contextvars(correlation_id=new_id)
    try:
        yield new_id
    finally:
        structlog.contextvars.unbind_contextvars("correlation_id")


@contextmanager
def performance_context(
    operation: str,
    logger: structlog.stdlib.BoundLogger | None = None,
) -> Generator[None, None, None]:
    """
    Measure and log the duration of a block.

    Logs a structured start event, then a completion or failure event
    with duration and status fields.

    Args:
        operation: Human-readable name of the operation being measured.
        logger: Optional logger to use. Defaults to 'etl.performance'.
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


def configure_logging(config: dict[str, Any] | None = None) -> None:
    """
    Apply optional runtime overrides to logging configuration.

    Respects the idempotency of setup_logging — call this after setup_logging
    if you need to adjust level or log directory based on runtime config.

    Args:
        config: Optional dict with keys:
            - 'level': log level string (e.g. 'DEBUG', 'WARNING')
            - 'directory': path string for log file output
    """
    if config is None:
        return

    level_name = config.get("level", "INFO").upper()
    logging.getLogger().setLevel(getattr(logging, level_name, logging.INFO))

    if log_dir := config.get("directory"):
        global _logging_configured
        _logging_configured = False  # allow re-initialisation with new directory
        setup_logging(Path(log_dir))