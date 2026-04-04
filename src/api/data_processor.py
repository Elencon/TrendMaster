r"""
Modern API response data processing for TrendMaster ETL pipelines.
Path: C:\Economy\Invest\TrendMaster\src\api\data_processor.py
"""

import anyio
import logging
import time
from datetime import datetime, timezone
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    TypeVar,
    Union,
)
from msgspec import Struct, field


from .api_models import APIResponse

logger = logging.getLogger(__name__)

T = TypeVar("T")  # Input type
R = TypeVar("R")  # Result type (msgspec.Struct)


class ProcessingStats(Struct):
    """Statistics for data processing operations."""

    # Unified naming (matches example_usage.py)
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0

    # Domain-specific metrics
    processed_items: int = 0
    error_items: int = 0
    processing_time: float = 0.0

    # Error grouping
    errors_by_type: dict[str, int] = field(default_factory=dict)

    # Timestamp for metrics window
    start_time: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    # ────────────────────────────────────────────
    # Recording
    # ────────────────────────────────────────────
    def record(
        self,
        success: bool,
        items: int = 0,
        errors: int = 0,
        error_type: str | None = None,
        duration: float = 0.0,
    ) -> None:
        """Record a processing event."""
        self.total_requests += 1

        if success:
            self.successful_requests += 1
        else:
            self.failed_requests += 1

        self.processed_items += items
        self.error_items += errors
        self.processing_time += duration

        if error_type:
            self.errors_by_type[error_type] = (
                self.errors_by_type.get(error_type, 0) + 1
            )

    # ────────────────────────────────────────────
    # Derived metrics
    # ────────────────────────────────────────────
    @property
    def success_rate(self) -> float:
        """Fraction of successful requests (0.0–1.0)."""
        if self.total_requests == 0:
            return 0.0
        return self.successful_requests / self.total_requests

    @property
    def avg_processing_time(self) -> float:
        """Average processing time per request."""
        if self.total_requests == 0:
            return 0.0
        return self.processing_time / self.total_requests

    # ────────────────────────────────────────────
    # Reset
    # ────────────────────────────────────────────
    def reset(self) -> None:
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.processed_items = 0
        self.error_items = 0
        self.processing_time = 0.0
        self.errors_by_type = {}
        self.start_time = datetime.now(timezone.utc)

    # ────────────────────────────────────────────
    # Serialization
    # ────────────────────────────────────────────
    def to_dict(self) -> dict[str, Any]:
        return {
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,

            "processed_items": self.processed_items,
            "error_items": self.error_items,

            "processing_time": round(self.processing_time, 3),
            "total_response_time": round(self.processing_time, 3),
            "avg_latency": round(self.avg_processing_time, 3),

            "errors_by_type": dict(self.errors_by_type),

            "success_rate": self.success_rate,
            "start_time": self.start_time.isoformat(),
        }


class APIDataProcessor(Generic[T, R]):
    """
    High-performance processor using AnyIO and msgspec.
    Handles the 'Transform' stage of the TrendMaster ETL pipeline.
    """

    def __init__(self, max_concurrent: int = 50):
        self._limiter = anyio.CapacityLimiter(max_concurrent)
        self.stats = ProcessingStats()

    async def process_responses(
        self,
        responses: List[APIResponse],
        target_type: type[R],
        error_handler: Optional[Callable[[APIResponse, Exception], Any]] = None,
    ) -> List[R]:
        """
        Processes responses concurrently using AnyIO Task Groups.
        """
        self.stats = ProcessingStats(total_responses=len(responses))
        start_time = time.perf_counter()

        final_results: List[R] = []

        async with anyio.create_task_group() as tg:
            # Create a memory stream to gather results from workers
            send_stream, receive_stream = anyio.create_memory_object_stream(max_buffer_size=len(responses))

            async with send_stream:
                for resp in responses:
                    tg.start_soon(self._worker, resp, target_type, send_stream.clone(), error_handler)

            # Collect results as they arrive
            async with receive_stream:
                async for batch in receive_stream:
                    final_results.extend(batch)

        self.stats.processing_time = time.perf_counter() - start_time
        return final_results

    async def _worker(
        self,
        response: APIResponse,
        target_type: type[R],
        send_stream: anyio.abc.ObjectSendStream[List[R]],
        error_handler: Optional[Callable]
    ):
        """Concurrent worker that transforms API data into typed msgspec Structs."""
        async with send_stream:
            async with self._limiter:
                try:
                    if not response.is_success:
                        self.stats.failed_responses += 1
                        return

                    # msgspec.convert replaces the old processor_func
                    # It validates and converts the dict to your Struct at C-speed
                    converted = msgspec.convert(response.data, type=Union[List[target_type], target_type])

                    items = converted if isinstance(converted, list) else [converted]

                    self.stats.successful_responses += 1
                    self.stats.processed_items += len(items)
                    await send_stream.send(items)

                except Exception as e:
                    self.stats.failed_responses += 1
                    self.stats.errors_by_type[type(e).__name__] += 1
                    logger.error(f"Processing error: {e} | URL: {response.url}")

                    if error_handler:
                        err_res = error_handler(response, e)
                        if err_res:
                            await send_stream.send(err_res if isinstance(err_res, list) else [err_res])

    @asynccontextmanager
    async def processing_context(self, job_name: str = "ETL Transformation"):
        """Context manager to auto-log performance metrics."""
        start = time.perf_counter()
        try:
            yield self
        finally:
            self.stats.processing_time = time.perf_counter() - start
            logger.info(
                f"{job_name} Done | Items: {self.stats.processed_items} | "
                f"Success: {self.stats.success_rate:.1f}% | Time: {self.stats.processing_time:.3f}s"
            )
