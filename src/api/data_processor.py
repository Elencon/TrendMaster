r"""
Modern API response data processing for TrendMaster ETL pipelines.
Path: C:\Economy\Invest\TrendMaster\src\api\data_processor.py
"""

import anyio
import logging
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
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

import msgspec
from .api_models import APIResponse

logger = logging.getLogger(__name__)

T = TypeVar("T")  # Input type
R = TypeVar("R")  # Result type (msgspec.Struct)

@dataclass
class ProcessingStats:
    """Statistics for data processing operations."""
    total_responses: int = 0
    successful_responses: int = 0
    failed_responses: int = 0
    processed_items: int = 0
    error_items: int = 0
    processing_time: float = 0.0
    errors_by_type: Dict[str, int] = field(default_factory=lambda: defaultdict(int))

    @property
    def success_rate(self) -> float:
        return (self.successful_responses / self.total_responses * 100) if self.total_responses > 0 else 0.0

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
                    if not response.http_success:
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
