r"""
Path: C:\Economy\Invest\TrendMaster\src\api\rate_limiter.py
"""
import asyncio
import time
from dataclasses import dataclass
from typing import Optional
from contextlib import asynccontextmanager

@dataclass(frozen=True) # Frozen makes the config itself immutable
class RateLimitConfig:
    requests_per_second: float = 5.0
    max_concurrent: int = 10
    enabled: bool = True

class RateLimiter:
    def __init__(self, config: Optional[RateLimitConfig] = None):
        self._config = config or RateLimitConfig()
        self._semaphore = asyncio.Semaphore(self._config.max_concurrent)
        self._last_request_time = 0.0
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def limit_context(self):
        """Standard usage: async with limiter.limit_context(): ..."""
        if not self._config.enabled:
            yield
            return

        async with self._semaphore:
            # 1. Handle Requests Per Second (Rate)
            async with self._lock:
                elapsed = time.perf_counter() - self._last_request_time
                delay = (1.0 / self._config.requests_per_second) - elapsed

                if delay > 0:
                    await asyncio.sleep(delay)

                self._last_request_time = time.perf_counter()

            # 2. Handle Concurrency (Semaphore already locked)
            yield

    @property
    def config(self) -> RateLimitConfig:
        return self._config
