r"""
Path: C:\Economy\Invest\TrendMaster\src\api\rate_limiter.py
"""

import asyncio
import time
from dataclasses import dataclass
from contextlib import asynccontextmanager


@dataclass(frozen=True)
class RateLimitConfig:
    """
    Configuration for the async RateLimiter.

    This protects your application from overloading external APIs by enforcing
    two limits:

    1. requests_per_second:
       Controls how fast requests are sent. If your code tries to send requests
       too quickly, the limiter automatically waits before allowing the next one.
       This prevents 429 errors, throttling, and temporary API bans.

    2. max_concurrent:
       Limits how many requests run in parallel. Even if many async tasks are
       launched at once, only this number will execute simultaneously. This
       prevents connection spikes and keeps API usage stable.

    When enabled=True, both protections are active. The RateLimiter uses an
    asyncio.Semaphore to control concurrency and timed delays to enforce the
    requests-per-second rate.
    """
    requests_per_second: float = 5.0
    max_concurrent: int = 10
    enabled: bool = True


class RateLimiter:

    """
    Async rate-limiting helper.

    This class enforces two protections when calling external APIs:
      • Limits how many requests run at the same time (max_concurrent)
      • Ensures a minimum delay between requests (requests_per_second)

    You use it as:  async with limiter.limit_context(): ...
    Every API call must pass through this context so the limiter can
    apply waiting and concurrency control.
    """   
    def __init__(self, config: RateLimitConfig | None = None):
        self._config = config or RateLimitConfig()
        self._semaphore = asyncio.Semaphore(self._config.max_concurrent)
        self._last_request_time = 0.0
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def limit_context(self):
        """
        Standard usage: async with limiter.limit_context(): ...
        Enforces concurrency limits first, then throughput (RPS).
        """
        if not self._config.enabled:
            yield
            return

        # 1. Concurrency limit (limits parallel execution)
        async with self._semaphore:

            # 2. RPS limit (limits start cadence)
            async with self._lock:
                now = time.perf_counter()
                elapsed = now - self._last_request_time
                min_interval = 1.0 / self._config.requests_per_second

                delay = min_interval - elapsed
                if delay > 0:
                    await asyncio.sleep(delay)

                # Update timestamp AFTER the potential sleep for accuracy
                self._last_request_time = time.perf_counter()

            # 3. Yield control to the actual API request
            try:
                yield
            finally:
                # Semaphore is automatically released here by the 'async with'
                pass

        
    @property
    def config(self) -> RateLimitConfig:
        return self._config