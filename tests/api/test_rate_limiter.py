import pytest
import asyncio
import time
from src.api.rate_limiter import RateLimiter, RateLimitConfig

@pytest.mark.asyncio
async def test_rate_limiter_concurrency():
    # Set limit to 2 concurrent requests
    config = RateLimitConfig(requests_per_second=100.0, max_concurrent=2, enabled=True)
    limiter = RateLimiter(config=config)
    
    start_time = time.perf_counter()
    
    async def task(i):
        async with limiter.limit_context():
            await asyncio.sleep(0.1)
            return i

    # Run 4 tasks concurrently
    results = await asyncio.gather(*(task(i) for i in range(4)))
    
    elapsed = time.perf_counter() - start_time
    
    # Each task takes 0.1s. With max_concurrent=2, 4 tasks should take at least 0.2s
    assert elapsed >= 0.2
    assert len(results) == 4

@pytest.mark.asyncio
async def test_rate_limiter_rps():
    # Set limit to 2 requests per second (0.5s per request)
    config = RateLimitConfig(requests_per_second=2.0, max_concurrent=10, enabled=True)
    limiter = RateLimiter(config=config)
    
    start_time = time.perf_counter()
    
    async def task(i):
        async with limiter.limit_context():
            return i

    # Run 3 tasks (should have 2 delays of ~0.5s each)
    # Task 1: 0 delay
    # Task 2: 0.5 delay
    # Task 3: 0.5 delay (total 1s)
    results = await asyncio.gather(*(task(i) for i in range(3)))
    
    elapsed = time.perf_counter() - start_time
    
    # Should be at least 1.0s (actually slightly less due to overlap start)
    # Let's say at least 0.9s to be safe
    assert elapsed >= 0.9
    assert len(results) == 3

@pytest.mark.asyncio
async def test_rate_limiter_disabled():
    config = RateLimitConfig(enabled=False)
    limiter = RateLimiter(config=config)
    
    async with limiter.limit_context():
        assert True # Should not block
