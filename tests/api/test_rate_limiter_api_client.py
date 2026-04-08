import pytest
import asyncio
import time

from src.api.api_client import AsyncAPIClient
from src.api.api_models import APIRequest, RequestMethod
from src.api.rate_limiter import RateLimitConfig


@pytest.mark.asyncio
async def test_api_client_rate_limiter_concurrency():
    """
    Ensures that AsyncAPIClient respects RateLimiter.max_concurrent
    when making real HTTP requests to https://httpbin.org.
    """

    # Allow only 2 concurrent requests
    rate_config = RateLimitConfig(
        requests_per_second=100.0,   # effectively unlimited RPS
        max_concurrent=2,
        enabled=True
    )

    async with AsyncAPIClient(
        base_url="https://httpbin.org",
        rate_limit_config=rate_config
    ) as client:

        start = time.perf_counter()

        async def make_request(i):
            req = APIRequest(url="/delay/0.1", method=RequestMethod.GET)
            # httpbin /delay/0.1 waits 100ms server-side
            resp = await client.request(req)
            return resp.status

        # Launch 4 requests concurrently
        results = await asyncio.gather(*(make_request(i) for i in range(4)))

        elapsed = time.perf_counter() - start

        # With max_concurrent=2:
        # Wave 1: 2 requests → 0.1s
        # Wave 2: 2 requests → 0.1s
        # Total ≈ 0.2s
        assert elapsed >= 0.2
        assert all(r == 200 for r in results)


@pytest.mark.asyncio
async def test_api_client_rate_limiter_rps():
    """
    Ensures that AsyncAPIClient respects RateLimiter.requests_per_second
    when making real HTTP requests to https://httpbin.org.
    """

    # 2 requests per second → one request every 0.5s
    rate_config = RateLimitConfig(
        requests_per_second=2.0,
        max_concurrent=10,
        enabled=True
    )

    async with AsyncAPIClient(
        base_url="https://httpbin.org",
        rate_limit_config=rate_config
    ) as client:

        start = time.perf_counter()

        async def make_request(i):
            req = APIRequest(url="/get", method=RequestMethod.GET)
            resp = await client.request(req)
            return resp.status

        # Launch 3 requests concurrently
        results = await asyncio.gather(*(make_request(i) for i in range(3)))

        elapsed = time.perf_counter() - start

        # Expected timing:
        # Request 1 → 0.0s
        # Request 2 → 0.5s delay
        # Request 3 → 0.5s delay
        # Total ≈ 1.0s
        assert elapsed >= 0.9
        assert all(r == 200 for r in results)