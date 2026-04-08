import pytest
import asyncio
import time
from src.api.api_client import AsyncAPIClient
from src.api.api_models import APIRequest, RequestMethod
from src.api.rate_limiter import RateLimitConfig

@pytest.mark.asyncio
async def test_api_client_stats_over_100_requests(capsys):
    # Setup rate limits to avoid getting banned by httpbin
    # 20 RPS is safe, but we'll use a high max_concurrent to test your semaphore
    rate_config = RateLimitConfig(
        requests_per_second=20.0,
        max_concurrent=15,
        enabled=True
    )

    # Note: We do NOT pass a transport here, so it uses the real network
    async with AsyncAPIClient(
        base_url="https://httpbin.org",
        rate_limit_config=rate_config
    ) as client:

        # Use a list to store tasks for concurrent execution
        tasks = []
        for i in range(100):
            # httpbin.org/get is a standard echo endpoint
            req = APIRequest(url="/get", method=RequestMethod.GET)
            tasks.append(client.request(req))
        
        # Execute all 100 requests. 
        # Your RateLimiter will ensure they don't all hit the server at once.
        results = await asyncio.gather(*tasks)

        # Assertions to verify the data integrity
        assert len(results) == 100
        assert all(r.status == 200 for r in results)

        # Get stats from the client
        stats = await client.get_stats()
        
        # Verify the RequestStats internal counters
        assert stats["total_requests"] == 100
        assert stats["successful_requests"] == 100
        assert stats["success_rate"] >= 0.99  # Allow for one-off network flukes

        # Display output in VS Code (requires 'pytest -s')
        with capsys.disabled():
            print(f"\n{'='*15} RequestStats Results {'='*15}")
            for key, value in stats.items():
                # Format floats for readability
                val_str = f"{value:.3f}" if isinstance(value, float) else value
                print(f"{key:20}: {val_str}")
            print(f"{'='*50}")