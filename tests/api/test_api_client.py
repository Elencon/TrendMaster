import pytest
import httpx

from src.api.api_client import AsyncAPIClient
from src.api.api_models import APIRequest, RequestMethod
from src.common.retry import RetryExhaustedError


# ────────────────────────────────────────────────
# 1. SUCCESS TEST — uses real /json endpoint
# ────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_api_client_success():
    async with AsyncAPIClient(base_url="https://httpbin.org") as client:
        request = APIRequest(url="/json", method=RequestMethod.GET)
        response = await client.request(request)

        # httpbin.org/json always returns a slideshow JSON object
        assert response.status == 200
        assert isinstance(response.data, dict)
        assert "slideshow" in response.data

        stats = await client.get_stats()
        assert stats["total_requests"] == 1
        assert stats["successful_requests"] == 1
        assert stats["failed_requests"] == 0


# ────────────────────────────────────────────────
# 2. ERROR + RETRY TEST — uses real /status/404
# ────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_api_client_error():
    async with AsyncAPIClient(base_url="https://httpbin.org") as client:
        request = APIRequest(url="/status/404", method=RequestMethod.GET)

        # RetryHandler should retry 3 times and then raise RetryExhaustedError
        with pytest.raises(RetryExhaustedError):
            await client.request(request)

        stats = await client.get_stats()

        # Default retry_config.max_attempts = 3
        assert stats["failed_requests"] == client.retry_config.max_attempts
        assert stats["total_requests"] == client.retry_config.max_attempts
        assert stats["successful_requests"] == 0


# ────────────────────────────────────────────────
# 3. TELEMETRY TEST — ensures stats fields exist
# ────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_api_client_telemetry():
    async with AsyncAPIClient(base_url="https://httpbin.org") as client:
        request = APIRequest(url="/get", method=RequestMethod.GET)
        await client.request(request)

        stats = await client.get_stats()

        assert "start_time" in stats
        assert "total_requests" in stats
        assert "avg_response_time" in stats
        assert stats["total_requests"] == 1