import pytest
import httpx
import datetime
from src.api.api_client import AsyncAPIClient
from src.api.api_models import APIRequest, RequestMethod
from src.common.exceptions import APIResponseError

@pytest.fixture
def mock_transport():
    def handler(request):
        response = httpx.Response(200, json={"foo": "bar"})
        response.elapsed = datetime.timedelta(milliseconds=100)
        return response
    return httpx.MockTransport(handler)

@pytest.mark.asyncio
async def test_api_client_success():
    def handler(request):
        response = httpx.Response(200, json={"status": "ok", "data": [1, 2, 3]})
        response.elapsed = datetime.timedelta(milliseconds=100)
        return response

    transport = httpx.MockTransport(handler)

    async with AsyncAPIClient(base_url="https://api.test.com") as client:
        client._client = httpx.AsyncClient(transport=transport, base_url="https://api.test.com")

        request = APIRequest(url="/data", method=RequestMethod.GET)
        response = await client.request(request)

        assert response.status == 200
        assert response.data == {"status": "ok", "data": [1, 2, 3]}

        stats = await client.get_stats()
        assert stats["total_requests"] == 1
        assert stats["successful_requests"] == 1

@pytest.mark.asyncio
async def test_api_client_error():
    def handler(request):
        response = httpx.Response(404, text="Not Found")
        response.elapsed = datetime.timedelta(milliseconds=50)
        return response

    transport = httpx.MockTransport(handler)

    async with AsyncAPIClient(base_url="https://api.test.com") as client:
        client._client = httpx.AsyncClient(transport=transport, base_url="https://api.test.com")

        request = APIRequest(url="/missing", method=RequestMethod.GET)

        # Should raise APIResponseError after retries (default 3)
        with pytest.raises(APIResponseError) as excinfo:
            await client.request(request)

        assert excinfo.value.status_code == 404

        stats = await client.get_stats()
        assert stats["failed_requests"] == 1
        assert stats["total_requests"] == 3  # Default 3 attempts

@pytest.mark.asyncio
async def test_api_client_telemetry():
    async with AsyncAPIClient(base_url="https://api.test.com") as client:
        stats = await client.get_stats()
        assert "uptime" in stats
        assert "total_requests" in stats
        assert "avg_latency" in stats