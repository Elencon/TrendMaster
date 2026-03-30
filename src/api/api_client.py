"""
TrendMaster Async API Client.
Path: src/api/api_client.py
"""
import logging
from datetime import datetime
from typing import Optional, Any, Dict
import httpx
from yarl import URL

from ..common import RetryHandler, RetryConfig
from ..common.exceptions import (
    APIConnectionError,
    APITimeoutError,
    APIResponseError,
    APIError
)
from .api_models import APIRequest, APIResponse

logger = logging.getLogger("TrendMaster.API")


class AsyncAPIClient:
    def __init__(
        self,
        base_url: str,
        timeout: float = 10.0,
        retry_config: Optional[RetryConfig] = None,
        transport=None
    ):
        self._base_url = URL(base_url)
        self._timeout = timeout

        self._stats = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "total_response_time": 0.0,
            "start_time": datetime.now()
        }

        default_config = RetryConfig(
            max_attempts=3,
            base_delay=1.5,
            retry_on_exception=(APIConnectionError, APITimeoutError, APIResponseError),
            retry_on_result=lambda res: hasattr(res, "data") and (res.data is None or res.data == {})
        )

        self.retry_handler = RetryHandler(config=retry_config or default_config)

        self._client = httpx.AsyncClient(
            base_url=str(self._base_url),
            timeout=timeout,
            follow_redirects=True,
            transport=transport,  # <-- Needed for MockTransport in tests
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10)
        )

    @property
    def base_url(self) -> str:
        return str(self._base_url)

    # ---------------------------------------------------------
    # ASYNC CONTEXT MANAGER SUPPORT
    # ---------------------------------------------------------
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def close(self):
        if not self._client.is_closed:
            await self._client.aclose()

    # ---------------------------------------------------------

    async def request(self, request_info: APIRequest) -> APIResponse:
        try:
            return await self.retry_handler.execute_async(self._execute_request, request_info)
        except Exception:
            self._stats["failed_requests"] += 1
            raise

    async def _execute_request(self, request_info: APIRequest) -> APIResponse:
        """
        Internal raw HTTP call with specific exception translation.
        Executes a single HTTP request and converts the response into an APIResponse.
        """
        self._stats["total_requests"] += 1

        try:
            # Build request arguments
            kwargs: Dict[str, Any] = {
                "method": request_info.method.value,
                "url": request_info.url,
                "params": request_info.params,
                "headers": request_info.headers,
                "timeout": request_info.timeout or self._timeout,
            }

            # Body handling
            if request_info.json is not None:
                kwargs["json"] = request_info.json
            elif request_info.form is not None:
                kwargs["data"] = request_info.form
            elif request_info.data is not None:
                kwargs["content"] = request_info.data

            # Perform HTTP request
            response = await self._client.request(**kwargs)

            # Track latency
            self._stats["total_response_time"] += response.elapsed.total_seconds()

            # Error response handling
            if response.is_error:
                raise APIResponseError(
                    message=f"API Request failed: {response.reason_phrase}",
                    status_code=response.status_code,
                    data=response.text,
                )

            self._stats["successful_requests"] += 1

            # Parse response body
            try:
                response_data = response.json() if response.content else None
            except (httpx.DecodingError, ValueError):
                response_data = {
                    "raw_body": response.text,
                    "decoding_error": True,
                }

            # Build APIResponse
            return APIResponse(
                status=response.status_code,
                data=response_data,
                headers=dict(response.headers),
                url=str(response.url),
                request_time=response.elapsed.total_seconds(),
                response_time=datetime.now(),
                metadata=request_info.metadata.copy() if request_info.metadata else {},
            )

        except APIError:
            raise
        except httpx.TimeoutException as e:
            raise APITimeoutError(f"Request timed out: {e}") from e
        except httpx.ConnectError as e:
            raise APIConnectionError(f"Connection failed: {e}") from e
        except Exception as e:
            raise APIError(f"Unexpected error: {e}") from e

    async def get_stats(self) -> Dict[str, Any]:
        stats = self._stats.copy()

        stats["uptime"] = str(datetime.now() - stats["start_time"])
        stats["retried_requests"] = getattr(self.retry_handler, "total_retries", 0)

        total = stats["total_requests"]
        stats["avg_latency"] = round(stats["total_response_time"] / total, 3) if total > 0 else 0.0
        stats["total_response_time"] = round(stats["total_response_time"], 3)

        return stats
