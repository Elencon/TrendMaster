r"""
TrendMaster Async API Client.
Path: src/api/api_client.py
"""
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional
import httpx

from src.common import RetryHandler, RetryConfig
from src.common.exceptions import (
    APIConnectionError,
    APITimeoutError,
    APIResponseError,
    APIError
)
# Import your RateLimiter classes
from .rate_limiter import RateLimiter, RateLimitConfig
from .api_models import APIRequest, APIResponse, RequestStats

from src.logging_system import get_api_logger
logger = get_api_logger()

class AsyncAPIClient:
    def __init__(
        self,
        base_url: str = "https://etl-server.fly.dev",
        timeout: float = 10.0,
        retry_config: Optional[RetryConfig] = None,
        rate_limit_config: Optional[RateLimitConfig] = None, # Added config to init
        transport: Optional[Any] = None,
    ):
        self._base_url = base_url
        self._timeout = timeout
        
        # 1. Initialize Rate Limiter
        self.rate_limiter = RateLimiter(rate_limit_config)
        
        self.retry_config = retry_config or RetryConfig(
            max_attempts=3,
            base_delay=1.5,
            retry_on_exception=(APIConnectionError, APITimeoutError, APIResponseError),
            retry_on_result=lambda res: hasattr(res, "data") and (res.data is None or res.data == {})
        )
        
        self.retry_handler = RetryHandler(config=self.retry_config)

        self._client = httpx.AsyncClient(
            base_url=base_url,
            timeout=timeout,
            follow_redirects=True,
            transport=transport,
            # Note: httpx Limits still apply at the connection level, 
            # while your RateLimiter applies at the logic level.
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10)
        )
        
        self.processing_stats = RequestStats()

    @property
    def base_url(self) -> str:
        return self._base_url

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def close(self):
        if not self._client.is_closed:
            await self._client.aclose()

    async def request(self, request_info: APIRequest) -> APIResponse:
        """
        Main entry point. Uses RetryHandler which calls _execute_request.
        """
        return await self.retry_handler.execute_async(self._execute_request, request_info)

    async def _execute_request(self, request_info: APIRequest) -> APIResponse:
        """
        Internal raw HTTP call wrapped in the RateLimiter context.
        """
        # 2. Apply Rate Limiting Context here
        async with self.rate_limiter.limit_context():
            try:
                kwargs: Dict[str, Any] = {
                    "method": request_info.method.value,
                    "url": request_info.url,
                    "params": request_info.params,
                    "headers": request_info.headers,
                    "timeout": request_info.timeout or self._timeout,
                }

                if request_info.json is not None:
                    kwargs["json"] = request_info.json
                elif request_info.form is not None:
                    kwargs["data"] = request_info.form
                elif request_info.data is not None:
                    kwargs["content"] = request_info.data

                start = time.perf_counter()
                response = await self._client.request(**kwargs)
                duration = time.perf_counter() - start

                # 3. Fixed Stats call to match RequestStats.record signature
                self.processing_stats.record(
                    success=response.is_success,
                    response_time=duration  # still correct
                )

                if response.is_error:
                    raise APIResponseError(
                        message=f"API Request failed: {response.reason_phrase}",
                        status_code=response.status_code,
                        data=response.text,
                    )

                try:
                    response_data = response.json() if response.content else None
                except (httpx.DecodingError, ValueError):
                    response_data = {
                        "raw_body": response.text,
                        "decoding_error": True,
                    }

                return APIResponse(
                    status=response.status_code,
                    data=response_data,
                    headers=dict(response.headers),
                    url=str(response.url),
                    request_time=response.elapsed.total_seconds(),
                    response_time=datetime.now(timezone.utc),
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

    async def get_stats(self) -> dict[str, Any]:
        return self.processing_stats.to_dict()