"""
API-specific configuration utilities and presets.
"""

from dataclasses import dataclass, field
from typing import Dict, List
from .etl_config import APIConfig


# ---------------------------------------------------------------------------
# REST API CONFIG
# ---------------------------------------------------------------------------

@dataclass
class RESTAPIConfig(APIConfig):
    """REST API configuration with clean defaults."""

    default_format: str = "json"
    pagination_limit: int = 100
    max_page_size: int = 1000
    endpoints: Dict[str, str] = field(default_factory=lambda: {
        "customers": "/api/customers",
        "orders": "/api/orders",
        "order_items": "/api/order_items",
        "products": "/api/products",
        "health": "/health",
    })

    def get_endpoint_url(self, endpoint: str) -> str:
        base = self.base_url.rstrip("/")
        path = self.endpoints.get(endpoint, f"/{endpoint.lstrip('/')}")
        return f"{base}{path}"


# ---------------------------------------------------------------------------
# GRAPHQL API CONFIG
# ---------------------------------------------------------------------------

@dataclass
class GraphQLAPIConfig(APIConfig):
    """GraphQL API configuration."""

    endpoint: str = "/graphql"
    introspection_enabled: bool = True
    max_query_depth: int = 10
    query_timeout: int = 30

    def get_graphql_url(self) -> str:
        base = self.base_url.rstrip("/")
        return f"{base}/{self.endpoint.lstrip('/')}"


# ---------------------------------------------------------------------------
# ASYNC API CONFIG
# ---------------------------------------------------------------------------

@dataclass
class AsyncAPIConfig(APIConfig):
    """Async API configuration with concurrency and retry controls."""

    connector_limit: int = 100
    connector_limit_per_host: int = 30
    total_timeout: float = 300.0

    async_retry_attempts: int = 3
    async_backoff_factor: float = 0.1
    async_max_backoff: float = 10.0

    enable_circuit_breaker: bool = True
    failure_threshold: int = 5
    recovery_timeout: int = 60

    expected_exception_types: List[str] = field(default_factory=lambda: [
        "aiohttp.ClientError",
        "asyncio.TimeoutError",
        "ConnectionError",
    ])

    def __post_init__(self):
        if self.connector_limit_per_host > self.connector_limit:
            raise ValueError("connector_limit_per_host cannot exceed connector_limit")

# ---------------------------------------------------------------------------
# Shared Factory Function
# ---------------------------------------------------------------------------

def api_config(base_url: str, **overrides) -> RESTAPIConfig:
    """
    Shared REST API config factory.
    Keeps defaults DRY and allows overrides.
    """
    defaults = dict(
        base_url=base_url,
        timeout=15,
        retries=2,
        pagination_limit=100,
        max_concurrent_requests=5,
    )
    defaults.update(overrides)
    return RESTAPIConfig(**defaults)


# ---------------------------------------------------------------------------
# PREDEFINED CONFIG FACTORIES
# ---------------------------------------------------------------------------

def get_etl_server_config() -> RESTAPIConfig:
    return api_config(
        "https://etl-server.fly.dev",
        timeout=30,
        retries=3,
        pagination_limit=500,
        max_concurrent_requests=10,
    )


def get_jsonplaceholder_config() -> RESTAPIConfig:
    return api_config(
        "https://jsonplaceholder.typicode.com",
        endpoints={
            "posts": "/posts",
            "users": "/users",
            "comments": "/comments",
            "albums": "/albums",
            "photos": "/photos",
        },
    )


def get_local_dev_config() -> RESTAPIConfig:
    return api_config(
        "http://localhost:8000",
        timeout=10,
        retries=1,
        max_concurrent_requests=5,
        rate_limit_calls=1000,
    )


def get_async_production_config() -> AsyncAPIConfig:
    return AsyncAPIConfig(
        max_concurrent_requests=50,
        connector_limit=200,
        connector_limit_per_host=50,
        total_timeout=600.0,
        failure_threshold=10,
    )
