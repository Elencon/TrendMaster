"""
API-specific configuration utilities and presets.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from . import APIConfig

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_ENDPOINTS: Dict[str, str] = {
    "customers":   "/api/customers",
    "orders":      "/api/orders",
    "order_items": "/api/order_items",
    "products":    "/api/products",
    "health":      "/health",
}

# Stored as strings intentionally — resolved later by the async client
_DEFAULT_EXCEPTION_TYPES: List[str] = [
    "aiohttp.ClientError",
    "asyncio.TimeoutError",
    "ConnectionError",
]

# ---------------------------------------------------------------------------
# Specialised API config classes
# ---------------------------------------------------------------------------

@dataclass
class RESTAPIConfig(APIConfig):
    """REST API specific configuration."""

    default_format:   str = "json"
    pagination_limit: int = 100
    max_page_size:    int = 1000

    # Endpoints overrideable per instance
    endpoints: Optional[Dict[str, str]] = None

    def __post_init__(self) -> None:
        # Avoid shared mutable defaults
        if self.endpoints is None:
            self.endpoints = dict(_DEFAULT_ENDPOINTS)

    def get_endpoint_url(self, endpoint: str) -> str:
        """
        Return the full URL for a named or relative endpoint.
        Normalises slashes to avoid accidental double-slashes.
        """
        base = self.base_url.rstrip("/")
        if endpoint in self.endpoints:
            path = self.endpoints[endpoint]
        else:
            path = f"/{endpoint.lstrip('/')}"
        return f"{base}{path}"


@dataclass
class GraphQLAPIConfig(APIConfig):
    """GraphQL API specific configuration."""

    endpoint:              str  = "/graphql"
    introspection_enabled: bool = True
    max_query_depth:       int  = 10
    query_timeout:         int  = 30

    def get_graphql_url(self) -> str:
        """Return the full GraphQL endpoint URL."""
        return f"{self.base_url.rstrip('/')}{self.endpoint}"


@dataclass
class AsyncAPIConfig(APIConfig):
    """Async API configuration with concurrency, retry, and circuit-breaker settings."""

    # Connector
    connector_limit:          int   = 100
    connector_limit_per_host: int   = 30
    total_timeout:            float = 300.0

    # Async retry
    async_retry_attempts: int   = 3
    async_backoff_factor: float = 0.1
    async_max_backoff:    float = 10.0

    # Circuit breaker
    enable_circuit_breaker:   bool                = True
    failure_threshold:        int                 = 5
    recovery_timeout:         int                 = 60
    expected_exception_types: Optional[List[str]] = None

    def __post_init__(self) -> None:
        # Avoid shared mutable defaults
        if self.expected_exception_types is None:
            self.expected_exception_types = list(_DEFAULT_EXCEPTION_TYPES)

        if self.connector_limit_per_host > self.connector_limit:
            raise ValueError(
                "connector_limit_per_host cannot exceed connector_limit"
            )


# ---------------------------------------------------------------------------
# Preset factories
# ---------------------------------------------------------------------------

def get_etl_server_config() -> RESTAPIConfig:
    """Return a config for the production ETL server API."""
    return RESTAPIConfig(
        base_url="https://etl-server.fly.dev",
        timeout=30,
        retries=3,
        pagination_limit=500,
        max_concurrent_requests=10,
    )


def get_jsonplaceholder_config() -> RESTAPIConfig:
    """Return a config for the JSONPlaceholder demo API."""
    return RESTAPIConfig(
        base_url="https://jsonplaceholder.typicode.com",
        timeout=15,
        retries=2,
        pagination_limit=100,
        endpoints={
            "posts":    "/posts",
            "users":    "/users",
            "comments": "/comments",
            "albums":   "/albums",
            "photos":   "/photos",
        },
    )


def get_local_dev_config() -> RESTAPIConfig:
    """Return a config for local development."""
    return RESTAPIConfig(
        base_url="http://localhost:8000",
        timeout=10,
        retries=1,
        max_concurrent_requests=5,
        rate_limit_calls=1000,
    )


def get_async_production_config() -> AsyncAPIConfig:
    """Return an async config tuned for production throughput."""
    return AsyncAPIConfig(
        max_concurrent_requests=50,
        semaphore_limit=25,
        connector_limit=200,
        connector_limit_per_host=50,
        total_timeout=600.0,
        enable_circuit_breaker=True,
        failure_threshold=10,
    )