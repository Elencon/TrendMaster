r"""
C:\Economy\Invest\TrendMaster\src\api\api_models.py
TrendMaster API Models.
Path: src/api/api_models.py
"""
from __future__ import annotations

import copy
from enum import Enum
from typing import Any, Optional, Union,  Generic, TypeVar
from datetime import datetime, timezone
from http import HTTPStatus
from dataclasses import dataclass, field


T = TypeVar("T")

class RequestMethod(Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"

    @property
    def is_idempotent(self) -> bool:
        return self in {RequestMethod.GET, RequestMethod.PUT, RequestMethod.DELETE}


@dataclass(slots=True, kw_only=True)
class APIRequest:
    url: str
    method: RequestMethod = RequestMethod.GET

    headers: dict[str, str] = field(default_factory=dict)
    params: dict[str, Any] = field(default_factory=dict)

    json: Optional[dict[str, Any]] = None
    data: Optional[Union[str, bytes, dict[str, Any]]] = None
    form: Optional[dict[str, Any]] = None

    timeout: Optional[float] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True, kw_only=True)
class APIResponse:
    status: int
    data: Any
    headers: dict[str, str]
    url: str
    request_time: float
    response_time: datetime
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def is_success(self) -> bool:
        return 200 <= self.status < 300

    @property
    def latency_ms(self) -> int:
        return int(self.request_time * 1000)


T = TypeVar("T")

@dataclass(slots=True, kw_only=True)
class TypedAPIResponse(Generic[T]):
    status: int
    url: str
    request_time: float
    response_time: datetime

    raw_data: Any = None
    headers: dict[str, str] = field(default_factory=dict)
    parsed: Optional[Union[T, list[T]]] = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def is_success(self) -> bool:
        return 200 <= self.status < 300

    @property
    def latency_ms(self) -> int:
        return int(self.request_time * 1000)

    @property
    def is_valid(self) -> bool:
        return self.is_success and self.parsed is not None


@dataclass(slots=True)
class RequestStats:
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0

    # Renamed field
    total_duration: float = 0.0

    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: dict[str, Any] = field(default_factory=dict)

    def record(self, success: bool, response_time: float) -> None:
        self.total_requests += 1
        self.total_duration += response_time

        if success:
            self.successful_requests += 1
        else:
            self.failed_requests += 1

    def reset(self) -> None:
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.total_duration = 0.0
        self.start_time = datetime.now(timezone.utc)
        self.metadata.clear()

    @property
    def avg_response_time(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return self.total_duration / self.total_requests

    @property
    def success_rate(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return self.successful_requests / self.total_requests

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "total_duration": round(self.total_duration, 3),
            "avg_response_time": round(self.avg_response_time, 3),
            "success_rate": round(self.success_rate, 4),
            "metadata": copy.deepcopy(self.metadata),
        }