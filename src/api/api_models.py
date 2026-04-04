r"""
C:\Economy\Invest\TrendMaster\src\api\api_models.py
TrendMaster API Models.
Path: src/api/api_models.py
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Dict, Optional, Union, List, Generic, TypeVar
from datetime import datetime, timezone
from http import HTTPStatus

from msgspec import Struct, field

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

class APIRequest(Struct):
    url: str
    method: RequestMethod = RequestMethod.GET

    headers: Dict[str, str] = field(default_factory=dict)
    params: Dict[str, Any] = field(default_factory=dict)

    # Payload options
    json: Optional[Dict[str, Any]] = None
    data: Optional[Union[str, bytes, Dict[str, Any]]] = None
    form: Optional[Dict[str, Any]] = None

    timeout: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

class APIResponse(Struct):
    status: int
    data: Any
    headers: Dict[str, str]
    url: str
    request_time: float
    response_time: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_success(self) -> bool:
        return HTTPStatus(self.status).value // 100 == 2

    @property
    def latency_ms(self) -> int:
        return int(self.request_time * 1000)


class TypedAPIResponse(Struct, Generic[T]):
    status: int
    url: str
    request_time: float
    response_time: datetime

    is_success: bool
    latency_ms: int
    is_valid: bool

    raw_data: Any = None
    headers: Dict[str, str] = field(default_factory=dict)
    parsed: Optional[Union[T, List[T]]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class RequestStats(Struct):
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_response_time: float = 0.0
    start_time: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def record(self, success: bool, response_time: float):
        self.total_requests += 1
        self.total_response_time += response_time

        if success:
            self.successful_requests += 1
        else:
            self.failed_requests += 1

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "total_response_time": round(self.total_response_time, 3),
            "avg_response_time": self.avg_response_time,
            "success_rate": self.success_rate,
            "start_time": self.start_time.isoformat(),
        }

    def reset(self):
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.total_response_time = 0.0
        self.start_time = datetime.now(timezone.utc)


    @property
    def avg_response_time(self) -> float:
        total = self.total_requests
        if total == 0:
            return 0.0
        return round(self.total_response_time / total, 3)

    @property
    def success_rate(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return self.successful_requests / self.total_requests
