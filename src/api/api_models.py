"""
TrendMaster API Models.
Path: src/api/api_models.py
"""
from __future__ import annotations
from enum import Enum
from typing import Any, Dict, Optional, Union, List, Generic, TypeVar
from datetime import datetime
import msgspec

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

class APIRequest(msgspec.Struct):
    url: str
    method: RequestMethod = RequestMethod.GET
    headers: Dict[str, str] = {}
    params: Dict[str, Any] = {}
    
    # Payload options - aligned with Client logic
    json: Optional[Dict[str, Any]] = None 
    data: Optional[Union[str, bytes, Dict[str, Any]]] = None
    form: Optional[Dict[str, Any]] = None  
    
    timeout: Optional[float] = None
    metadata: Dict[str, Any] = {}

class APIResponse(msgspec.Struct):
    status: int
    data: Any
    headers: Dict[str, str]
    url: str
    request_time: float
    response_time: datetime
    metadata: Dict[str, Any] = {}

    @property
    def http_success(self) -> bool:
        return 200 <= self.status <= 299

    @property
    def latency_ms(self) -> int:
        return int(self.request_time * 1000)

class TypedAPIResponse(msgspec.Struct, Generic[T]):
    status: int
    url: str
    headers: Dict[str, str]
    request_time: float
    response_time: datetime
    http_success: bool
    latency_ms: int
    is_valid: bool
    raw_data: Any = None
    parsed: Optional[Union[T, List[T]]] = None