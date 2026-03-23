import pytest
from datetime import datetime
from src.api.api_models import APIRequest, APIResponse, RequestMethod

def test_request_method_idempotency():
    assert RequestMethod.GET.is_idempotent is True
    assert RequestMethod.PUT.is_idempotent is True
    assert RequestMethod.DELETE.is_idempotent is True
    assert RequestMethod.POST.is_idempotent is False
    assert RequestMethod.PATCH.is_idempotent is False

def test_api_request_creation():
    request = APIRequest(
        url="https://api.example.com/data",
        method=RequestMethod.GET,
        params={"id": 123},
        headers={"Authorization": "Bearer token"}
    )
    assert request.url == "https://api.example.com/data"
    assert request.method == RequestMethod.GET
    assert request.params == {"id": 123}
    assert request.headers == {"Authorization": "Bearer token"}

def test_api_response_properties():
    response = APIResponse(
        status=200,
        data={"result": "success"},
        headers={"Content-Type": "application/json"},
        url="https://api.example.com/data",
        request_time=0.150,
        response_time=datetime.now()
    )
    assert response.http_success is True
    assert response.latency_ms == 150

    error_response = APIResponse(
        status=404,
        data=None,
        headers={},
        url="https://api.example.com/data",
        request_time=0.050,
        response_time=datetime.now()
    )
    assert error_response.http_success is False
