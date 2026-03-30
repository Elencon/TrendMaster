"""
Tests for TrendMaster Universal Retry Handler.
Path: tests/common/test_retry.py
"""

import pytest
from unittest.mock import AsyncMock, MagicMock

from src.common.retry import RetryHandler, RetryConfig, RetryExhaustedError
from src.common.exceptions import APIConnectionError, APITimeoutError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_handler(config: RetryConfig = None) -> RetryHandler:
    return RetryHandler(config=config)


# ---------------------------------------------------------------------------
# Success cases
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_async_func_succeeds_on_first_attempt():
    func = AsyncMock(return_value="ok")
    handler = make_handler()

    result = await handler.execute_async(func)

    assert result == "ok"
    assert func.call_count == 1
    assert handler.total_retries == 0


@pytest.mark.asyncio
async def test_sync_func_succeeds_on_first_attempt():
    func = MagicMock(return_value=42)
    handler = make_handler()

    result = await handler.execute_async(func)

    assert result == 42
    assert func.call_count == 1
    assert handler.total_retries == 0


@pytest.mark.asyncio
async def test_sync_func_in_thread_succeeds():
    func = MagicMock(return_value="threaded")
    handler = make_handler()

    result = await handler.execute_async(func, run_sync_in_thread=True)

    assert result == "threaded"
    assert func.call_count == 1
    assert handler.total_retries == 0


# ---------------------------------------------------------------------------
# Exception-based retry
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_retries_on_matching_exception():
    func = AsyncMock(side_effect=[APIConnectionError("fail"), APIConnectionError("fail"), "ok"])
    config = RetryConfig(
        max_attempts=3,
        base_delay=0,
        jitter=False,
        retry_on_exception=(APIConnectionError,),
    )
    handler = make_handler(config)

    result = await handler.execute_async(func)

    assert result == "ok"
    assert func.call_count == 3
    assert handler.total_retries == 2


@pytest.mark.asyncio
async def test_does_not_retry_on_non_matching_exception():
    func = AsyncMock(side_effect=ValueError("unexpected"))
    config = RetryConfig(
        max_attempts=3,
        base_delay=0,
        jitter=False,
        retry_on_exception=(APIConnectionError,),
    )
    handler = make_handler(config)

    with pytest.raises(ValueError, match="unexpected"):
        await handler.execute_async(func)

    assert func.call_count == 1
    assert handler.total_retries == 0


@pytest.mark.asyncio
async def test_exhausts_retries_and_reraises_exception():
    func = AsyncMock(side_effect=APITimeoutError("timeout"))
    config = RetryConfig(
        max_attempts=3,
        base_delay=0,
        jitter=False,
        retry_on_exception=(APITimeoutError,),
    )
    handler = make_handler(config)

    with pytest.raises(APITimeoutError, match="timeout"):
        await handler.execute_async(func)

    assert func.call_count == 3
    assert handler.total_retries == 2


@pytest.mark.asyncio
async def test_succeeds_on_second_attempt():
    func = AsyncMock(side_effect=[APIConnectionError("fail"), "recovered"])
    config = RetryConfig(
        max_attempts=3,
        base_delay=0,
        jitter=False,
        retry_on_exception=(APIConnectionError,),
    )
    handler = make_handler(config)

    result = await handler.execute_async(func)

    assert result == "recovered"
    assert func.call_count == 2
    assert handler.total_retries == 1


# ---------------------------------------------------------------------------
# Result-based retry
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_retries_on_matching_result():
    func = AsyncMock(side_effect=[None, None, {"data": [1, 2, 3]}])
    config = RetryConfig(
        max_attempts=3,
        base_delay=0,
        jitter=False,
        retry_on_result=lambda r: r is None,
    )
    handler = make_handler(config)

    result = await handler.execute_async(func)

    assert result == {"data": [1, 2, 3]}
    assert func.call_count == 3
    assert handler.total_retries == 2


@pytest.mark.asyncio
async def test_raises_retry_exhausted_error_on_bad_result():
    func = AsyncMock(return_value=None)
    config = RetryConfig(
        max_attempts=3,
        base_delay=0,
        jitter=False,
        retry_on_result=lambda r: r is None,
    )
    handler = make_handler(config)

    with pytest.raises(RetryExhaustedError) as excinfo:
        await handler.execute_async(func)

    assert excinfo.value.last_result is None
    assert func.call_count == 3


@pytest.mark.asyncio
async def test_retry_exhausted_error_carries_last_result():
    func = AsyncMock(return_value={"error": True})
    config = RetryConfig(
        max_attempts=2,
        base_delay=0,
        jitter=False,
        retry_on_result=lambda r: r.get("error") is True,
    )
    handler = make_handler(config)

    with pytest.raises(RetryExhaustedError) as excinfo:
        await handler.execute_async(func)

    assert excinfo.value.last_result == {"error": True}


# ---------------------------------------------------------------------------
# Stats tracking
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_total_retries_tracked():
    func = AsyncMock(side_effect=[APIConnectionError("e"), APIConnectionError("e"), "ok"])
    config = RetryConfig(
        max_attempts=3,
        base_delay=0,
        jitter=False,
        retry_on_exception=(APIConnectionError,),
    )
    handler = make_handler(config)

    await handler.execute_async(func)

    assert handler.total_retries == 2


@pytest.mark.asyncio
async def test_total_delay_tracked():
    func = AsyncMock(side_effect=[APIConnectionError("e"), "ok"])
    config = RetryConfig(
        max_attempts=3,
        base_delay=0.1,
        exponential=False,
        jitter=False,
        retry_on_exception=(APIConnectionError,),
    )
    handler = make_handler(config)

    await handler.execute_async(func)

    assert handler.total_delay == pytest.approx(0.1, rel=1e-3)


@pytest.mark.asyncio
async def test_no_retries_on_success():
    func = AsyncMock(return_value="fine")
    handler = make_handler()

    await handler.execute_async(func)

    assert handler.total_retries == 0
    assert handler.total_delay == 0.0


# ---------------------------------------------------------------------------
# Fatal exceptions are never retried
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_keyboard_interrupt_not_retried():
    func = AsyncMock(side_effect=KeyboardInterrupt)
    handler = make_handler()

    with pytest.raises(KeyboardInterrupt):
        await handler.execute_async(func)

    assert func.call_count == 1
    assert handler.total_retries == 0


@pytest.mark.asyncio
async def test_system_exit_not_retried():
    func = AsyncMock(side_effect=SystemExit)
    handler = make_handler()

    with pytest.raises(SystemExit):
        await handler.execute_async(func)

    assert func.call_count == 1
    assert handler.total_retries == 0


# ---------------------------------------------------------------------------
# Delay calculation
# ---------------------------------------------------------------------------

def test_exponential_backoff_no_jitter():
    config = RetryConfig(base_delay=1.0, max_delay=10.0, exponential=True, jitter=False)
    handler = make_handler(config)

    assert handler._calculate_delay(1) == 1.0
    assert handler._calculate_delay(2) == 2.0
    assert handler._calculate_delay(3) == 4.0
    assert handler._calculate_delay(4) == 8.0
    assert handler._calculate_delay(5) == 10.0


def test_flat_delay_no_jitter():
    config = RetryConfig(base_delay=2.0, exponential=False, jitter=False)
    handler = make_handler(config)

    assert handler._calculate_delay(1) == 2.0
    assert handler._calculate_delay(3) == 2.0


def test_equal_jitter_within_bounds():
    config = RetryConfig(base_delay=2.0, exponential=False, jitter=True)
    handler = make_handler(config)

    for _ in range(50):
        delay = handler._calculate_delay(1)
        assert 1.0 <= delay <= 2.0