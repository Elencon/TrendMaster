"""
Path: src/common/retry.py
TrendMaster Universal Retry Handler.
"""

import inspect
import anyio
import logging
import random
import time
import json
import functools
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional, Type, TypeVar, Union

T = TypeVar("T")
logger = logging.getLogger(__name__)


# ────────────────────────────────────────────────
# Exceptions
# ────────────────────────────────────────────────

class RetryExhaustedError(Exception):
    """Raised when all retry attempts are exhausted."""
    def __init__(self, message: str, last_result: Any = None):
        super().__init__(message)
        self.last_result = last_result


class _ResultRetryTrigger(Exception):
    """Internal exception used to signal result-based retry conditions."""
    def __init__(self, message: str, result: Any = None):
        super().__init__(message)
        self.result = result


# ────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────

@dataclass(frozen=True)
class RetryConfig:
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 10.0
    exponential: bool = True
    jitter: bool = True

    # FIX 1: restore default retry_on_exception
    retry_on_exception: Optional[tuple[Type[Exception], ...]] = (Exception,)

    retry_on_result: Optional[Callable[[Any], bool]] = None


# ────────────────────────────────────────────────
# Retry Handler
# ────────────────────────────────────────────────

class RetryHandler:
    def __init__(self, config: Optional[RetryConfig] = None):
        self.config = config or RetryConfig()
        self.total_retries = 0
        self.total_delay = 0.0

    def _log_structured(self, level: int, message: str, **fields):
        log_entry = {"message": message, **fields}
        logger.log(level, json.dumps(log_entry))

    # ────────────────────────────────────────────
    # Public API (Async)
    # ────────────────────────────────────────────

    async def execute_async(
        self,
        func: Union[Callable[..., T], Callable[..., Awaitable[T]]],
        *args,
        run_sync_in_thread: bool = False,
        **kwargs,
    ) -> T:

        # FIX 2: prevent leaking internal kwarg into user functions
        kwargs.pop("run_sync_in_thread", None)

        func_name = getattr(func, "__name__", "anonymous_func")
        cancelled_exc = anyio.get_cancelled_exc_class()

        for attempt in range(1, self.config.max_attempts + 1):
            try:
                # Execute function
                if inspect.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)

                elif run_sync_in_thread:
                    result = await anyio.to_thread.run_sync(
                        functools.partial(func, *args, **kwargs)
                    )

                else:
                    result = func(*args, **kwargs)
                    if inspect.isawaitable(result):
                        result = await result

                # Result-based retry
                if self.config.retry_on_result and self.config.retry_on_result(result):
                    raise _ResultRetryTrigger("Result-based retry triggered", result=result)

                return result

            except Exception as e:
                if isinstance(e, (KeyboardInterrupt, SystemExit, cancelled_exc)):
                    raise

                self._handle_failure(e, attempt, func_name, is_async=True)

                if attempt < self.config.max_attempts:
                    delay = self._calculate_delay(attempt)
                    self.total_retries += 1
                    self.total_delay += delay
                    await anyio.sleep(delay)

    # ────────────────────────────────────────────
    # Public API (Sync wrapper if needed)
    # ────────────────────────────────────────────

    async def execute(
        self,
        func,
        *args,
        run_sync_in_thread: bool = False,
        **kwargs,
    ) -> T:
        return await self.execute_async(
            func,
            *args,
            run_sync_in_thread=run_sync_in_thread,
            **kwargs,
        )

    # ────────────────────────────────────────────
    # Internal helpers
    # ────────────────────────────────────────────

    def execute_sync(self, func: Callable[..., T], *args, **kwargs) -> T:
        func_name = getattr(func, "__name__", "anonymous_func")

        for attempt in range(1, self.config.max_attempts + 1):
            try:
                result = func(*args, **kwargs)

                if self.config.retry_on_result and self.config.retry_on_result(result):
                    raise _ResultRetryTrigger("Result-based retry triggered", result=result)

                return result

            except Exception as e:
                if isinstance(e, (KeyboardInterrupt, SystemExit)):
                    raise

                self._handle_failure(e, attempt, func_name, is_async=False)

                if attempt < self.config.max_attempts:
                    delay = self._calculate_delay(attempt)
                    self.total_retries += 1
                    self.total_delay += delay
                    time.sleep(delay)

    def _handle_failure(self, e: Exception, attempt: int, func_name: str, is_async: bool):
        is_result_retry = isinstance(e, _ResultRetryTrigger)

        is_exception_retry = (
            self.config.retry_on_exception
            and isinstance(e, self.config.retry_on_exception)
        )

        if not (is_result_retry or is_exception_retry):
            raise e

        if attempt == self.config.max_attempts:
            mode = "async" if is_async else "sync"

            self._log_structured(
                logging.ERROR,
                f"Retry exhausted ({mode})",
                function=func_name,
                attempts=attempt,
            )

            if is_result_retry:
                raise RetryExhaustedError(
                    f"Max attempts reached for {func_name}",
                    last_result=e.result
                ) from e

            raise e

        self._log_structured(
            logging.WARNING,
            "Failure detected, retrying...",
            function=func_name,
            attempt=attempt,
            exception=type(e).__name__,
        )

    def _calculate_delay(self, attempt: int) -> float:
        if self.config.exponential:
            raw = min(
                self.config.base_delay * (2 ** (attempt - 1)),
                self.config.max_delay
            )
        else:
            raw = self.config.base_delay

        if self.config.jitter:
            return raw / 2 + random.uniform(0, raw / 2)

        return raw


# ────────────────────────────────────────────────
# Decorators
# ────────────────────────────────────────────────

def retryable_sync(config: Optional[RetryConfig] = None):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return RetryHandler(config).execute_sync(func, *args, **kwargs)
        return wrapper
    return decorator


def retryable_async(config: Optional[RetryConfig] = None, run_sync_in_thread: bool = False):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            return await RetryHandler(config).execute_async(
                func,
                *args,
                run_sync_in_thread=run_sync_in_thread,
                **kwargs,
            )
        return wrapper
    return decorator