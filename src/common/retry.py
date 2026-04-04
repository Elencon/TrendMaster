"""
Path: src/common/retry.py
TrendMaster Universal Retry Handler.
multiple modifications  check sttructural log
"""

import inspect
import anyio
import logging
import random
import functools
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional, TypeVar, Union

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
    """Internal exception to trigger retry based on result."""

    def __init__(self, message: str, result: Any):
        super().__init__(message)
        self.result = result

# ────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────

@dataclass(frozen=True)
class RetryConfig:
    """Configuration for RetryHandler."""

    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0          # More reasonable default for exponential backoff
    exponential: bool = True
    jitter: bool = True

 
    retry_on_exception: type[Exception] | tuple[type[Exception], ...] | None = None
    retry_on_result: Callable[[Any], bool] | None = None

# ────────────────────────────────────────────────
# Retry Handler
# ────────────────────────────────────────────────

class RetryHandler:
    def __init__(self, config: Optional[RetryConfig] = None):
        self.config = config or RetryConfig()
        self.total_retries = 0
        self.total_delay = 0.0

    def _log_structured(self, level: int, message: str, **fields) -> None:
        logger.log(level, message, extra=fields)

    # ────────────────────────────────────────────
    # Core execution logic
    # ────────────────────────────────────────────
    async def _execute_core(
        self,
        func: Callable[..., Any],
        args: tuple,
        kwargs: dict,
        func_name: str,
        is_coroutine_function: bool,
        run_sync_in_thread: bool = False,
    ) -> T:
        cancelled_exc = anyio.get_cancelled_exc_class()

        for attempt in range(1, self.config.max_attempts + 1):
            try:
                # Execute the function
                if is_coroutine_function:
                    result = func(*args, **kwargs)
                    if inspect.isawaitable(result):
                        result = await result
                elif run_sync_in_thread:
                    result = await anyio.to_thread.run_sync(
                        functools.partial(func, *args, **kwargs)
                    )
                else:
                    result = func(*args, **kwargs)

                # Result-based retry trigger
                if self.config.retry_on_result and self.config.retry_on_result(result):
                    raise _ResultRetryTrigger(
                        "Result-based retry triggered", result=result
                    )

                return result

            except Exception as e:
                if isinstance(e, (cancelled_exc, KeyboardInterrupt, SystemExit)):
                    raise

                if not self._should_retry(e):
                    raise

                if attempt == self.config.max_attempts:
                    self._log_structured(
                        logging.ERROR,
                        "Retry exhausted",
                        function=func_name,
                        attempts=attempt,
                        exception=type(e).__name__,
                    )
                    if isinstance(e, _ResultRetryTrigger):
                        raise RetryExhaustedError(
                            f"Max attempts reached for {func_name} (result-based)",
                            last_result=e.result,
                        ) from e
                    raise RetryExhaustedError(f"Max attempts reached for {func_name}") from e

                # Log and delay before next attempt
                self._log_structured(
                    logging.WARNING,
                    "Failure detected, retrying...",
                    function=func_name,
                    attempt=attempt,
                    exception=type(e).__name__,
                )

                delay = self._calculate_delay(attempt)
                self.total_retries += 1
                self.total_delay += delay

                await anyio.sleep(delay)

        raise RuntimeError("RetryHandler: unexpected fallthrough")

    def _should_retry(self, e: Exception) -> bool:
        """Determine if we should retry on this exception."""
        if isinstance(e, _ResultRetryTrigger):
            return True
        if self.config.retry_on_exception is None:
            return True  # retry on all exceptions by default
        return isinstance(e, self.config.retry_on_exception)

    def _calculate_delay(self, attempt: int) -> float:
        if self.config.exponential:
            raw = min(
                self.config.base_delay * (2 ** (attempt - 1)),
                self.config.max_delay,
            )
        else:
            raw = self.config.base_delay

        if self.config.jitter:
            return raw / 2 + random.uniform(0, raw / 2)
        return raw

    # ────────────────────────────────────────────
    # Public API
    # ────────────────────────────────────────────

    async def execute_async(
        self,
        func: Union[Callable[..., T], Callable[..., Awaitable[T]]],
        *args,
        run_sync_in_thread: bool = False,
        **kwargs,
    ) -> T:
        """Execute async or sync function with retries (from async context)."""
        func_name = getattr(func, "__name__", "anonymous_func")
        is_coroutine_function = inspect.iscoroutinefunction(func)

        return await self._execute_core(
            func,
            args,
            kwargs,
            func_name,
            is_coroutine_function=is_coroutine_function,
            run_sync_in_thread=run_sync_in_thread,
        )

    def execute_sync(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Execute sync function with retries (from sync context)."""
        func_name = getattr(func, "__name__", "anonymous_func")

        return anyio.run(
            self._execute_core,
            func,
            args,
            kwargs,
            func_name,
            False,  # is_coroutine_function
            False,  # run_sync_in_thread
        )

    async def execute(self, *args, **kwargs) -> T:
        """Convenience alias for execute_async."""
        return await self.execute_async(*args, **kwargs)


# ────────────────────────────────────────────────
# Decorators
# ────────────────────────────────────────────────

def retryable_sync(config: Optional[RetryConfig] = None):
    """Decorator for synchronous functions with retries."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return RetryHandler(config).execute_sync(func, *args, **kwargs)
        return wrapper
    return decorator


def retryable_async(config: Optional[RetryConfig] = None, run_sync_in_thread: bool = False):
    """Decorator for asynchronous functions (or sync functions run in thread)."""
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            return await RetryHandler(config).execute_async(
                func, *args, run_sync_in_thread=run_sync_in_thread, **kwargs
            )
        return wrapper
    return decorator