"""
TrendMaster Universal Retry Handler.
Path: src/common/retry.py
"""

import inspect
import logging
import random
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional, Type, TypeVar, Union

import anyio

T = TypeVar("T")
logger = logging.getLogger("TrendMaster.Common")


class RetryExhaustedError(Exception):
    """Raised when all retry attempts are exhausted on a result-based retry condition."""
    def __init__(self, message: str, last_result: Any = None):
        super().__init__(message)
        self.last_result = last_result


class _ResultRetryTrigger(Exception):
    """Internal exception used to signal result-based retry conditions."""
    def __init__(self, message: str, result: Any = None):
        super().__init__(message)
        self.result = result


@dataclass(frozen=True)
class RetryConfig:
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 10.0
    exponential: bool = True
    jitter: bool = True
    retry_on_exception: Optional[tuple[Type[Exception], ...]] = None
    retry_on_result: Optional[Callable[[Any], bool]] = None


class RetryHandler:
    def __init__(self, config: Optional[RetryConfig] = None):
        self.config = config or RetryConfig()
        self.total_retries = 0
        self.total_delay = 0.0

    async def execute(
        self,
        func: Union[Callable[..., T], Callable[..., Awaitable[T]], Awaitable[T]],
        *args,
        run_sync_in_thread: bool = False,
        **kwargs,
    ) -> T:
        """
        Execute a function with retry logic.

        Args:
            func: Async or sync callable, or coroutine object.
            *args: Positional arguments forwarded to func.
            run_sync_in_thread: If True and func is synchronous, runs it in a
                thread pool via anyio. Use only for blocking I/O calls.
            **kwargs: Keyword arguments forwarded to func.
        """
        func_name = getattr(func, "__name__", "anonymous_func")
        cancelled_exc = anyio.get_cancelled_exc_class()

        for attempt in range(1, self.config.max_attempts + 1):
            try:
                # Async function
                if inspect.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)

                # Coroutine object (awaitable)
                elif inspect.isawaitable(func):
                    result = await func

                # Sync function in thread
                elif run_sync_in_thread:
                    result = await anyio.to_thread.run_sync(lambda: func(*args, **kwargs))

                # Sync function directly
                else:
                    result = func(*args, **kwargs)

                # Result-based retry
                if self.config.retry_on_result and self.config.retry_on_result(result):
                    raise _ResultRetryTrigger("Result-based retry triggered", result=result)

                return result

            except Exception as e:
                # Fatal exceptions should never be retried
                if isinstance(e, (KeyboardInterrupt, SystemExit, cancelled_exc)):
                    raise

                is_result_retry = isinstance(e, _ResultRetryTrigger)
                is_exception_retry = (
                    self.config.retry_on_exception
                    and isinstance(e, self.config.retry_on_exception)
                )

                if not (is_result_retry or is_exception_retry):
                    raise

                # Final attempt — raise appropriate error
                if attempt == self.config.max_attempts:
                    if is_result_retry:
                        final_error = RetryExhaustedError(
                            f"[{func_name}] Result-based retry exhausted after "
                            f"{self.config.max_attempts} attempts. "
                            f"Last result: {e.result!r}",
                            last_result=e.result,
                        )
                        logger.error(str(final_error))
                        raise final_error from e

                    logger.error(
                        f"[{func_name}] Exception-based retry exhausted after "
                        f"{attempt}/{self.config.max_attempts} attempts. "
                        f"{type(e).__name__}: {e}"
                    )
                    raise

                # Retry path
                self.total_retries += 1
                delay = self._calculate_delay(attempt)
                self.total_delay += delay

                logger.warning(
                    f"[{func_name}] Attempt {attempt}/{self.config.max_attempts} failed "
                    f"({type(e).__name__}: {e}). Retrying in {delay:.2f}s..."
                )

                await anyio.sleep(delay)

    def _calculate_delay(self, attempt: int) -> float:
        # Exponential backoff
        if self.config.exponential:
            raw = min(self.config.base_delay * (2 ** (attempt - 1)), self.config.max_delay)
        else:
            raw = self.config.base_delay

        # Equal jitter
        if self.config.jitter:
            return raw / 2 + random.uniform(0, raw / 2)

        return raw