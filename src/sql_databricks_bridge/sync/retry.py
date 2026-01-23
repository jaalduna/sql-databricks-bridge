"""Retry logic with exponential backoff."""

import asyncio
import logging
import random
from functools import wraps
from typing import Any, Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class RetryExhaustedError(Exception):
    """Raised when all retry attempts are exhausted."""

    def __init__(self, message: str, attempts: int, last_error: Exception | None = None):
        super().__init__(message)
        self.attempts = attempts
        self.last_error = last_error


def calculate_backoff(
    attempt: int,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: bool = True,
) -> float:
    """Calculate exponential backoff delay.

    Args:
        attempt: Current attempt number (0-indexed).
        base_delay: Base delay in seconds.
        max_delay: Maximum delay in seconds.
        jitter: Add random jitter to prevent thundering herd.

    Returns:
        Delay in seconds.
    """
    # Exponential backoff: base * 2^attempt
    delay = min(base_delay * (2 ** attempt), max_delay)

    if jitter:
        # Add random jitter (±25%)
        jitter_range = delay * 0.25
        delay += random.uniform(-jitter_range, jitter_range)

    return max(0, delay)


async def retry_async(
    func: Callable[..., Any],
    *args: Any,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    retryable_exceptions: tuple[type[Exception], ...] = (Exception,),
    on_retry: Callable[[int, Exception], None] | None = None,
    **kwargs: Any,
) -> Any:
    """Execute async function with retry logic.

    Args:
        func: Async function to execute.
        *args: Positional arguments for func.
        max_attempts: Maximum retry attempts.
        base_delay: Base delay between retries.
        max_delay: Maximum delay between retries.
        retryable_exceptions: Exception types to retry on.
        on_retry: Callback on retry (receives attempt number and exception).
        **kwargs: Keyword arguments for func.

    Returns:
        Result of func.

    Raises:
        RetryExhaustedError: If all attempts fail.
    """
    last_error: Exception | None = None

    for attempt in range(max_attempts):
        try:
            return await func(*args, **kwargs)

        except retryable_exceptions as e:
            last_error = e

            if attempt == max_attempts - 1:
                # Last attempt failed
                raise RetryExhaustedError(
                    f"All {max_attempts} attempts failed: {e}",
                    attempts=max_attempts,
                    last_error=e,
                )

            delay = calculate_backoff(attempt, base_delay, max_delay)

            logger.warning(
                f"Attempt {attempt + 1}/{max_attempts} failed: {e}. "
                f"Retrying in {delay:.2f}s..."
            )

            if on_retry:
                on_retry(attempt, e)

            await asyncio.sleep(delay)

    # Should not reach here
    raise RetryExhaustedError(
        f"All {max_attempts} attempts failed",
        attempts=max_attempts,
        last_error=last_error,
    )


def retry_sync(
    func: Callable[..., T],
    *args: Any,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    retryable_exceptions: tuple[type[Exception], ...] = (Exception,),
    on_retry: Callable[[int, Exception], None] | None = None,
    **kwargs: Any,
) -> T:
    """Execute sync function with retry logic.

    Args:
        func: Function to execute.
        *args: Positional arguments for func.
        max_attempts: Maximum retry attempts.
        base_delay: Base delay between retries.
        max_delay: Maximum delay between retries.
        retryable_exceptions: Exception types to retry on.
        on_retry: Callback on retry (receives attempt number and exception).
        **kwargs: Keyword arguments for func.

    Returns:
        Result of func.

    Raises:
        RetryExhaustedError: If all attempts fail.
    """
    import time

    last_error: Exception | None = None

    for attempt in range(max_attempts):
        try:
            return func(*args, **kwargs)

        except retryable_exceptions as e:
            last_error = e

            if attempt == max_attempts - 1:
                raise RetryExhaustedError(
                    f"All {max_attempts} attempts failed: {e}",
                    attempts=max_attempts,
                    last_error=e,
                )

            delay = calculate_backoff(attempt, base_delay, max_delay)

            logger.warning(
                f"Attempt {attempt + 1}/{max_attempts} failed: {e}. "
                f"Retrying in {delay:.2f}s..."
            )

            if on_retry:
                on_retry(attempt, e)

            time.sleep(delay)

    raise RetryExhaustedError(
        f"All {max_attempts} attempts failed",
        attempts=max_attempts,
        last_error=last_error,
    )


def with_retry(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    retryable_exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator for adding retry logic to a function.

    Args:
        max_attempts: Maximum retry attempts.
        base_delay: Base delay between retries.
        max_delay: Maximum delay between retries.
        retryable_exceptions: Exception types to retry on.

    Returns:
        Decorated function.
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            return retry_sync(
                func,
                *args,
                max_attempts=max_attempts,
                base_delay=base_delay,
                max_delay=max_delay,
                retryable_exceptions=retryable_exceptions,
                **kwargs,
            )

        return wrapper

    return decorator
