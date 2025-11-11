"""Rate limiting and backoff utilities."""

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


@dataclass
class RateLimiter:
    """Token bucket rate limiter."""

    requests_per_second: float
    max_retries: int = 5
    backoff_start: float = 1.0
    backoff_max: float = 60.0

    def __post_init__(self) -> None:
        self.min_interval = 1.0 / self.requests_per_second
        self.last_request = 0.0

    async def acquire(self) -> None:
        """Wait until next request is allowed."""
        now = time.monotonic()
        time_since_last = now - self.last_request

        if time_since_last < self.min_interval:
            wait_time = self.min_interval - time_since_last
            await asyncio.sleep(wait_time)

        self.last_request = time.monotonic()

    def calculate_backoff(self, attempt: int) -> float:
        """
        Calculate exponential backoff time.

        Args:
            attempt: Attempt number (0-indexed)

        Returns:
            Backoff time in seconds
        """
        backoff = self.backoff_start * (2**attempt)
        return float(min(backoff, self.backoff_max))


@dataclass
class RetryConfig:
    """Configuration for retry logic."""

    max_retries: int
    backoff_start: float
    backoff_max: float
    retryable_exceptions: tuple[type[Exception], ...] = (Exception,)

    def should_retry(self, attempt: int, exception: Exception) -> bool:
        """
        Check if should retry after exception.

        Args:
            attempt: Current attempt number (0-indexed)
            exception: Exception that occurred

        Returns:
            True if should retry
        """
        if attempt >= self.max_retries:
            return False

        return isinstance(exception, self.retryable_exceptions)

    def get_backoff(self, attempt: int) -> float:
        """
        Get backoff time for attempt.

        Args:
            attempt: Attempt number (0-indexed)

        Returns:
            Backoff time in seconds
        """
        backoff = self.backoff_start * (2**attempt)
        return float(min(backoff, self.backoff_max))


async def with_retry(
    func: Callable[..., Any],
    retry_config: RetryConfig,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """
    Execute async function with retry logic.

    Args:
        func: Async function to execute
        retry_config: Retry configuration
        *args: Function args
        **kwargs: Function kwargs

    Returns:
        Function result

    Raises:
        Last exception if all retries exhausted
    """
    last_exception: Exception | None = None

    for attempt in range(retry_config.max_retries + 1):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            last_exception = e

            if not retry_config.should_retry(attempt, e):
                raise

            backoff = retry_config.get_backoff(attempt)
            await asyncio.sleep(backoff)

    # Should not reach here, but just in case
    if last_exception:
        raise last_exception
    raise RuntimeError("Retry logic error")
