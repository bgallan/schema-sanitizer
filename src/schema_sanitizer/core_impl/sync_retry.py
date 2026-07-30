"""Generic bounded synchronous retry helpers."""

from __future__ import annotations

from collections.abc import Callable
from time import sleep
from typing import TypeVar

from .async_scheduler import retry_delay

T = TypeVar("T")
_MAX_SYNC_RETRIES = 32


def retry_sync(
    operation: Callable[[], T],
    *,
    retries: int,
    should_retry: Callable[[Exception], bool] | None = None,
) -> T:
    """Run one blocking operation with bounded retry and backoff."""
    bounded_retries = min(max(int(retries), 0), _MAX_SYNC_RETRIES)
    for attempt in range(bounded_retries + 1):
        try:
            return operation()
        except Exception as exc:
            retryable = should_retry(exc) if should_retry is not None else True
            if attempt >= bounded_retries or not retryable:
                raise
            sleep(retry_delay(attempt))
    raise RuntimeError("unreachable synchronous retry state")


__all__ = ["retry_sync"]
