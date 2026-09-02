"""Generic bounded synchronous retry helpers.

It combines cancellation checks, bounded exponential backoff, and optional provider-throttle
outcomes without replaying a successful operation.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

from ..errors import SchemaSanitizerCancelledError
from .async_scheduler import retry_delay
from .cancellation import cancellable_sleep, check_operation_cancelled

T = TypeVar("T")
_MAX_SYNC_RETRIES = 32


def retry_sync(
    operation: Callable[[], T],
    *,
    retries: int,
    should_retry: Callable[[Exception], bool] | None = None,
    throttle_key: str | None = None,
) -> T:
    """Run one blocking operation with bounded retry and backoff."""
    bounded_retries = min(max(int(retries), 0), _MAX_SYNC_RETRIES)
    for attempt in range(bounded_retries + 1):
        check_operation_cancelled(stage="sync_retry")
        lease = None
        try:
            if throttle_key is not None:
                from ..remote_impl.provider_throttle import acquire_provider_request_sync

                lease = acquire_provider_request_sync(throttle_key)
            result = operation()
        except SchemaSanitizerCancelledError:
            if lease is not None:
                lease.release()
            raise
        except Exception as exc:
            if lease is not None:
                lease.failure(exc)
            retryable = should_retry(exc) if should_retry is not None else True
            if attempt >= bounded_retries or not retryable:
                raise
            cancellable_sleep(retry_delay(attempt), stage="sync_retry_backoff")
            continue
        except BaseException:
            if lease is not None:
                lease.release()
            raise

        # The user operation has already completed successfully. Keep outcome
        # telemetry outside the retryable region so instrumentation failures
        # cannot replay a non-idempotent blocking request.
        if lease is not None:
            lease.success()
        check_operation_cancelled(stage="sync_retry")
        return result
    raise RuntimeError("unreachable synchronous retry state")


__all__ = ["retry_sync"]
