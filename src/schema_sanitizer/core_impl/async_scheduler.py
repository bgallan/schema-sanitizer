"""Generic bounded async scheduling and retry helpers."""

from __future__ import annotations

import asyncio
import random
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, TypeVar

from ..errors import SchemaSanitizerCancelledError
from .cancellation import cancellable_async_sleep, check_operation_cancelled

T = TypeVar("T")

_MAX_ASYNC_RETRIES = 32


def retry_delay(attempt: int) -> float:
    """Return jittered exponential backoff delay for remote I/O retries."""
    bounded_attempt = min(max(attempt, 0), 16)
    return min(8.0, 0.25 * (2**bounded_attempt)) + random.uniform(0.0, 0.25)


async def retry_async(
    operation: Callable[[], Awaitable[T]],
    *,
    retries: int,
    should_retry: Callable[[Exception], bool] | None = None,
    throttle_key: str | None = None,
) -> T:
    """Run one async operation with bounded retry/backoff."""
    bounded_retries = min(max(int(retries), 0), _MAX_ASYNC_RETRIES)
    for attempt in range(bounded_retries + 1):
        check_operation_cancelled(stage="async_retry")
        lease = None
        try:
            if throttle_key is not None:
                from ..remote_impl.provider_throttle import acquire_provider_request

                lease = await acquire_provider_request(throttle_key)
            result = await operation()
        except SchemaSanitizerCancelledError:
            if lease is not None:
                lease.release()
            raise
        except asyncio.CancelledError:
            if lease is not None:
                lease.release()
            raise
        except Exception as exc:
            if lease is not None:
                lease.failure(exc)
            retryable = should_retry(exc) if should_retry is not None else True
            if attempt >= bounded_retries or not retryable:
                raise
            await cancellable_async_sleep(retry_delay(attempt), stage="async_retry_backoff")
            continue
        except BaseException:
            if lease is not None:
                lease.release()
            raise

        # The user operation has already completed successfully.  Keep success
        # accounting outside the retry exception handler so an instrumentation
        # failure cannot repeat a non-idempotent operation.
        if lease is not None:
            lease.success()
        check_operation_cancelled(stage="async_retry")
        return result
    raise RuntimeError("unreachable async retry state")


async def _indexed_worker(
    indices: asyncio.Queue[int],
    results: asyncio.Queue[tuple[int, Any, BaseException | None]],
    fetch: Callable[[int], Awaitable[Any]],
) -> None:
    """Consume scheduled indices and publish one result per fetch."""
    while True:
        check_operation_cancelled(stage="async_worker")
        index = await indices.get()
        try:
            value = await fetch(index)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            # A worker must always publish a terminal outcome. Otherwise a
            # non-Exception failure (for example a custom BaseException) would
            # terminate the worker and leave the ordered consumer blocked.
            await results.put((index, None, exc))
        else:
            await results.put((index, value, None))
        finally:
            indices.task_done()


def _start_indexed_workers(
    worker_count: int,
    indices: asyncio.Queue[int],
    results: asyncio.Queue[tuple[int, Any, BaseException | None]],
    fetch: Callable[[int], Awaitable[Any]],
) -> list[asyncio.Task[None]]:
    """Start a fixed worker pool for indexed asynchronous work."""
    return [
        asyncio.create_task(_indexed_worker(indices, results, fetch)) for _ in range(worker_count)
    ]


async def _stop_workers(workers: list[asyncio.Task[None]]) -> None:
    """Cancel and drain worker tasks without leaking pending coroutines."""
    for worker in workers:
        worker.cancel()
    if workers:
        await asyncio.gather(*workers, return_exceptions=True)


async def ordered_indexed_results(
    count: int,
    fetch: Callable[[int], Awaitable[Any]],
    *,
    window: int,
) -> AsyncIterator[tuple[int, Any]]:
    """Yield indexed async results in input order with bounded prefetch."""
    if count <= 0:
        return
    worker_count = min(count, max(1, int(window)))
    indices: asyncio.Queue[int] = asyncio.Queue(maxsize=worker_count)
    results: asyncio.Queue[tuple[int, Any, BaseException | None]] = asyncio.Queue(
        maxsize=worker_count
    )
    for index in range(worker_count):
        indices.put_nowait(index)
    next_to_schedule = worker_count
    workers = _start_indexed_workers(worker_count, indices, results, fetch)
    pending: dict[int, tuple[Any, BaseException | None]] = {}

    try:
        for expected in range(count):
            check_operation_cancelled(stage="ordered_async_results")
            while expected not in pending:
                index, value, error = await results.get()
                pending[index] = (value, error)
            value, error = pending.pop(expected)
            if error is not None:
                raise error
            yield expected, value
            if next_to_schedule < count:
                indices.put_nowait(next_to_schedule)
                next_to_schedule += 1
    finally:
        await _stop_workers(workers)


async def unordered_indexed_results(
    count: int,
    fetch: Callable[[int], Awaitable[Any]],
    *,
    window: int,
) -> AsyncIterator[tuple[int, Any]]:
    """Yield indexed async results as they complete with a fixed worker pool."""
    if count <= 0:
        return
    worker_count = min(count, max(1, int(window)))
    indices: asyncio.Queue[int] = asyncio.Queue(maxsize=worker_count)
    results: asyncio.Queue[tuple[int, Any, BaseException | None]] = asyncio.Queue(
        maxsize=worker_count
    )
    for index in range(worker_count):
        indices.put_nowait(index)
    next_to_schedule = worker_count
    workers = _start_indexed_workers(worker_count, indices, results, fetch)

    try:
        for _ in range(count):
            check_operation_cancelled(stage="unordered_async_results")
            index, value, error = await results.get()
            if error is not None:
                raise error
            yield index, value
            if next_to_schedule < count:
                indices.put_nowait(next_to_schedule)
                next_to_schedule += 1
    finally:
        await _stop_workers(workers)


async def drain_ordered_indexed_results(
    count: int,
    fetch: Callable[[int], Awaitable[Any]],
    *,
    window: int,
) -> None:
    """Run indexed async work in order when callers only need side effects."""
    async for _index, _result in ordered_indexed_results(count, fetch, window=window):
        continue
