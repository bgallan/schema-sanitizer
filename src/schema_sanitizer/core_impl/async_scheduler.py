"""Generic bounded async scheduling and retry helpers."""

from __future__ import annotations

import asyncio
import random
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, TypeVar

T = TypeVar("T")

_MAX_ASYNC_WORKERS = 512
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
) -> T:
    """Run one async operation with bounded retry/backoff."""
    bounded_retries = min(max(int(retries), 0), _MAX_ASYNC_RETRIES)
    for attempt in range(bounded_retries + 1):
        try:
            return await operation()
        except Exception as exc:
            retryable = should_retry(exc) if should_retry is not None else True
            if attempt >= bounded_retries or not retryable:
                raise
            await asyncio.sleep(retry_delay(attempt))
    raise RuntimeError("unreachable async retry state")


async def _indexed_worker(
    indices: asyncio.Queue[int],
    results: asyncio.Queue[tuple[int, Any, BaseException | None]],
    fetch: Callable[[int], Awaitable[Any]],
) -> None:
    """Consume scheduled indices and publish one result per fetch."""
    while True:
        index = await indices.get()
        try:
            value = await fetch(index)
        except Exception as exc:
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
    worker_count = min(count, max(1, int(window)), _MAX_ASYNC_WORKERS)
    indices: asyncio.Queue[int] = asyncio.Queue(maxsize=worker_count)
    results: asyncio.Queue[tuple[int, Any, BaseException | None]] = asyncio.Queue(
        maxsize=worker_count
    )
    for index in range(worker_count):
        indices.put_nowait(index)
    next_to_schedule = worker_count
    workers = _start_indexed_workers(worker_count, indices, results, fetch)
    pending: dict[int, Any] = {}

    try:
        for expected in range(count):
            while expected not in pending:
                index, value, error = await results.get()
                if error is not None:
                    raise error
                pending[index] = value
            yield expected, pending.pop(expected)
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
    worker_count = min(count, max(1, int(window)), _MAX_ASYNC_WORKERS)
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
