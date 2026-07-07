"""Shared async scheduling helpers for remote file staging."""

from __future__ import annotations

import asyncio
import os
import random
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any, TypeVar

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class DirectoryDownloadTuning:
    """Runtime controls for concurrent remote directory downloads."""

    concurrency: int
    window: int
    retries: int


def read_int_env(name: str, default: int) -> int:
    """Read a positive integer from the environment."""
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def read_float_env(name: str, default: float) -> float:
    """Read a positive float from the environment."""
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def retry_delay(attempt: int) -> float:
    """Return jittered exponential backoff delay for remote I/O retries."""
    return min(8.0, 0.25 * (2**attempt)) + random.uniform(0.0, 0.25)


async def retry_async(operation: Callable[[], Awaitable[T]], *, retries: int) -> T:
    """Run one async operation with retry/backoff for raised exceptions."""
    for attempt in range(retries + 1):
        try:
            return await operation()
        except Exception:
            if attempt >= retries:
                raise
            await asyncio.sleep(retry_delay(attempt))
    raise RuntimeError("unreachable async retry state")


def directory_download_tuning() -> DirectoryDownloadTuning:
    """Return remote directory concurrency, prefetch, and retry controls."""
    concurrency = read_int_env("SCHEMA_SANITIZER_ASYNC_CONCURRENCY", 64)
    window = max(
        concurrency, read_int_env("SCHEMA_SANITIZER_ASYNC_PREFETCH_FILES", concurrency * 2)
    )
    retries = read_int_env("SCHEMA_SANITIZER_ASYNC_RETRIES", 4)
    return DirectoryDownloadTuning(concurrency=concurrency, window=window, retries=retries)


async def ordered_indexed_results(
    count: int,
    fetch: Callable[[int], Awaitable[Any]],
    *,
    window: int,
) -> AsyncIterator[tuple[int, Any]]:
    """Yield indexed async results in input order while prefetching ahead."""
    tasks: dict[int, asyncio.Task[Any]] = {}
    next_to_schedule = 0

    def schedule() -> None:
        """Schedule tasks up to the configured prefetch window."""
        nonlocal next_to_schedule
        while next_to_schedule < count and len(tasks) < window:
            tasks[next_to_schedule] = asyncio.create_task(fetch(next_to_schedule))
            next_to_schedule += 1

    try:
        schedule()
        for expected in range(count):
            task = tasks.pop(expected)
            result = await task
            yield expected, result
            schedule()
    finally:
        for task in tasks.values():
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks.values(), return_exceptions=True)


async def unordered_indexed_results(
    count: int,
    fetch: Callable[[int], Awaitable[Any]],
    *,
    window: int,
) -> AsyncIterator[tuple[int, Any]]:
    """Yield indexed async results as they complete while prefetching ahead."""
    tasks: dict[asyncio.Task[Any], int] = {}
    next_to_schedule = 0

    def schedule() -> None:
        """Schedule tasks up to the configured prefetch window."""
        nonlocal next_to_schedule
        while next_to_schedule < count and len(tasks) < window:
            task = asyncio.create_task(fetch(next_to_schedule))
            tasks[task] = next_to_schedule
            next_to_schedule += 1

    try:
        schedule()
        while tasks:
            done, _pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                index = tasks.pop(task)
                result = await task
                yield index, result
                schedule()
    finally:
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


async def drain_ordered_indexed_results(
    count: int,
    fetch: Callable[[int], Awaitable[Any]],
    *,
    window: int,
) -> None:
    """Run indexed async work in order when callers only need side effects."""
    async for _index, _result in ordered_indexed_results(count, fetch, window=window):
        continue
