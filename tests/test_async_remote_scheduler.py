"""Tests for async remote staging scheduler helpers."""

from __future__ import annotations

import asyncio

import pytest

from schema_sanitizer.core_impl.async_scheduler import (
    ordered_indexed_results,
    retry_async,
    unordered_indexed_results,
)


def test_retry_async_retries_raised_operations() -> None:
    """Verify retry_async retries raised operations and returns the first success."""

    async def run() -> None:
        """Run the async retry scenario."""
        attempts = 0

        async def operation() -> bytes:
            """Fail once, then succeed."""
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("temporary")
            return b"ok"

        assert await retry_async(operation, retries=2) == b"ok"
        assert attempts == 2

    asyncio.run(run())


def test_ordered_indexed_results_cancels_prefetched_tasks_on_failure() -> None:
    """Verify failed ordered work drains cancellation for prefetched tasks."""

    async def run() -> None:
        """Run the async scheduler failure scenario."""
        cancelled: list[int] = []

        async def fetch(index: int) -> bytes:
            """Fail the first task while later prefetched tasks wait."""
            if index == 0:
                await asyncio.sleep(0)
                raise RuntimeError("boom")
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                cancelled.append(index)
                raise
            return str(index).encode()

        with pytest.raises(RuntimeError, match="boom"):
            async for _index, _payload in ordered_indexed_results(3, fetch, window=3):
                raise AssertionError("failed first task should not yield")

        assert cancelled == [1, 2]

    asyncio.run(run())


def test_unordered_indexed_results_uses_bounded_task_window() -> None:
    """Verify unordered results cap active tasks without waiting for input order."""

    async def run() -> None:
        """Run the unordered scheduler bounded-window scenario."""
        active = 0
        max_active = 0
        release_zero = asyncio.Event()
        zero_started = asyncio.Event()

        async def fetch(index: int) -> int:
            """Hold index 0 so index 1 can complete first deterministically."""
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            try:
                if index == 0:
                    zero_started.set()
                    await release_zero.wait()
                    return index
                if index == 1:
                    await zero_started.wait()
                    return index
                return index
            finally:
                active -= 1

        results: list[tuple[int, int]] = []
        async for index, value in unordered_indexed_results(5, fetch, window=2):
            results.append((index, value))
            if index == 1:
                release_zero.set()

        assert max_active == 2
        assert sorted(results) == [(index, index) for index in range(5)]
        assert results[0][0] == 1

    asyncio.run(run())


def test_unordered_indexed_results_cancels_prefetched_tasks_on_failure() -> None:
    """Verify unordered failed work drains cancellation for pending prefetched tasks."""

    async def run() -> None:
        """Run the unordered scheduler failure scenario."""
        cancelled: list[int] = []

        async def fetch(index: int) -> int:
            """Fail one task while another waits."""
            if index == 0:
                await asyncio.sleep(0)
                raise RuntimeError("boom")
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                cancelled.append(index)
                raise
            return index

        with pytest.raises(RuntimeError, match="boom"):
            async for _index, _value in unordered_indexed_results(3, fetch, window=2):
                raise AssertionError("failed task should not yield")

        assert cancelled == [1]

    asyncio.run(run())


def test_unordered_indexed_results_reuses_fixed_worker_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify large batches create only the configured worker count."""

    async def run() -> None:
        """Run a large trivial batch while counting task construction."""
        created = 0
        original_create_task = asyncio.create_task

        def counted_create_task(coro: object) -> asyncio.Task[object]:
            """Count scheduler-owned task construction."""
            nonlocal created
            created += 1
            return original_create_task(coro)  # type: ignore[arg-type]

        monkeypatch.setattr(asyncio, "create_task", counted_create_task)

        async def fetch(index: int) -> int:
            """Return one result after yielding control once."""
            await asyncio.sleep(0)
            return index

        values = [value async for _index, value in unordered_indexed_results(100, fetch, window=4)]
        assert sorted(values) == list(range(100))
        assert created == 4

    asyncio.run(run())
