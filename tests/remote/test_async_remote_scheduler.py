"""Tests for async remote staging scheduler helpers."""

from __future__ import annotations

import asyncio
import time
from typing import AbstractSet

import pytest

from schema_sanitizer.core_impl import async_scheduler
from schema_sanitizer.core_impl.async_scheduler import (
    drain_ordered_iterable_results,
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
            async for _index, _payload in ordered_indexed_results(
                3, fetch, window=3, expected_retained_bytes=1024
            ):
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
        async for index, value in unordered_indexed_results(
            5, fetch, window=2, expected_retained_bytes=256
        ):
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
            async for _index, _value in unordered_indexed_results(
                3, fetch, window=2, expected_retained_bytes=256
            ):
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

        values = [
            value
            async for _index, value in unordered_indexed_results(
                100, fetch, window=4, expected_retained_bytes=256
            )
        ]
        assert sorted(values) == list(range(100))
        assert created == 4

    asyncio.run(run())


def test_completed_worker_pool_stops_without_terminal_debt_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Idle result-slot waits remain directly cancellable on Python 3.11."""

    original_park = async_scheduler._park_async_terminal_debt
    park_calls = 0
    parked_tasks: list[asyncio.Task[None]] = []

    def record_terminal_debt(
        tasks: AbstractSet[asyncio.Task[None]],
        admission: async_scheduler._AsyncSchedulerAdmission,
        result_slots: list[async_scheduler._AsyncWorkerResultSlot] | None,
        pending_slots: list[async_scheduler._AsyncPendingResultSlot] | None = None,
        *,
        reap_completed: bool = True,
    ) -> bool:
        """Record a regression, then preserve ownership and make cleanup finite."""
        nonlocal park_calls
        park_calls += 1
        parked_tasks.extend(tasks)
        # A second cancellation lets an implementation that regresses clean up
        # its terminal debt instead of contaminating the rest of the test run.
        for task in tasks:
            task.cancel()
        return original_park(
            tasks,
            admission,
            result_slots,
            pending_slots,
            reap_completed=reap_completed,
        )

    monkeypatch.setattr(async_scheduler, "_ASYNC_CANCEL_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(async_scheduler, "_park_async_terminal_debt", record_terminal_debt)

    async def run() -> tuple[
        float,
        async_scheduler.AsyncSchedulerSnapshot,
        async_scheduler.AsyncSchedulerSnapshot,
    ]:
        """Complete a pool and capture exact scheduler ownership around shutdown."""
        before = async_scheduler.async_scheduler_snapshot()

        async def fetch(index: int) -> int:
            """Produce enough values to recycle every fixed worker repeatedly."""
            await asyncio.sleep(0)
            return index

        started = time.monotonic()
        values = [
            value
            async for _index, value in unordered_indexed_results(
                100, fetch, window=4, expected_retained_bytes=256
            )
        ]
        elapsed = time.monotonic() - started
        assert sorted(values) == list(range(100))
        if parked_tasks:
            await asyncio.gather(*parked_tasks, return_exceptions=True)
        return elapsed, before, async_scheduler.async_scheduler_snapshot()

    elapsed, before, after = asyncio.run(run())
    assert park_calls == 0
    assert elapsed < 1.0
    assert after.in_use == before.in_use
    assert after.active_operations == before.active_operations
    assert after.terminal_debts == before.terminal_debts


def test_bounded_event_wait_propagates_external_task_cancellation() -> None:
    """An outer cancellation is never converted into a local polling timeout."""

    async def run() -> float:
        """Cancel an Event poll while its structured timeout is still armed."""
        waiting = asyncio.create_task(
            async_scheduler._bounded_async_event_wait(
                asyncio.Event(), stage="async_result_slot_test"
            )
        )
        await asyncio.sleep(0)
        started = time.monotonic()
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        return time.monotonic() - started

    assert asyncio.run(run()) < 0.5


def test_retry_async_stops_on_non_retryable_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """Permanent remote failures are not retried or delayed."""

    async def run() -> None:
        """Run a permanent-error retry scenario."""
        attempts = 0
        sleeps: list[float] = []

        async def operation() -> bytes:
            """Raise one non-retryable permission error."""
            nonlocal attempts
            attempts += 1
            raise PermissionError("forbidden")

        async def fake_sleep(delay: float) -> None:
            """Record unexpected backoff calls."""
            sleeps.append(delay)

        monkeypatch.setattr(asyncio, "sleep", fake_sleep)
        with pytest.raises(PermissionError, match="forbidden"):
            await retry_async(
                operation,
                retries=5,
                should_retry=lambda exc: not isinstance(exc, PermissionError),
            )
        assert attempts == 1
        assert sleeps == []

    asyncio.run(run())


def test_retry_async_exhausts_selected_transient_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retryable failures stop after the configured number of retries."""

    async def run() -> None:
        """Run an exhausted transient-error retry scenario."""
        attempts = 0
        sleeps: list[float] = []

        async def operation() -> bytes:
            """Raise one retryable transient error."""
            nonlocal attempts
            attempts += 1
            raise RuntimeError("transient")

        async def fake_sleep(delay: float, *, stage: str) -> None:
            """Record each configured cancellable backoff call."""
            assert stage == "async_retry_backoff"
            sleeps.append(delay)

        monkeypatch.setattr(
            "schema_sanitizer.core_impl.async_scheduler.cancellable_async_sleep",
            fake_sleep,
        )
        with pytest.raises(RuntimeError, match="transient"):
            await retry_async(
                operation,
                retries=2,
                should_retry=lambda exc: isinstance(exc, RuntimeError),
            )
        assert attempts == 3
        assert len(sleeps) == 2

    asyncio.run(run())


def test_ordered_indexed_results_reports_earliest_ordinal_failure() -> None:
    """A fast later failure must wait for the earlier failing ordinal."""

    async def run() -> None:
        """Force failures to complete in reverse source order."""
        later_failed = asyncio.Event()

        async def fetch(index: int) -> int:
            """Fail index one first, then release index zero."""
            if index == 1:
                later_failed.set()
                raise RuntimeError("later")
            await later_failed.wait()
            raise ValueError("earlier")

        with pytest.raises(ValueError, match="earlier"):
            async for _index, _value in ordered_indexed_results(
                2, fetch, window=2, expected_retained_bytes=256
            ):
                raise AssertionError("failing work must not yield")

    asyncio.run(run())


def test_drain_ordered_iterable_results_materializes_only_one_window() -> None:
    """Large iterables retain only O(window) unconsumed references."""

    async def run() -> None:
        produced = 0
        consumed = 0
        max_ahead = 0

        def values():
            nonlocal produced, max_ahead
            for value in range(100):
                produced += 1
                max_ahead = max(max_ahead, produced - consumed)
                yield value

        async def fetch(value: int) -> None:
            nonlocal consumed
            await asyncio.sleep(0)
            consumed += 1

        await drain_ordered_iterable_results(values(), fetch, window=4)
        assert consumed == 100
        assert max_ahead <= 4

    asyncio.run(run())
