"""Regression coverage for concurrency process governors and pressure."""

from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace

import pytest

from schema_sanitizer.api_impl.source_plan.remote import RemoteChunkPrefetchIterator
from schema_sanitizer.core_impl import memory_budget as memory_budget_module
from schema_sanitizer.core_impl.execution_policy import execution_policy
from schema_sanitizer.core_impl.memory_budget import (
    OperationMemoryLedger,
    ProcessResidentMemorySnapshot,
    adaptive_concurrency_target,
    process_resident_memory_snapshot,
)
from schema_sanitizer.pipeline.partition_lookahead import ThreadPoolExecutor
from schema_sanitizer.remote_impl.io_coordinator import RemoteIoCoordinator
from schema_sanitizer.remote_impl.io_permits import RemoteIoPermitGovernor


def test_remote_permits_bound_unrelated_coordinator_loops(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two operation event loops cannot exceed one shared weighted ceiling."""
    governor = RemoteIoPermitGovernor(2)
    first_started = threading.Event()
    second_started = threading.Event()
    release_first = threading.Event()
    small_queued = threading.Event()
    original_enqueue = governor._enqueue_waiter_locked  # noqa: SLF001

    def observe_enqueue(waiter: object) -> None:
        """Publish when the blocked small request is authoritatively queued."""
        original_enqueue(waiter)  # type: ignore[arg-type]
        if getattr(waiter, "label", None) == "small":
            small_queued.set()

    monkeypatch.setattr(governor, "_enqueue_waiter_locked", observe_enqueue)
    first = RemoteIoCoordinator(permit_governor=governor)
    second = RemoteIoCoordinator(permit_governor=governor)

    async def large(_context: object) -> str:
        """Occupy the complete weighted process capacity."""
        first_started.set()
        while not release_first.is_set():
            await asyncio.sleep(0.005)
        return "large"

    async def small(_context: object) -> str:
        """Record admission after the large request releases capacity."""
        second_started.set()
        return "small"

    large_future = first.submit(large, permit_weight=2, permit_label="large")
    assert first_started.wait(timeout=1)
    small_future = second.submit(small, permit_weight=1, permit_label="small")
    assert small_queued.wait(timeout=1)
    assert not second_started.is_set()
    release_first.set()
    assert large_future.result(timeout=1) == "large"
    assert small_future.result(timeout=1) == "small"
    first.close()
    second.close()
    snapshot = governor.snapshot()
    assert snapshot.in_use == 0
    assert snapshot.peak_in_use == 2
    assert snapshot.grants == 2


def test_remote_permit_bypass_is_latency_friendly_but_bounded() -> None:
    """Tiny requests may bypass a blocked large request only a finite number of times."""

    async def exercise() -> tuple[int, bool]:
        """Drive four allowed bypasses, then prove the fifth waits."""
        governor = RemoteIoPermitGovernor(4)
        holder = await governor.acquire(3, label="holder")
        large_task = asyncio.create_task(governor.acquire(4, label="large"))
        await asyncio.sleep(0)
        for index in range(4):
            tiny = await asyncio.wait_for(governor.acquire(1, label=f"tiny-{index}"), timeout=0.2)
            tiny.release()
        fifth = asyncio.create_task(governor.acquire(1, label="tiny-5"))
        await asyncio.sleep(0)
        assert governor.snapshot().waiting == 2
        fifth_waited = not fifth.done()
        holder.release()
        large = await asyncio.wait_for(large_task, timeout=0.2)
        assert not fifth.done()
        large.release()
        tiny = await asyncio.wait_for(fifth, timeout=0.2)
        tiny.release()
        return governor.snapshot().bounded_bypasses, fifth_waited

    bypasses, fifth_waited = asyncio.run(exercise())
    assert bypasses == 4
    assert fifth_waited


def test_cancelled_remote_waiter_returns_queue_capacity() -> None:
    """Cancelling a queued weighted request cannot leave phantom admission."""

    async def exercise() -> tuple[int, int]:
        """Cancel one waiter while all capacity is occupied."""
        governor = RemoteIoPermitGovernor(1)
        holder = await governor.acquire(1, label="holder")
        waiter = asyncio.create_task(governor.acquire(1, label="cancelled"))
        await asyncio.sleep(0)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        holder.release()
        await asyncio.sleep(0)
        snapshot = governor.snapshot()
        return snapshot.in_use, snapshot.cancellations

    in_use, cancellations = asyncio.run(exercise())
    assert in_use == 0
    assert cancellations == 1


def test_live_concurrency_target_keeps_untracked_safety_reserve(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Dynamic windows shrink before the exact resident ledger is exhausted."""
    monkeypatch.setattr(
        memory_budget_module,
        "process_resident_memory_snapshot",
        lambda: ProcessResidentMemorySnapshot(1000, 700, 800),
    )
    assert adaptive_concurrency_target(8, per_slot_bytes=100, reserve_bytes=100) == 2
    assert adaptive_concurrency_target(8, per_slot_bytes=100, reserve_bytes=300) == 1


def test_native_execution_policy_shrinks_under_process_pressure() -> None:
    """Native worker and prefetch limits react to other live operations."""
    baseline_policy = execution_policy("multi", 256 << 20)
    if baseline_policy.effective_workers <= 1:
        pytest.skip("host exposes no multi-worker baseline")
    baseline = process_resident_memory_snapshot()
    available = baseline.capacity_bytes - baseline.reserved_bytes
    if available <= (2 << 20):
        pytest.skip("process resident pool has no pressure-test headroom")
    blocker = OperationMemoryLedger(baseline.capacity_bytes)
    lease = blocker.acquire(available - (1 << 20), stage="adaptive_worker_pressure")
    try:
        pressured = execution_policy("multi", 256 << 20)
        assert pressured.effective_workers == 1
        assert pressured.task_queue_capacity == 1
        assert pressured.remote_chunk_prefetch == 1
        assert pressured.async_concurrency == 1
        assert pressured.fallback_to_one_worker_reason == "memory_limited"
    finally:
        lease.release()
        blocker.close()


def test_memory_ledger_reports_over_release_instead_of_only_clamping() -> None:
    """Cleanup bugs remain visible after safe saturation at zero."""
    ledger = OperationMemoryLedger(8 << 20)
    ledger.reserve(4096, stage="diagnostic_release")
    ledger.release(6144)
    assert ledger.snapshot().reserved_bytes == 0
    diagnostics = ledger.diagnostics()
    assert diagnostics.over_release_count == 1
    assert diagnostics.over_release_bytes == 2048
    ledger.close()


def test_memory_ledger_records_bytes_still_live_when_owner_closes() -> None:
    """Premature owner shutdown is observable even if a late lease later drains."""
    ledger = OperationMemoryLedger(8 << 20)
    lease = ledger.acquire(4096, stage="close_outstanding")
    ledger.close()
    assert ledger.diagnostics().close_outstanding_bytes == 4096
    lease.release()
    assert ledger.snapshot().reserved_bytes == 0


def test_daemon_lookahead_executor_has_bounded_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cancellation-resistant lookahead task cannot block process shutdown."""
    started = threading.Event()
    release = threading.Event()
    executor = ThreadPoolExecutor(
        max_workers=1,
        thread_name_prefix="schema-sanitizer-partition-lookahead-test",
    )

    def stubborn() -> str:
        """Ignore executor cancellation until explicitly released."""
        started.set()
        release.wait()
        return "done"

    future = executor.submit(stubborn)
    assert started.wait(timeout=1)
    real_join = executor._thread.join  # noqa: SLF001
    join_timeouts: list[float | None] = []

    def observe_join(timeout: float | None = None) -> None:
        """Reject any blocking join from the explicit non-waiting shutdown path."""
        join_timeouts.append(timeout)
        assert timeout == 0.0
        real_join(timeout=timeout)

    monkeypatch.setattr(executor._thread, "join", observe_join)  # noqa: SLF001
    try:
        executor.shutdown(wait=False, cancel_futures=True)
        assert join_timeouts == [0.0]
        assert executor._thread.daemon  # noqa: SLF001
        assert executor._thread.is_alive()  # noqa: SLF001
    finally:
        release.set()
        assert future.result(timeout=1) == "done"
        real_join(timeout=1)
    assert not executor._thread.is_alive()  # noqa: SLF001


def test_remote_chunk_weight_scales_with_estimated_bytes() -> None:
    """Large staged chunks consume proportionally more global I/O permits."""
    manifest = SimpleNamespace(
        threading_mode="multi",
        memory_limit_bytes=256 << 20,
        estimated_chunk_bytes=lambda _start: 128 << 20,
    )
    iterator = RemoteChunkPrefetchIterator(manifest)
    assert 1 < iterator._stage_permit_weight(0) <= iterator._policy.async_concurrency  # noqa: SLF001


def test_remote_capacity_registration_shrinks_after_coordinator_close() -> None:
    """A completed high-concurrency operation cannot relax later admission."""
    governor = RemoteIoPermitGovernor(1)
    high = RemoteIoCoordinator(permit_governor=governor, permit_capacity=8)
    low = RemoteIoCoordinator(permit_governor=governor, permit_capacity=2)
    assert governor.snapshot().capacity == 8
    assert governor.snapshot().active_capacity_registrations == 2
    high.close()
    assert governor.snapshot().capacity == 2
    low.close()
    snapshot = governor.snapshot()
    assert snapshot.capacity == 1
    assert snapshot.active_capacity_registrations == 0


def test_transfer_helper_forwards_chunk_weight(monkeypatch: pytest.MonkeyPatch) -> None:
    """Top-level transfer fan-out is represented in global permit admission."""
    from schema_sanitizer.api_impl import operation_context as context_module
    from schema_sanitizer.api_impl.operation_context import OperationExecutionContext

    observed: dict[str, object] = {}
    fake = SimpleNamespace(
        memory_limit_bytes=123,
        policy=SimpleNamespace(async_concurrency=4),
    )

    def run_remote(operation, *, permit_label: str, footprint) -> str:
        """Capture the atomic footprint without starting an event loop."""
        observed.update(footprint=footprint, label=permit_label, operation=operation)
        return "ok"

    fake.run_remote = run_remote
    monkeypatch.setattr(
        context_module,
        "memory_budget",
        lambda _limit: SimpleNamespace(io_chunk_bytes=10),
    )

    def operation():
        """Represent the transfer coroutine factory passed through unchanged."""

    result = OperationExecutionContext.run_remote_transfer(
        fake,
        operation,
        estimated_bytes=35,
        permit_label="weighted-transfer",
    )
    assert result == "ok"
    assert observed["label"] == "weighted-transfer"
    assert observed["operation"] is operation
    footprint = observed["footprint"]
    assert footprint.remote_weight == 4
    assert footprint.network_fds == 0
    assert footprint.local_file_fds == 0


def test_process_memory_pressure_reports_untracked_rss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Opaque interpreter and SDK RSS remains observable beside exact charges."""
    monkeypatch.setattr(
        memory_budget_module,
        "process_resident_memory_snapshot",
        lambda: ProcessResidentMemorySnapshot(10_000, 2_000, 3_000),
    )
    monkeypatch.setattr(memory_budget_module, "_read_process_rss_bytes", lambda: 7_500)
    snapshot = memory_budget_module.process_memory_pressure_snapshot()
    assert snapshot.exact_headroom_bytes == 8_000
    assert snapshot.rss_bytes == 7_500
    assert snapshot.untracked_rss_bytes == 5_500


def test_temporary_storage_reports_close_and_over_release(tmp_path) -> None:
    """Disk cleanup underflow and live-at-close state are explicit diagnostics."""
    from schema_sanitizer.core_impl.temporary_storage import TemporaryStoragePermitPool

    pool = TemporaryStoragePermitPool(64 << 20)
    lease = pool.acquire(4096, label="diagnostic-spool", path=tmp_path)
    pool.close()
    diagnostics = pool.diagnostics()
    assert diagnostics.close_outstanding_bytes == 4096
    assert diagnostics.close_active_leases == 1
    filesystem_key = lease._filesystem_key  # noqa: SLF001
    lease.release()
    pool._release(1024, filesystem_key=filesystem_key)  # noqa: SLF001
    diagnostics = pool.diagnostics()
    assert diagnostics.over_release_count == 1
    assert diagnostics.over_release_bytes == 1024


def test_remote_governor_reports_weight_underflow() -> None:
    """Permit cleanup underflow is visible rather than silently clamped."""
    governor = RemoteIoPermitGovernor(2)
    governor._release(3)  # noqa: SLF001
    snapshot = governor.snapshot()
    assert snapshot.in_use == 0
    assert snapshot.over_release_count == 1
    assert snapshot.over_release_weight == 3
