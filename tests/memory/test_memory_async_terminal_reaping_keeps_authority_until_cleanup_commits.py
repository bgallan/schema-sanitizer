"""Regression coverage for memory async terminal reaping keeps authority until cleanup commits."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src" / "schema_sanitizer"
CPP = ROOT / "cpp" / "src"


def _source(relative: str) -> str:
    return (SRC / relative).read_text(encoding="utf-8")


def test_async_terminal_reaping_keeps_authority_until_cleanup_commits() -> None:
    from schema_sanitizer.core_impl import async_scheduler as scheduler

    scheduler._reap_async_terminal_debts()

    class DoneTask:
        def done(self) -> bool:
            return True

    class FailOnce:
        def __init__(self) -> None:
            self.calls = 0

        def close(self) -> None:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("injected close failure")

    owner = FailOnce()
    admission = scheduler._AsyncSchedulerAdmission(1, stage_admission=owner)
    task = DoneTask()
    assert scheduler._park_async_terminal_debt({task}, admission, None)  # type: ignore[arg-type]
    before = scheduler._ASYNC_TERMINAL_DEBT_COUNT
    with pytest.raises(RuntimeError, match="injected close failure"):
        scheduler._reap_one_async_terminal_debt()
    assert scheduler._ASYNC_TERMINAL_DEBT_COUNT == before
    assert any(
        debt.state == scheduler._ASYNC_DEBT_REAPING for debt in scheduler._ASYNC_TERMINAL_DEBTS
    )
    assert admission.stage_admission is owner

    assert scheduler._reap_one_async_terminal_debt()
    assert scheduler._ASYNC_TERMINAL_DEBT_COUNT == before - 1
    assert admission.stage_admission is None


def test_cleanup_only_failure_is_parked_without_live_tasks() -> None:
    from schema_sanitizer.core_impl import async_scheduler as scheduler

    scheduler._reap_async_terminal_debts()

    class FailOnce:
        def __init__(self) -> None:
            self.calls = 0

        def close(self) -> None:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("cleanup-only failure")

    owner = FailOnce()
    admission = scheduler._AsyncSchedulerAdmission(1, stage_admission=owner)
    before = scheduler._ASYNC_TERMINAL_DEBT_COUNT
    with pytest.raises(RuntimeError, match="cleanup-only failure"):
        scheduler._release_or_park_async_terminal_ownership(admission, None)
    assert scheduler._ASYNC_TERMINAL_DEBT_COUNT == before + 1
    assert scheduler._reap_one_async_terminal_debt()
    assert scheduler._ASYNC_TERMINAL_DEBT_COUNT == before
    assert admission.stage_admission is None


def test_async_result_slot_retains_failed_lease_for_retry() -> None:
    from schema_sanitizer.core_impl import async_scheduler as scheduler

    class FailOnceLease:
        def __init__(self) -> None:
            self.calls = 0

        def close(self) -> None:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("lease close failed")

    slot = scheduler._AsyncWorkerResultSlot(asyncio.Event())
    lease = FailOnceLease()
    slot.publish(7, b"payload", None, lease)
    with pytest.raises(RuntimeError, match="lease close failed"):
        slot.terminal_release()
    assert slot.state == scheduler._ASYNC_RESULT_READY
    assert slot.retained_lease is lease
    assert slot.terminal_release()
    assert slot.state == scheduler._ASYNC_RESULT_EMPTY
    assert slot.retained_lease is None


def test_unknown_async_results_degrade_to_single_materializer() -> None:
    from schema_sanitizer.core_impl.async_scheduler import ordered_indexed_results

    class UnknownPayload:
        pass

    async def run() -> int:
        active = 0
        peak = 0

        async def fetch(_index: int) -> UnknownPayload:
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.001)
            active -= 1
            return UnknownPayload()

        async for _index, _value in ordered_indexed_results(12, fetch, window=8):
            pass
        return peak

    assert asyncio.run(run()) == 1


def test_async_hard_bound_never_uses_sampled_tail_extrapolation() -> None:
    from schema_sanitizer.core_impl.async_scheduler import (
        _estimate_async_result_bytes,
        _known_async_result_upper_bound,
    )

    payload = [b"x" for _ in range(64)] + [b"z" * (8 << 20)]
    assert _known_async_result_upper_bound(payload) is None
    # The old bounded sampler remains diagnostic only and may be far smaller.
    assert _estimate_async_result_bytes(payload) < len(payload[-1])
    assert _known_async_result_upper_bound(object()) is None


def test_async_terminal_publication_uses_preallocated_slots_not_result_queue_put() -> None:
    source = _source("core_impl/async_scheduler.py")
    worker = source[
        source.index("async def _indexed_worker") : source.index("def _start_indexed_workers")
    ]
    assert "result_slot.publish(" in worker
    assert "results.put(" not in worker
    assert "_ASYNC_TERMINAL_TASK_BANK" in source
    assert "_ASYNC_DEBT_REAPING" in source
    assert "async_terminal_ownership_banks" in source


def test_cross_process_finalizer_recycles_generation_repeatedly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl.bounded_generation import BoundedGenerationPool
    from schema_sanitizer.core_impl.cross_process_memory import _ProcessCrossMemoryCoordinator

    coordinator = _ProcessCrossMemoryCoordinator(16 << 20)
    coordinator._generation_pool = BoundedGenerationPool(1)
    # Exceed the production generation-bank size to prove GC/finalizer-style
    # recycling cannot exhaust generations after long-running churn.
    for _ in range(5000):
        reservation = coordinator.acquire(0)
        reservation._release_nonblocking()
        coordinator.reconcile_pending()
        assert not coordinator._contributions


def test_cross_process_acquire_drains_published_release_before_capacity_reject(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl.bounded_generation import BoundedGenerationPool
    from schema_sanitizer.core_impl.cross_process_memory import _ProcessCrossMemoryCoordinator

    coordinator = _ProcessCrossMemoryCoordinator(16 << 20)
    coordinator._generation_pool = BoundedGenerationPool(1)
    first = coordinator.acquire(0)
    first._release_nonblocking()  # published, deliberately not reconciled
    second = coordinator.acquire(0)  # must drain then reuse the sole generation
    assert second.reserved_bytes == 0
    second.close()


def test_cross_process_auth_mismatch_stays_published_until_retry() -> None:
    from schema_sanitizer.core_impl.cross_process_memory import _ProcessCrossMemoryCoordinator

    coordinator = _ProcessCrossMemoryCoordinator(16 << 20)
    reservation = coordinator.acquire(0)
    original = reservation._capability
    reservation._capability = object()
    reservation._release_nonblocking()
    coordinator.reconcile_pending()
    assert coordinator._contributions
    assert coordinator._finalizer_releases.published_count() == 1

    reservation._capability = original
    coordinator.reconcile_pending()
    assert not coordinator._contributions
    assert coordinator._finalizer_releases.published_count() == 0


def test_cgroup_mount_join_rejects_unrelated_subtree() -> None:
    from schema_sanitizer.core_impl.cgroup_view import _join_mount_path

    assert _join_mount_path("/sys/fs/cgroup", "/kubepods/a", "/kubepods/b/pod") is None
    nested = _join_mount_path("/sys/fs/cgroup", "/kubepods/a", "/kubepods/a/pod")
    root_relative = _join_mount_path("/sys/fs/cgroup", "/", "/user.slice/a")
    assert nested is not None
    assert root_relative is not None
    assert nested.as_posix() == "/sys/fs/cgroup/pod"
    assert root_relative.as_posix() == "/sys/fs/cgroup/user.slice/a"


def test_cgroup_resolution_fails_closed_after_repeated_membership_migration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import cgroup_view

    a = ("/a", {})
    b = ("/b", {})
    c = ("/c", {})
    sequence = iter((b, c))
    monkeypatch.setattr(cgroup_view, "_read_current_membership", lambda: next(sequence))
    monkeypatch.setattr(
        cgroup_view,
        "_resolve_linux_cgroup_view_once",
        lambda _membership: cgroup_view.CgroupView(
            2, Path("/x"), Path("/sys/fs/cgroup"), resolution_known=True
        ),
    )
    view = cgroup_view._resolve_linux_cgroup_view(a)
    assert view.version == 0
    assert not view.resolution_known


def test_pressure_events_aggregate_all_ancestors(monkeypatch: pytest.MonkeyPatch) -> None:
    from schema_sanitizer.core_impl import system_pressure

    monkeypatch.setattr(
        system_pressure,
        "read_cgroup_hierarchy_texts",
        lambda *_args, **_kwargs: (
            "high 1\noom 0\noom_kill 0\n",
            "high 7\noom 2\noom_kill 3\n",
        ),
    )
    assert system_pressure._cgroup_events() == (8, 5)


def test_prebounded_async_results_still_run_concurrently_without_terminal_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import async_scheduler as scheduler

    monkeypatch.setattr(
        scheduler,
        "_acquire_async_scheduler_admission",
        lambda requested: scheduler._AsyncSchedulerAdmission(requested),
    )
    monkeypatch.setattr(scheduler, "_borrow_idle_async_capacity", lambda *_args: None)
    monkeypatch.setattr(scheduler, "_record_async_admission_shortfall", lambda *_args: None)
    # Avoid native memory-ledger dependency in this environment while retaining
    # the preflight concurrency contract itself.
    original_fetch_with_admission = scheduler.__dict__["_fetch_with_result_admission"]

    async def fake_fetch_with_admission(index, fetch, retained, expected):
        return await fetch(index), None

    monkeypatch.setitem(
        scheduler.__dict__, "_fetch_with_result_admission", fake_fetch_with_admission
    )

    async def run() -> int:
        active = 0
        peak = 0

        async def fetch(index: int) -> bytes:
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            try:
                await asyncio.sleep(0.005)
                return str(index).encode()
            finally:
                active -= 1

        values = [
            value
            async for _index, value in scheduler.unordered_indexed_results(
                12, fetch, window=4, expected_retained_bytes=64
            )
        ]
        assert {int(value) for value in values} == set(range(12))
        return peak

    try:
        assert asyncio.run(run()) == 4
    finally:
        scheduler.__dict__["_fetch_with_result_admission"] = original_fetch_with_admission


def test_native_cgroup_and_backpressure_contracts_are_hardened() -> None:
    cgroup = (CPP / "internal/runtime/cgroup_view.hh").read_text(encoding="utf-8")
    arena = (CPP / "internal/runtime/operation_task_arena.cc").read_text(encoding="utf-8")
    header = (CPP / "internal/runtime/operation_task_arena.hh").read_text(encoding="utf-8")
    ordered = (CPP / "internal/runtime/ordered_executor.hh").read_text(encoding="utf-8")

    assert "Never fabricate mountpoint + unrelated membership" in cgroup
    assert "membership_after" in cgroup
    assert "cancel_requested" in arena
    assert "backpressure_deadline_ns" in arena
    assert "retained_ready.wait_until" in arena
    assert "RequestCancellation() noexcept" in header
    assert "SetBackpressureDeadlineMillis" in header
    # An OrderedExecutor is only one stage using the operation-wide arena.
    # Stage cancellation must remain local so queued closures can retire their
    # leases and unrelated stages can continue using the shared arena.
    assert "arena_->RequestCancellation();" not in ordered
    assert "arena_shared_->stop_source.request_stop();" in ordered
