"""Tests the process-wide availability notifier together with fork callbacks, bounded
native registries, scheduler fallback, executor shutdown, cleanup dispatch, and stage
admission. The queued map is authoritative, control-budget work stays outside dispatcher
locks, and emergency roots and remote scans remain allocation-bounded."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _source(relative: str) -> str:
    """Return the production source text inspected by this module."""
    return (ROOT / "src/schema_sanitizer" / relative).read_text(encoding="utf-8")


def test_availability_notifier_lifecycle_uses_authoritative_queued_map() -> None:
    """Verify availability notifier lifecycle uses authoritative queued map."""
    from schema_sanitizer.core_impl import process_resources as module

    notifier = module._AvailabilityNotifier()
    notifier._state = module._NotifierLifecycle.STOPPED
    marker = object()
    notifier._queued[(1, module.AvailabilityEvent.RETRY_SCHEDULER, marker)] = SimpleNamespace(
        next_attempt_ns=10**30
    )
    assert notifier.close(deadline_seconds=0.0) is False
    assert notifier.snapshot().delayed_callbacks == 1
    with pytest.raises(RuntimeError, match="non-quiescent"):
        notifier.reopen_for_tests()


def test_fork_quarantine_mode_cannot_hide_an_unreachable_child_callback() -> None:
    """Verify fork quarantine mode cannot hide an unreachable child callback."""
    from schema_sanitizer.core_impl import fork_manager as module

    with pytest.raises(ValueError, match="unreachable child callback"):
        module.register_fork_handler(
            "availability-notifier-lifecycle-uses-authoritative-queued-unreachable-child",
            after_in_child=lambda: None,
            mode="quarantine_only",
        )


def test_all_unprepared_child_handlers_explicitly_opt_into_child_safe_contract() -> None:
    """Verify all unprepared child handlers explicitly opt into child safe contract."""
    import ast

    offenders: list[str] = []
    for path in (ROOT / "src/schema_sanitizer").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            fn = node.func
            name = fn.id if isinstance(fn, ast.Name) else getattr(fn, "attr", "")
            if name not in {"register_fork_handler", "_register_fork_handler"}:
                continue
            keywords = {keyword.arg: keyword.value for keyword in node.keywords}
            if "after_in_child" not in keywords or "before" in keywords:
                continue
            opted_in = keywords.get("child_safe_without_prepare")
            if not isinstance(opted_in, ast.Constant) or opted_in.value is not True:
                offenders.append(f"{path.relative_to(ROOT)}:{node.lineno}")
    assert offenders == []


def test_native_allocation_registry_is_flat_bounded_and_resident_accounted() -> None:
    """Verify native allocation registry is flat bounded and resident accounted."""
    registry = (ROOT / "cpp/src/internal/memory/memory_pool_registry.cc.inc").read_text()
    tracking = (ROOT / "cpp/src/internal/memory/tracking_memory_pool.cc.inc").read_text()
    assert "std::unordered_map" not in registry
    assert (
        "std::make_unique<AllocationSlot[]>" in registry
        or "std::make_unique<GlobalAllocationSlot[]>" in registry
    )
    assert "kMaximumRecords" in registry or "kMaximumPoolRecords" in registry
    assert "kMaximumGlobalRegistryMetadataBytes" in registry
    assert "live_allocation_registry_metadata_capacity_bytes" in registry
    assert "live_allocation_registry_metadata_bytes" in registry
    assert "live_allocation_registry_rejections" in registry
    assert "process_resident_governor_" in tracking
    assert "live_allocation_registry_metadata_capacity_bytes()" in tracking
    catalog = (ROOT / "cpp/src/internal/abi/python_abi3/method_catalog.inc").read_text()
    assert "allocation_registry_stats," in catalog


def test_environment_thread_and_fd_requests_are_clamped_to_absolute_hard_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify environment thread and FD requests are clamped to absolute hard limits."""
    from schema_sanitizer.core_impl import process_resources as module

    monkeypatch.setattr(
        module.os,
        "get" + "env",
        lambda name: (
            "1000000"
            if name in {"SCHEMA_SANITIZER_MAX_PROJECT_THREADS", "SCHEMA_SANITIZER_MAX_OPEN_FILES"}
            else None
        ),
    )
    assert 2 <= module._thread_capacity() <= module._ABSOLUTE_MAX_PROJECT_THREADS
    assert 16 <= module._fd_capacity() <= module._ABSOLUTE_MAX_OPEN_FILES
    assert module._thread_capacity() <= module._thread_hard_capacity()
    assert module._fd_capacity() <= module._fd_hard_capacity()


def test_async_scheduler_has_process_global_task_and_control_admission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify async scheduler has process global task and control admission."""
    from schema_sanitizer.core_impl import async_scheduler as module

    monkeypatch.setattr(module, "_MAX_PROCESS_ASYNC_TASK_SLOTS", 2)
    first = module._acquire_async_scheduler_admission(2)
    try:
        assert 1 <= first.slots <= 2
        second = module._acquire_async_scheduler_admission(1)
        assert first.slots + second.slots <= 2
        assert module.async_scheduler_snapshot().in_use == first.slots + second.slots
        second.close()
    finally:
        first.close()
    assert module.async_scheduler_snapshot().in_use == 0


def test_async_scheduler_saturation_falls_back_to_inline_progress(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify async scheduler saturation falls back to inline progress."""
    from schema_sanitizer.core_impl import async_scheduler as module

    blocker = module._AsyncSchedulerAdmission(0)
    monkeypatch.setattr(module, "_acquire_async_scheduler_admission", lambda _requested: blocker)
    monkeypatch.setattr(
        module,
        "_start_indexed_workers",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("workers created")),
    )

    async def run() -> list[tuple[int, int]]:
        """Collect doubled results through the saturated asynchronous scheduler."""

        async def fetch(index: int) -> int:
            """Return the controlled fetch result used by the asynchronous operation."""
            return index * 2

        return [item async for item in module.ordered_indexed_results(4, fetch, window=4)]

    assert asyncio.run(run()) == [(0, 0), (1, 2), (2, 4), (3, 6)]


def test_ordered_executor_shutdown_is_notification_driven_not_busy_polled() -> None:
    """Verify ordered executor shutdown is notification driven not busy polled."""
    source = (ROOT / "cpp/src/internal/runtime/ordered_executor.hh").read_text()
    wait = source[
        source.index("WaitUntil(") : source.index(
            "Outcome ExecutePacket", source.index("WaitUntil(")
        )
    ]
    finish = source[
        source.index("void Finish(") : source.index(
            "AllScheduledFinished", source.index("void Finish(")
        )
    ]
    assert "sleep_for(std::chrono::microseconds(50))" not in wait
    assert "completion_ready.wait_until" in wait
    assert "completion_waiter.store(true" in wait
    assert "std::lock_guard lock(completion_mutex)" in finish
    assert "completion_ready.notify_all()" in finish


def test_cleanup_dispatcher_control_budget_calls_are_outside_dispatcher_lock() -> None:
    """Verify cleanup dispatcher control budget calls are outside dispatcher lock."""
    source = _source("core_impl/cleanup_dispatcher.py")
    uncharge = source[
        source.index("    def _uncharge_owner_locked") : source.index(
            "    def _enqueue_runnable_locked"
        )
    ]
    submit = source[
        source.index("    def submit(") : source.index("    def _has_failed_worker_leases_locked")
    ]
    assert "release_control_plane(" not in uncharge
    reserve_index = submit.index('reserve_control_plane("cleanup_call", 384)')
    assert reserve_index < submit.index("with self._condition", reserve_index)
    assert "release_control_plane(release_ticket)" in submit


def test_memory_emergency_finalizer_roots_are_physically_preallocated() -> None:
    """Verify memory emergency finalizer roots are physically preallocated."""
    memory = _source("core_impl/memory_budget.py")
    assert "[None] * _MAX_ABANDONED_MEMORY_OWNERS" in memory
    assert "_ABANDONED_MEMORY_EMERGENCY.append(" not in memory


def test_stage_concurrency_admission_composes_control_plane_with_slots_and_bytes() -> None:
    """Verify stage concurrency admission composes control plane with slots and bytes."""
    from schema_sanitizer.core_impl import memory_budget as module

    assert issubclass(module.StageConcurrencyAdmission, module.CompositeParallelAdmission)
    assert module.StageConcurrencyAdmission is not module.CompositeParallelAdmission
    admission = module.StageConcurrencyAdmission(1, 1024)
    assert hasattr(admission, "control_ticket")
    source = _source("core_impl/memory_budget.py")
    assert "stage_concurrency:" in source
    assert '"stage_concurrency_admission"' in source


def test_remote_group_scans_do_not_materialize_dict_item_pairs() -> None:
    """Verify remote group scans do not materialize dict item pairs."""
    for relative in (
        "remote_impl/providers/s3.py",
        "remote_impl/providers/gcs.py",
        "remote_impl/providers/azure.py",
    ):
        source = _source(relative)
        assert "list(groups.items())" not in source
        assert "group_keys = tuple(groups)" not in source
        assert "drain_ordered_iterable_results" in source
