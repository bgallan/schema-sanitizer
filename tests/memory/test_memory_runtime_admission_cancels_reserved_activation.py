"""Exercises cancellation of reserved service activation with deadline close, teardown
capacity, terminal-host pruning, cleanup-graph charges, cross-memory coalescing,
adaptive slots, coercion rejection, bounded terminal ledgers, observability, and
fork-lock reset. Cancelled activation never becomes live; shutdown requires complete
observable ownership, and inherited or hostile accounting cannot enter stage gates."""

from __future__ import annotations

import gc
import os

import pytest
from _support.synchronization import run_isolated_python_probe


def test_runtime_admission_cancels_reserved_activation() -> None:
    """Verify runtime admission cancels reserved activation."""
    from schema_sanitizer.core_impl.runtime_registry import _RuntimeServiceRegistry

    class Service:
        def close(self, *, deadline_seconds: float) -> bool:
            """Close the resources owned by the service test double."""
            return True

    registry = _RuntimeServiceRegistry()
    registration = registry.reserve(
        Service(), kind="runtime-admission-cancels-reserved-activation", close_name="close"
    )
    registry.close_admission()
    with pytest.raises(RuntimeError, match="admission closed"):
        registration.activate()
    snapshot = registry.snapshot()
    assert snapshot.registered_services == 0
    assert snapshot.reserved_services == 0


def test_runtime_service_requires_deadline_close_contract() -> None:
    """Verify runtime service requires deadline close contract."""
    from schema_sanitizer.core_impl.runtime_registry import _RuntimeServiceRegistry

    class Service:
        def close(self) -> bool:
            """Close the resources owned by the service test double."""
            return True

    with pytest.raises(TypeError, match="deadline_seconds"):
        _RuntimeServiceRegistry().reserve(
            Service(), kind="runtime-admission-cancels-reserved-activation", close_name="close"
        )


def test_governor_preserves_physical_teardown_capacity() -> None:
    """Verify governor preserves physical teardown capacity."""
    from schema_sanitizer.core_impl.process_resources import _Governor

    governor = _Governor(
        4, label="runtime-admission-cancels-reserved-activation", teardown_reserve=1
    )
    external = governor.acquire(3, timeout_seconds=0.01)
    snapshot = governor.snapshot()
    assert snapshot.external_capacity == 3
    assert snapshot.external_in_use == 3
    teardown = governor.acquire(1, timeout_seconds=0.01, _teardown=True)
    assert governor.snapshot().in_use == 4
    with pytest.raises(Exception):
        governor.acquire(1, timeout_seconds=0.0)
    teardown.release()
    external.release()


def test_terminal_hosts_prune_dead_weakrefs_before_capacity_check() -> None:
    """Verify terminal hosts prune dead weakrefs before capacity check."""
    from schema_sanitizer.core_impl.terminal_hosts import TerminalHostMarkers

    class Host:
        pass

    markers = TerminalHostMarkers(
        1, category="runtime-admission-cancels-reserved-activation_terminal"
    )
    host = Host()
    assert markers.add(host)
    del host
    gc.collect()
    replacement = Host()
    assert markers.add(replacement)
    snapshot = markers.snapshot()
    assert snapshot.hosts == 1
    assert snapshot.dead_pruned >= 1
    markers.discard(replacement)


def test_cleanup_rejects_floor_charged_bound_owner_graph(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify cleanup rejects floor charged bound owner graph."""
    from schema_sanitizer.core_impl.cleanup_dispatcher import _CleanupDispatcher

    dispatcher = _CleanupDispatcher()
    monkeypatch.setattr(dispatcher, "_ensure_workers", lambda: None)

    class Owner:
        def cleanup(self) -> None:
            """Return the cleanup callback retained by the lifecycle owner."""
            return None

    assert not dispatcher.submit(Owner().cleanup, retained_bytes=1024)
    snapshot = dispatcher.snapshot()
    assert snapshot.rejected_hidden_owner_calls == 1
    assert snapshot.owned_calls == 0


def test_cleanup_separates_retained_from_already_reserved_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify cleanup separates retained from already reserved bytes."""
    from schema_sanitizer.core_impl.cleanup_dispatcher import _CleanupDispatcher

    dispatcher = _CleanupDispatcher()
    monkeypatch.setattr(dispatcher, "_ensure_workers", lambda: None)

    def cleanup() -> None:
        """Return the cleanup task retained by the lifecycle owner."""
        return None

    assert dispatcher.submit(cleanup, retained_bytes=2048, reserved_bytes=8 << 20)
    snapshot = dispatcher.snapshot()
    assert snapshot.owned_bytes == 2048
    assert snapshot.owned_reserved_bytes == 8 << 20


def test_process_cross_memory_aggregates_growth_and_coalesces_shrink(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify process cross memory aggregates growth and coalesces shrink."""
    from schema_sanitizer.core_impl import cross_process_memory as module

    class Physical:
        def __init__(self, _capacity: int, _initial: int) -> None:
            """Initialize the physical test double."""
            self._coordinated = False
            self._coordination_path = None
            self.calls: list[int] = []

        def resize(self, value: int) -> None:
            """Resize the resource represented by the physical test double."""
            self.calls.append(value)

        def _set_capacity(self, _value: int) -> None:
            """Set the governor capacity for the contention scenario."""
            return

    monkeypatch.setattr(module, "CrossProcessMemoryLease", Physical)
    coordinator = module._ProcessCrossMemoryCoordinator(64 << 20)
    first = coordinator.acquire(1 << 20)
    second = coordinator.acquire(1 << 20)
    assert coordinator._physical.calls == [4 << 20]
    first.resize(5 << 20)
    assert coordinator._physical.calls[-1] == 8 << 20
    # Finalizer publication is token-only and must not touch the coordinator
    # lock or cross-process state until a governed path drains it.
    with coordinator._lock:
        second._release_nonblocking()
    assert not coordinator._pending_shrink
    coordinator.reconcile_pending()
    assert coordinator._pending_shrink is False
    assert second._token not in coordinator._contributions
    first._released = True
    second._released = True


def test_adaptive_parallel_slots_can_suppress_all_helper_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify adaptive parallel slots can suppress all helper work."""
    from schema_sanitizer.core_impl import memory_budget as module
    from schema_sanitizer.core_impl import system_pressure

    snapshot = module.ProcessResidentMemorySnapshot(
        capacity_bytes=16 << 20,
        reserved_bytes=16 << 20,
        peak_reserved_bytes=16 << 20,
    )
    monkeypatch.setattr(module, "process_resident_memory_snapshot", lambda: snapshot)
    monkeypatch.setattr(system_pressure, "pressure_adjusted_target", lambda desired: desired)
    assert module.adaptive_parallel_slots(8, per_slot_bytes=1 << 20, reserve_bytes=0) == 0
    assert module.adaptive_concurrency_target(8, per_slot_bytes=1 << 20, reserve_bytes=0) == 1


def test_cross_process_storage_rejects_coercive_accounting_values() -> None:
    """Verify cross process storage rejects coercive accounting values."""
    from schema_sanitizer.core_impl.cross_process_storage import _reserve_cross_process_raw

    with pytest.raises(TypeError, match="exact integers"):
        _reserve_cross_process_raw(1, True, 1024, enabled=False)


def test_terminal_ownership_ledger_is_metadata_only_and_bounded() -> None:
    """Verify terminal ownership ledger is metadata only and bounded."""
    from schema_sanitizer.core_impl.terminal_ownership import TerminalOwnershipLedger

    ledger = TerminalOwnershipLedger(capacity=2)
    assert ledger.publish("a", 1, retained_bytes=10)
    assert ledger.publish("b", 2, retained_bytes=20)
    assert not ledger.publish("c", 3, retained_bytes=30)
    snapshot = ledger.snapshot()
    assert snapshot.owners == 2
    assert snapshot.retained_bytes == 30
    assert snapshot.rejected == 1
    ledger.retire("a", 1)
    assert ledger.snapshot().owners == 1


def test_runtime_shutdown_success_requires_complete_observability() -> None:
    """Verify runtime shutdown success requires complete observability."""
    import inspect

    from schema_sanitizer.core_impl import runtime_shutdown

    source = inspect.getsource(runtime_shutdown._perform_shutdown)
    assert "observability_complete = not observability_failures" in source
    assert "terminal_ownership:publication_rejected" in source
    assert "resources_drained," in source
    assert "observability_complete," in source


def test_cleanup_publication_can_be_finalizer_safe_without_starting_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify cleanup publication can be finalizer safe without starting worker."""
    from schema_sanitizer.core_impl import cleanup_dispatcher as module

    dispatcher = module._CleanupDispatcher()
    starts: list[bool] = []
    monkeypatch.setattr(dispatcher, "_ensure_workers", lambda: starts.append(True))
    accepted = dispatcher.submit(
        lambda: None,
        retained_bytes=256,
        start_worker=False,
        subsystem=module.CleanupSubsystem.MEMORY,
    )
    assert accepted
    assert starts == []
    snapshot = dispatcher.snapshot()
    assert snapshot.owned_calls == 1


def _probe_cross_process_memory_fork_reset() -> None:
    """Exercise the cross-process reset in a disposable process."""
    from schema_sanitizer.core_impl import cross_process_memory as module

    old_process_lock = module._PROCESS_COORDINATOR_LOCK
    old_process_lock.acquire()
    try:
        module._reset_cross_process_memory_after_fork()
        assert module._PROCESS_COORDINATOR_LOCK is not old_process_lock
    finally:
        old_process_lock.release()


def test_cross_process_memory_fork_reset_rebinds_inherited_locks() -> None:
    """Verify cross process memory fork reset rebinds inherited locks."""
    run_isolated_python_probe(__file__, "_probe_cross_process_memory_fork_reset")


def _probe_operation_memory_fork_reset() -> None:
    """Exercise the operation-memory reset in a disposable process."""
    from schema_sanitizer.core_impl import memory_budget as module

    old_lock = module._ABANDONED_MEMORY_LOCK
    old_lock.acquire()
    try:
        module._reset_operation_memory_ledger_after_fork()
        assert module._ABANDONED_MEMORY_LOCK is not old_lock
    finally:
        old_lock.release()


def test_operation_memory_fork_reset_rebinds_abandoned_owner_lock() -> None:
    """Verify operation memory fork reset rebinds abandoned owner lock."""
    run_isolated_python_probe(__file__, "_probe_operation_memory_fork_reset")


def test_operation_memory_stage_rejects_coercive_objects() -> None:
    """Verify operation memory stage rejects coercive objects."""
    from schema_sanitizer.core_impl.memory_budget import OperationMemoryLedger

    ledger = object.__new__(OperationMemoryLedger)
    ledger._pid = os.getpid()
    with pytest.raises(TypeError, match="exact string"):
        OperationMemoryLedger.reserve(ledger, 1, stage=object())  # type: ignore[arg-type]
