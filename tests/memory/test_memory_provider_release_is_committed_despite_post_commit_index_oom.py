"""Regression coverage for memory provider release is committed despite post commit index oom."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from threading import Lock

import pytest


class _FailingSetOrderedDict(dict):
    def __setitem__(self, key, value):  # type: ignore[no-untyped-def]
        raise MemoryError("injected derived-index OOM")


def test_provider_release_is_committed_despite_post_commit_index_oom() -> None:
    from schema_sanitizer.remote_impl.provider_throttle import ProviderThrottleGovernor

    governor = ProviderThrottleGovernor(max_tracked_keys=4)
    lease, delay = governor.try_acquire("provider-release-is-committed-despite-post-endpoint")
    assert lease is not None and delay == 0.0
    governor._inactive_keys = _FailingSetOrderedDict()  # type: ignore[assignment]

    # The physical slot/outcome must commit even though the eviction accelerator
    # cannot be updated. Most importantly the wrapper must not reactivate itself.
    lease.release()
    assert lease._state == "released"
    assert governor.snapshot("provider-release-is-committed-despite-post-endpoint").in_flight == 0
    registry = governor.registry_snapshot()
    assert registry.active_leases == 0
    assert registry.post_commit_failures >= 1


def test_provider_circuit_expiration_storage_is_o_live_keys_not_failures() -> None:
    from schema_sanitizer.remote_impl.provider_throttle import ProviderThrottleGovernor

    governor = ProviderThrottleGovernor(max_tracked_keys=2)
    for _ in range(200):
        lease, _delay = governor.try_acquire("same-endpoint")
        if lease is None:
            # The circuit can be open after repeated failures; expiry metadata
            # must remain bounded even when admission is temporarily rejected.
            continue
        lease._release_outcome(outcome="failure", throttled=True, retry_after_seconds=0.0)
        # Keep exercising circuit extensions without sleeping; authoritative
        # lease ownership has already been retired by _release_outcome.
        with governor._condition:
            governor._states["same-endpoint"].circuit_open_until = 0.0
    snap = governor.registry_snapshot()
    assert snap.tracked_keys <= 1
    assert snap.expiry_entries == 0
    # The fixed retry index keeps exactly one mutable heap node per tracked key. Repeated
    # circuit extensions therefore remain O(live keys), not O(events).
    assert snap.peak_expiry_entries <= snap.max_tracked_keys
    assert snap.stale_expiry_entries == 0


def test_remote_permit_release_never_reactivates_after_scheduler_oom(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.remote_impl.io_permits import RemoteIoPermitGovernor

    async def scenario() -> None:
        governor = RemoteIoPermitGovernor(capacity=1)
        permit = await governor.acquire(operation_id="provider-release-is-committed-despite-post")
        monkeypatch.setattr(
            governor,
            "_grant_ready_locked",
            lambda: (_ for _ in ()).throw(MemoryError("scheduler OOM")),
        )
        permit.release()
        snap = governor.snapshot()
        assert permit._released is True
        assert snap.in_use == 0
        assert snap.active_permits == 0
        assert snap.post_commit_failures >= 1

    asyncio.run(scenario())


def test_remote_capacity_prepare_failure_retains_exact_capability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.remote_impl.io_permits import RemoteIoPermitGovernor

    governor = RemoteIoPermitGovernor(capacity=2)
    registration = governor.register_capacity(5)
    before = governor.snapshot()
    monkeypatch.setattr(
        governor,
        "_prepare_registration_removal_locked",
        lambda _token: (_ for _ in ()).throw(MemoryError("prepare OOM")),
    )
    with pytest.raises(MemoryError):
        registration.release()
    after = governor.snapshot()
    assert registration._released is False
    assert after.active_capacity_capabilities == before.active_capacity_capabilities == 1
    assert after.active_capacity_registrations == before.active_capacity_registrations == 1
    assert after.capacity == before.capacity
    # Do not invoke a second release through the patched function in __del__.
    registration._pid = -1


def test_cross_process_storage_account_is_exact_and_serialized() -> None:
    from schema_sanitizer.core_impl.cross_process_storage import (
        close_cross_process_storage_account,
        open_cross_process_storage_account,
        release_cross_process_account,
        reserve_cross_process_account,
    )

    account = open_cross_process_storage_account(47)
    calls: list[tuple[str, int, int]] = []

    def reserve_impl(device: int, amount: int, capacity: int, **kwargs: object) -> int:
        calls.append(("reserve", device, amount))
        return amount

    def release_impl(device: int, amount: int, **kwargs: object) -> int:
        calls.append(("release", device, amount))
        return 0

    assert reserve_cross_process_account(account, 16, 128, _reserve_impl=reserve_impl) == 16
    assert account.reserved_bytes == 16
    with pytest.raises(RuntimeError, match="exceeds authoritative contribution"):
        release_cross_process_account(account, 17, _release_impl=release_impl)
    assert account.reserved_bytes == 16
    release_cross_process_account(account, 16, _release_impl=release_impl)
    assert account.reserved_bytes == 0
    close_cross_process_storage_account(account)
    with pytest.raises(RuntimeError, match="not active"):
        reserve_cross_process_account(account, 1, 128, _reserve_impl=reserve_impl)
    with pytest.raises(RuntimeError, match="not active"):
        close_cross_process_storage_account(account)
    assert calls == [("reserve", 47, 16), ("release", 47, 16)]


def test_cross_process_storage_local_account_registry_is_bounded_and_reuses_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import cross_process_storage as module

    # Isolate this test from the process-global registry used by other tests.
    monkeypatch.setattr(module, "_ACCOUNTS", {})
    monkeypatch.setattr(module, "_ACCOUNT_SEQUENCE", 0)
    monkeypatch.setattr(module, "_MAX_LOCAL_ACCOUNTS", 2)
    first = module.open_cross_process_storage_account(1)
    second = module.open_cross_process_storage_account(2)
    with pytest.raises(RuntimeError, match="registry exhausted"):
        module.open_cross_process_storage_account(3)
    first_token = first.token
    module.close_cross_process_storage_account(first)
    third = module.open_cross_process_storage_account(3)
    assert third.token == first_token
    # Token reuse is safe because the old object/capability no longer authenticates.
    with pytest.raises(RuntimeError, match="not active"):
        module._authenticate_account(first)
    module.close_cross_process_storage_account(second)
    module.close_cross_process_storage_account(third)


def test_control_plane_budget_is_exact_and_bounded() -> None:
    from schema_sanitizer.core_impl.control_plane_budget import _ProcessControlPlaneBudget
    from schema_sanitizer.errors import SchemaSanitizerResourceError

    budget = _ProcessControlPlaneBudget()
    budget.configure(1024)
    first = budget.reserve("waiter", 600)
    with pytest.raises(SchemaSanitizerResourceError):
        budget.reserve("retry", 500)
    snap = budget.snapshot()
    assert snap.reserved_bytes == 600
    assert snap.active_tickets == 1
    assert snap.rejected_tickets == 1
    # Mutable diagnostics are never release authority.
    first.amount = 1024
    first.token += 1
    budget.release(first)
    assert budget.snapshot().reserved_bytes == 600
    first.token -= 1
    budget.release(first)
    budget.release(first)
    snap = budget.snapshot()
    assert snap.reserved_bytes == 0
    assert snap.active_tickets == 0
    # One stale-capability mutation is rejected. Releasing the already-retired
    # exact ticket again is intentionally idempotent.
    assert snap.over_release_count == 1


def test_control_plane_budget_composes_with_native_resident_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    from schema_sanitizer.core_impl import memory_budget as memory_module
    from schema_sanitizer.core_impl.control_plane_budget import _ProcessControlPlaneBudget
    from schema_sanitizer.errors import SchemaSanitizerResourceError

    monkeypatch.setattr(
        memory_module,
        "_optional_process_resident_memory_snapshot",
        lambda: SimpleNamespace(capacity_bytes=1024, reserved_bytes=800),
    )
    budget = _ProcessControlPlaneBudget()
    budget.configure(1024)
    with pytest.raises(SchemaSanitizerResourceError) as captured:
        budget.reserve("combined", 256)
    assert captured.value.detail["limit_name"] == "process_governed_memory_bytes"
    assert budget.snapshot().reserved_bytes == 0


def test_control_plane_budget_stays_hard_bounded_without_native_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import memory_budget as memory_module
    from schema_sanitizer.core_impl.control_plane_budget import _ProcessControlPlaneBudget

    monkeypatch.setattr(
        memory_module,
        "_optional_process_resident_memory_snapshot",
        lambda: None,
    )
    budget = _ProcessControlPlaneBudget()
    budget.configure(512)
    ticket = budget.reserve("source-only", 256)
    snapshot = budget.snapshot()
    assert snapshot.reserved_bytes == 256
    assert snapshot.capacity_bytes == 512
    budget.release(ticket)
    assert budget.snapshot().reserved_bytes == 0


def test_operation_memory_reservation_uses_shared_governed_admission_lock() -> None:
    source = Path("src/schema_sanitizer/core_impl/memory_budget.py").read_text()
    assert "with _GOVERNED_MEMORY_ADMISSION_LOCK:" in source
    assert "resident.reserved_bytes" in source
    assert "+ control.governed_bytes" in source
    assert "process_governed_memory_bytes" in source


def test_operation_memory_credit_transfer_keeps_same_owner_and_bytes() -> None:
    from schema_sanitizer.core_impl.memory_budget import OperationMemoryLease

    lease = object.__new__(OperationMemoryLease)
    lease._pid = os.getpid()
    lease._lock = Lock()
    lease._released = False
    lease._size_bytes = 8192
    lease.stage = "reader"
    result = lease.transfer_stage("writer")
    assert result is lease
    assert lease.stage == "writer"
    assert lease._size_bytes == 8192


def test_composite_parallel_admission_transfers_and_releases_once() -> None:
    from schema_sanitizer.core_impl.memory_budget import CompositeParallelAdmission

    class FakeLease:
        def __init__(self) -> None:
            self.stages: list[str] = []
            self.closed = 0

        def transfer_stage(self, stage: str) -> None:
            self.stages.append(stage)

        def close(self) -> None:
            self.closed += 1

    lease = FakeLease()
    admission = CompositeParallelAdmission(3, 4096, lease)  # type: ignore[arg-type]
    assert admission.reserved_bytes == 12288
    assert admission.transfer_stage("serialize") is admission
    admission.close()
    admission.close()
    assert lease.stages == ["serialize"]
    assert lease.closed == 1


def test_all_io_pairs_advertise_integrated_credit_and_composite_admission() -> None:
    from schema_sanitizer.core_impl import concurrency_coverage as coverage

    matrix = coverage.concurrency_pair_guarantees()
    rows = [row for outputs in matrix.values() for row in outputs.values()]
    assert len(rows) == 56
    assert all(row["resident_credit_transfer"] is True for row in rows)
    assert all(row["composite_slot_byte_admission"] is True for row in rows)
    assert all(row["control_plane_budgeted"] is True for row in rows)


def test_reserved_finalizer_generation_exhaustion_retires_slot_without_wrap() -> None:
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    escrow: ReservedFinalizerEscrow[object] = ReservedFinalizerEscrow(1)
    escrow._generations[0] = escrow._max_generation
    assert escrow.reserve_ticket() is None
    snap = escrow.capacity_snapshot()
    assert snap.capacity == 1
    assert snap.active == 0
    assert snap.available == 0
    assert snap.retired == 1
    # Generation exhaustion is a safe admission/capacity loss, not evidence
    # that a published owner was lost.
    assert snap.overflowed is False
    assert snap.publication_failures == 0
    assert snap.invariant_ok
    assert escrow.reserved_count() == 0


def test_runtime_registry_constructor_oom_cannot_publish_invisible_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import runtime_registry as module

    class Service:
        def close(self, *, deadline_seconds: float) -> bool:
            return True

    registry = module._RuntimeServiceRegistry()

    class FailingRegistration:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            raise MemoryError("registration OOM")

    monkeypatch.setattr(module, "RuntimeServiceRegistration", FailingRegistration)
    with pytest.raises(MemoryError):
        registry.reserve(
            Service(), kind="provider-release-is-committed-despite-post", close_name="close"
        )
    assert registry.snapshot().registered_services == 0


def test_runtime_thread_start_commit_does_not_report_diagnostic_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from threading import Event, Thread

    from schema_sanitizer.core_impl.runtime_registry import _RuntimeServiceRegistry

    class Service:
        def close(self, *, deadline_seconds: float) -> bool:
            return True

    registry = _RuntimeServiceRegistry()
    registration = registry.reserve(
        Service(), kind="provider-release-is-committed-despite-post", close_name="close"
    )
    exited = Event()
    thread = Thread(target=lambda: exited.wait(1.0))

    # All lifecycle progress publication around the physical start/retirement is
    # diagnostic-only. Once the registration exists, diagnostic OOM must not
    # strand START_AUTHORIZED state, mask a successful thread.start(), or make
    # unregister fail after the thread exits.
    def fail_diagnostics() -> None:
        raise MemoryError("diagnostic OOM")

    monkeypatch.setattr(registry, "_mark_progress_locked", fail_diagnostics)
    registration.start_thread(thread)
    assert thread.is_alive()
    assert registry.snapshot().registered_services == 1
    exited.set()
    thread.join(1.0)
    registration.close()
    assert registry.snapshot().registered_services == 0


def test_level_triggered_availability_failure_leaves_autonomous_retry_debt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import process_resources as module

    governor = module._Governor(
        1, "provider-release-is-committed-despite-post", level_triggered_availability=True
    )
    lease = governor.acquire(1)
    assert governor.register_availability_event(module.AvailabilityEvent.RETRY_SCHEDULER)

    scheduled: list[object] = []
    monkeypatch.setattr(
        governor,
        "_schedule_availability_retry_noexcept",
        lambda: scheduled.append(governor._availability_retry_callback),
    )
    monkeypatch.setattr(module._AVAILABILITY_NOTIFIER, "publish", lambda _batch: (object(),))
    lease.release()
    assert governor._availability_dirty is True
    assert scheduled == [governor._availability_retry_callback]

    monkeypatch.setattr(module._AVAILABILITY_NOTIFIER, "publish", lambda _batch: ())
    callback = scheduled.pop()
    callback()  # type: ignore[operator]
    assert governor._availability_dirty is False


def test_shutdown_uses_multiple_finalizer_quiescence_barriers() -> None:
    source = Path("src/schema_sanitizer/core_impl/runtime_shutdown.py").read_text()
    assert "def quiesce_finalizers" in source
    assert source.count("quiesce_finalizers(") >= 4  # definition + three barriers
    assert 'authoritative["finalizer_admission"]' in source
    assert "finalizer_redrain_rounds" in source


def test_cross_process_registries_prune_in_place_and_have_record_bounds() -> None:
    storage = Path("src/schema_sanitizer/core_impl/cross_process_storage.py").read_text()
    memory = Path("src/schema_sanitizer/core_impl/cross_process_memory.py").read_text()
    assert "_MAX_PROCESS_RECORDS = 4096" in storage
    assert "_MAX_PROCESS_LEASE_RECORDS = 4096" in memory
    assert "stale_key" in storage and "processes.pop(stale_key" in storage
    assert "stale_key" in memory and "leases.pop(stale_key" in memory
    assert "dict(processes)" not in storage
    assert "dict(leases)" not in memory
