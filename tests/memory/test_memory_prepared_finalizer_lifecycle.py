"""Exercises prepared-finalizer publication, cancellation acknowledgement transfer, stale
control capabilities, temporary-device identity, remote freeze, cross-process roots,
shutdown observation, wrapper deferral, provider tails, path rollback, and cleanup
outside locks. Primary authority disarms before secondary retirement, stale or underflow
states fail closed, and tail retries never double-release."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src" / "schema_sanitizer"


def _fail_first_release_for(monkeypatch: pytest.MonkeyPatch, target: object) -> None:
    """Inject the first release for failure at the controlled test point."""
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    original = ReservedFinalizerEscrow.release_ticket
    failed = False

    def flaky(self: object, ticket: int) -> bool:
        """Inject the flaky failure at the controlled test point."""
        nonlocal failed
        if self is target and not failed:
            failed = True
            return False
        return original(self, ticket)  # type: ignore[arg-type]

    monkeypatch.setattr(ReservedFinalizerEscrow, "release_ticket", flaky)


def test_prepared_finalizer_pre_root_survives_publish_lock_contention() -> None:
    """Verify prepared finalizer pre root survives publish lock contention."""
    from schema_sanitizer.core_impl import finalizer_cleanup as module

    escrow = module._PREPARED_FINALIZER_ESCROW
    baseline_active = escrow.active_count()
    baseline_published = escrow.published_count()
    calls = 0

    def cleanup(_capsule: object) -> None:
        """Count each cleanup invocation."""
        nonlocal calls
        calls += 1

    capsule = module.reserve_finalizer_cleanup(cleanup)
    ticket = capsule.ticket
    slot = escrow._ticket_slots[ticket]
    lock = escrow._slot_locks[slot]

    lock.acquire()
    try:
        # Publication cannot take the slot lock, but the owner was rooted before
        # wrapper exposure and can therefore make a durable allocation-free handoff.
        assert module.defer_prepared_finalizer_cleanup(capsule)
        assert capsule.ticket == 0
        assert escrow.active_count() == baseline_active + 1
        assert escrow.published_count() == baseline_published
    finally:
        lock.release()

    module.drain_finalizer_cleanup()
    assert calls == 1
    assert escrow.active_count() <= baseline_active


def test_prepared_cancel_failure_transfers_ack_without_primary_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify prepared cancel failure transfers ack without primary replay."""
    from schema_sanitizer.core_impl import finalizer_cleanup as module

    calls = 0

    def primary(_capsule: object) -> None:
        """Run the primary operation before its injected cleanup failure."""
        nonlocal calls
        calls += 1

    capsule = module.reserve_finalizer_cleanup(primary)
    _fail_first_release_for(monkeypatch, module._PREPARED_FINALIZER_ESCROW)

    # Exact retirement fault-injection must not strand the newly pre-rooted
    # RESERVED owner. Cancellation durably converts it into ACK-only cleanup.
    module.cancel_prepared_finalizer_cleanup(capsule)
    assert capsule.ticket == 0
    module.drain_finalizer_cleanup()
    assert calls == 0


def test_control_plane_stale_live_capability_fails_closed() -> None:
    """Verify control plane stale live capability fails closed."""
    from schema_sanitizer.core_impl.control_plane_budget import (
        ControlPlaneTicket,
        _ProcessControlPlaneBudget,
    )

    budget = _ProcessControlPlaneBudget(include_static_baseline=False)
    ticket = budget.reserve("prepared-finalizer-pre-root-survives-publish_stale", 256)
    stale = ControlPlaneTicket(
        amount=ticket.amount,
        kind=ticket.kind,
        pid=ticket.pid,
        token=ticket.token,
        capability=object(),
    )

    assert budget.release(stale) is False
    assert ticket.released is False
    assert budget._owners[ticket.token].ticket_ref() is ticket
    assert budget.snapshot().reserved_bytes >= 256

    assert budget.release(ticket) is True
    assert ticket.released is True
    assert ticket.token not in budget._owners


def test_control_plane_failed_tail_can_be_retried_from_ledger_root() -> None:
    """Verify control plane failed tail can be retried from ledger root."""
    from schema_sanitizer.core_impl.control_plane_budget import _ProcessControlPlaneBudget

    budget = _ProcessControlPlaneBudget(include_static_baseline=False)
    ticket = budget.reserve("prepared-finalizer-pre-root-survives-publish_deferred", 256)
    assert budget.request_retirement(ticket)
    assert budget._owners[ticket.token].ticket_ref() is ticket
    assert ticket.retire_requested is True

    assert budget.drain_requested_retirements(limit=1) == 1
    assert ticket.released is True
    assert ticket.token not in budget._owners


def test_temporary_storage_governor_users_underflow_pins_device_identity() -> None:
    """Verify temporary storage governor users underflow pins device identity."""
    from schema_sanitizer.core_impl.temporary_storage_governor import (
        _FilesystemReservationState,
        _ProcessTemporaryStorageGovernor,
    )

    governor = _ProcessTemporaryStorageGovernor()
    state = _FilesystemReservationState(capacity_bytes=1024, capacity_inodes=1024)
    governor._states[7] = state
    assert state.users == 0

    governor._return_state(7, state)

    assert governor._states[7] is state
    assert governor._protocol_violations == 1


def test_remote_io_admission_freeze_rejects_new_authorities() -> None:
    """Verify remote I/O admission freeze rejects new authorities."""
    from schema_sanitizer.remote_impl.io_permits import RemoteIoPermitGovernor

    governor = RemoteIoPermitGovernor(2)
    governor.close_admission()
    snapshot = governor.snapshot()
    assert snapshot.admission_closed is True

    with pytest.raises(RuntimeError, match="admission is closed"):
        governor.reserve_submission()
    with pytest.raises(RuntimeError, match="admission is closed"):
        governor.register_capacity(1)


def test_cross_process_reservation_uses_separate_pre_rooted_finalizer_owner() -> None:
    """Verify cross process reservation uses separate pre rooted finalizer owner."""
    import gc

    from schema_sanitizer.core_impl.cross_process_memory import _ProcessCrossMemoryCoordinator

    coordinator = _ProcessCrossMemoryCoordinator(16 << 20)
    reservation = coordinator.acquire(0)
    token = reservation._token
    ticket = reservation._finalizer_ticket
    owner = reservation._finalizer_owner
    escrow = coordinator._finalizer_releases
    slot = escrow._ticket_slots[ticket]

    # The escrow roots only the compact cleanup authority, never the reservation
    # whose __del__ must remain reachable when user ownership disappears.
    assert escrow._slots[slot] is owner
    assert escrow._slots[slot] is not reservation
    assert not owner.is_armed_for(ticket)

    del reservation
    gc.collect()
    assert owner.is_armed_for(ticket)
    coordinator.reconcile_pending()
    assert token not in coordinator._contributions


def test_authoritative_quiescence_counters_do_not_saturate() -> None:
    """Verify authoritative quiescence counters do not saturate."""
    sources = {
        "temporary_storage": SRC / "core_impl" / "temporary_storage.py",
        "partition": SRC / "pipeline" / "partition_lookahead.py",
        "async_scheduler": SRC / "core_impl" / "async_scheduler.py",
        "storage_governor": SRC / "core_impl" / "temporary_storage_governor.py",
        "remote_io": SRC / "remote_impl" / "io_permits.py",
    }
    text = {name: path.read_text(encoding="utf-8") for name, path in sources.items()}

    assert "self._pending_active_leases = max(0" not in text["temporary_storage"]
    assert "self._active_leases = max(0" not in text["temporary_storage"]
    assert "self._submissions_inflight = max(0" not in text["partition"]
    assert "_ASYNC_TASK_SLOTS_IN_USE = max(0" not in text["async_scheduler"]
    assert "_ASYNC_ACTIVE_OPERATIONS = max(0" not in text["async_scheduler"]
    assert "_ASYNC_TERMINAL_DEBT_COUNT = max(0" not in text["async_scheduler"]
    assert "state.users = max(0" not in text["storage_governor"]
    assert "self._waiting_count = max(0" not in text["remote_io"]
    assert "self._sync_waiters = max(0" not in text["remote_io"]
    assert "self._pending_submissions = max(0" not in text["remote_io"]


def test_shutdown_observes_remote_waiters_protocol_and_freeze() -> None:
    """Verify shutdown observes remote waiters protocol and freeze."""
    source = (SRC / "core_impl" / "runtime_shutdown.py").read_text(encoding="utf-8")
    assert "close_remote_io_permit_admission" in source
    assert "remote_io_waiting" in source
    assert "remote_io_sync_waiters" in source
    assert "remote_io_protocol_violations" in source
    assert '"remote_io:protocol_violation"' in source


def test_io_wrappers_use_ack_after_primary_release_and_safe_defer() -> None:
    """Verify I/O wrappers use ack after primary release and safe defer."""
    paths = [
        SRC / "remote_impl" / "transport.py",
        SRC / "remote_impl" / "sync_http.py",
        SRC / "remote_impl" / "upload_policy.py",
        SRC / "api_impl" / "results.py",
    ]
    bodies = [path.read_text(encoding="utf-8") for path in paths]
    for body in bodies:
        assert "acknowledge_prepared_finalizer_cleanup" in body
    assert (
        "if ticket and capsule is not None and defer_prepared_finalizer_cleanup(capsule):"
        in bodies[0]
    )
    assert (
        "if ticket and capsule is not None and defer_prepared_finalizer_cleanup(capsule):"
        in bodies[1]
    )
    assert (
        "if ticket and capsule is not None and defer_prepared_finalizer_cleanup(capsule):"
        in bodies[2]
    )


def test_prepared_finalizer_ack_disarms_primary_before_retirement_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify prepared finalizer ack disarms primary before retirement failure."""
    from schema_sanitizer.core_impl import finalizer_cleanup as module

    calls = 0

    def primary(_capsule: object) -> None:
        """Run the primary operation before its injected cleanup failure."""
        nonlocal calls
        calls += 1

    capsule = module.reserve_finalizer_cleanup(primary)
    _fail_first_release_for(monkeypatch, module._PREPARED_FINALIZER_ESCROW)

    with pytest.raises(RuntimeError, match="acknowledgement did not commit"):
        module.acknowledge_prepared_finalizer_cleanup(capsule)

    assert capsule.ticket != 0
    assert capsule.callback is module._drop_detached_references_capsule
    assert module.defer_prepared_finalizer_cleanup(capsule)
    module.drain_finalizer_cleanup()
    assert calls == 0


def test_remote_permit_ack_failure_retries_only_secondary_tail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify remote permit ack failure retries only secondary tail."""
    from schema_sanitizer.core_impl import finalizer_cleanup
    from schema_sanitizer.remote_impl.io_permits import RemoteIoPermitGovernor

    governor = RemoteIoPermitGovernor(1)
    permit = asyncio.run(governor.acquire(label="prepared-finalizer-ack-disarms-primary-before"))
    calls = 0
    real_release = governor._release_permit

    def counting_release(owner: object) -> None:
        """Record counting release for the enclosing assertion."""
        nonlocal calls
        calls += 1
        real_release(owner)  # type: ignore[arg-type]

    monkeypatch.setattr(governor, "_release_permit", counting_release)
    _fail_first_release_for(monkeypatch, finalizer_cleanup._PREPARED_FINALIZER_ESCROW)

    with pytest.raises(RuntimeError, match="acknowledgement did not commit"):
        permit.release()
    assert permit._released is True
    assert calls == 1

    permit.release()
    assert calls == 1
    assert permit._finalizer_ticket == 0


def test_provider_control_tail_failure_keeps_exact_owner_without_double_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify provider control tail failure keeps exact owner without double release."""
    from schema_sanitizer.remote_impl import provider_throttle as module

    governor = module.ProviderThrottleGovernor(max_tracked_keys=4)
    lease, _delay = governor.try_acquire("prepared-finalizer-ack-disarms-primary-before")
    assert lease is not None
    lease_entry = governor._active_leases[lease._lease_id]
    lease_ticket = lease_entry.control_ticket
    real_release = module.release_control_plane
    failed = False

    def flaky(ticket: object) -> bool:
        """Inject the flaky failure at the controlled test point."""
        nonlocal failed
        if ticket is lease_ticket and not failed:
            failed = True
            return False
        return bool(real_release(ticket))  # type: ignore[arg-type]

    monkeypatch.setattr(module, "release_control_plane", flaky)
    with pytest.raises(RuntimeError, match="control-plane retirement did not commit"):
        lease.release()

    assert governor._states["prepared-finalizer-ack-disarms-primary-before"].in_flight == 0
    assert governor._active_leases[lease._lease_id].resource_released is True
    assert lease._state == "active"

    lease.release()
    assert governor._states["prepared-finalizer-ack-disarms-primary-before"].in_flight == 0
    assert lease._lease_id not in governor._active_leases
    assert lease._state == "released"

    state = governor._states["prepared-finalizer-ack-disarms-primary-before"]
    assert flaky(state.control_ticket)
    state.control_ticket = None


def test_cross_process_exact_stale_authority_fails_closed() -> None:
    """Verify cross process exact stale authority fails closed."""
    from schema_sanitizer.core_impl.cross_process_memory import _ProcessCrossMemoryCoordinator

    coordinator = _ProcessCrossMemoryCoordinator(16 << 20)
    reservation = coordinator.acquire(64)
    token = reservation._token
    capability = reservation._capability
    original = reservation.reserved_bytes

    with pytest.raises(RuntimeError, match="not authoritative"):
        coordinator.resize(token, id(reservation) + 1, capability, original + 1)
    assert reservation.reserved_bytes == original
    assert coordinator._contributions[token] == original

    with pytest.raises(RuntimeError, match="not authoritative"):
        coordinator.release(token, id(reservation) + 1, capability)
    assert token in coordinator._contributions
    reservation.release()
    assert token not in coordinator._contributions


def test_path_claim_capacity_rollback_never_decrements_uncommitted_admission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify path claim capacity rollback never decrements uncommitted admission."""
    from schema_sanitizer.core_impl import path_identity as module

    baseline = module._PATH_CLAIM_ADMISSIONS
    escrow = module._PATH_CLAIM_FINALIZER_ESCROW
    before = escrow.published_count()
    monkeypatch.setattr(module, "_MAX_LIVE_PATH_CLAIMS", baseline)
    _fail_first_release_for(monkeypatch, escrow)

    with pytest.raises(OSError, match="path-claim capacity exhausted"):
        module._acquire_path_claim_admission()

    assert module._PATH_CLAIM_ADMISSIONS == baseline
    assert escrow.published_count() == before + 1
    module._drain_path_claim_finalizers(limit=1)
    assert escrow.published_count() == before


def test_quiescence_counters_do_not_use_saturating_decrements() -> None:
    """Verify quiescence counters do not use saturating decrements."""
    sources = {
        "janitor": SRC / "core_impl" / "temporary_janitor.py",
        "prefetch": SRC / "api_impl" / "source_plan" / "remote.py",
        "cleanup": SRC / "core_impl" / "cleanup_dispatcher.py",
        "retry": SRC / "core_impl" / "retry_scheduler.py",
        "provider_pool": SRC / "remote_impl" / "provider_session_pool.py",
        "remote_io": SRC / "remote_impl" / "io_coordinator.py",
    }
    text = {name: path.read_text(encoding="utf-8") for name, path in sources.items()}
    assert "self._quarantine_inflight = max(0" not in text["janitor"]
    assert "self._admissions_inflight = max(0" not in text["prefetch"]
    assert "self._cleanup_callbacks_inflight = max(0" not in text["prefetch"]
    assert "self._workers_starting = max(0" not in text["cleanup"]
    assert "self._execution_starting = max(0" not in text["retry"]
    assert "self._active_retries = max(0" not in text["retry"]
    assert "gate.users = max(0" not in text["provider_pool"]
    assert "self._submission_callbacks_inflight = max(" not in text["remote_io"]


def test_temporary_storage_replacement_cleanup_is_outside_pool_condition() -> None:
    """Verify temporary storage replacement cleanup is outside pool condition."""
    source = (SRC / "core_impl" / "temporary_storage.py").read_text(encoding="utf-8")
    start = source.index("    def _resize_lease(")
    end = source.index("\n    def ", start + 10)
    body = source[start:end]
    assert "entry.resize_replacement = replacement" in body
    assert "cleanup_replacement = entry.resize_replacement" in body
    cleanup = "_PROCESS_TEMPORARY_STORAGE.release_capability(cleanup_replacement)"
    assert cleanup in body
    assert body.index("cleanup_replacement = entry.resize_replacement") < body.index(cleanup)
