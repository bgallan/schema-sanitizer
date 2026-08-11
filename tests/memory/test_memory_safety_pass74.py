from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src" / "schema_sanitizer"


def _fail_first_release_for(monkeypatch: pytest.MonkeyPatch, target: object) -> None:
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    original = ReservedFinalizerEscrow.release_ticket
    failed = False

    def flaky(self: object, ticket: int) -> bool:
        nonlocal failed
        if self is target and not failed:
            failed = True
            return False
        return original(self, ticket)  # type: ignore[arg-type]

    monkeypatch.setattr(ReservedFinalizerEscrow, "release_ticket", flaky)


def test_prepared_finalizer_pre_root_survives_publish_lock_contention() -> None:
    from schema_sanitizer.core_impl import finalizer_cleanup as module

    escrow = module._PREPARED_FINALIZER_ESCROW
    baseline_active = escrow.active_count()
    baseline_published = escrow.published_count()
    calls = 0

    def cleanup(_capsule: object) -> None:
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

    assert module.drain_finalizer_cleanup() >= 1
    assert calls == 1
    assert escrow.active_count() <= baseline_active


def test_prepared_cancel_failure_transfers_ack_without_primary_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import finalizer_cleanup as module

    calls = 0

    def primary(_capsule: object) -> None:
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
    from schema_sanitizer.core_impl.control_plane_budget import (
        ControlPlaneTicket,
        _ProcessControlPlaneBudget,
    )

    budget = _ProcessControlPlaneBudget(include_static_baseline=False)
    ticket = budget.reserve("pass74_stale", 256)
    stale = ControlPlaneTicket(
        amount=ticket.amount,
        kind=ticket.kind,
        pid=ticket.pid,
        token=ticket.token,
        capability=object(),
    )

    assert budget.release(stale) is False
    assert ticket.released is False
    assert budget._owners[ticket.token][0] is ticket
    assert budget.snapshot().reserved_bytes >= 256

    assert budget.release(ticket) is True
    assert ticket.released is True
    assert ticket.token not in budget._owners


def test_control_plane_failed_tail_can_be_retried_from_ledger_root() -> None:
    from schema_sanitizer.core_impl.control_plane_budget import _ProcessControlPlaneBudget

    budget = _ProcessControlPlaneBudget(include_static_baseline=False)
    ticket = budget.reserve("pass74_deferred", 256)
    assert budget.request_retirement(ticket)
    assert budget._owners[ticket.token][0] is ticket
    assert ticket.retire_requested is True

    assert budget.drain_requested_retirements(limit=1) == 1
    assert ticket.released is True
    assert ticket.token not in budget._owners


def test_temporary_storage_governor_users_underflow_pins_device_identity() -> None:
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
    assert owner._escrow_armed is False

    del reservation
    gc.collect()
    assert owner._escrow_armed is True
    coordinator.reconcile_pending()
    assert token not in coordinator._contributions


def test_pass74_authoritative_quiescence_counters_do_not_saturate() -> None:
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


def test_pass74_shutdown_observes_remote_waiters_protocol_and_freeze() -> None:
    source = (SRC / "core_impl" / "runtime_shutdown.py").read_text(encoding="utf-8")
    assert "close_remote_io_permit_admission" in source
    assert "remote_io_waiting" in source
    assert "remote_io_sync_waiters" in source
    assert "remote_io_protocol_violations" in source
    assert '"remote_io:protocol_violation"' in source


def test_pass74_io_wrappers_use_ack_after_primary_release_and_safe_defer() -> None:
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
