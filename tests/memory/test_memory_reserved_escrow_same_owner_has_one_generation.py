"""Regression coverage for memory reserved escrow same owner has one generation."""

from __future__ import annotations

from pathlib import Path
from threading import Lock

import pytest


def test_reserved_escrow_same_owner_has_one_generation() -> None:
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow
    from schema_sanitizer.core_impl.rooted_finalizer import RootedFinalizerAuthority

    owner = RootedFinalizerAuthority(lambda _owner: None)
    escrow: ReservedFinalizerEscrow[RootedFinalizerAuthority] = ReservedFinalizerEscrow(2)
    first = escrow.reserve_rooted(owner)
    assert first is not None
    # Lost-return retry of an unarmed RESERVED owner is idempotent.
    second = escrow.reserve_rooted(owner)
    assert second == first
    assert escrow.active_count() == 1

    assert escrow.publish_rooted(first, owner)
    assert owner._escrow_armed_ticket == first
    with pytest.raises(RuntimeError, match="already has an active generation"):
        escrow.reserve_rooted(owner)
    assert escrow.active_count() == 1

    assert escrow.process_one(lambda _ticket, value: value.run())
    assert escrow.active_count() == 0


def test_reserved_escrow_rollback_clears_stale_ticket_and_arm(monkeypatch) -> None:
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow
    from schema_sanitizer.core_impl.rooted_finalizer import RootedFinalizerAuthority

    escrow: ReservedFinalizerEscrow[RootedFinalizerAuthority] = ReservedFinalizerEscrow(1)
    owner = RootedFinalizerAuthority(lambda _owner: None)
    original = ReservedFinalizerEscrow._bump_progress
    failed = False
    stale_ticket = 0

    def interrupt(self: ReservedFinalizerEscrow[RootedFinalizerAuthority]) -> None:
        nonlocal failed, stale_ticket
        if self is escrow and not failed:
            failed = True
            stale_ticket = owner.ticket
            raise KeyboardInterrupt("reserved-escrow-same-owner-has-one reserve rollback")
        original(self)

    monkeypatch.setattr(ReservedFinalizerEscrow, "_bump_progress", interrupt)
    with pytest.raises(KeyboardInterrupt, match="reserve rollback"):
        escrow.reserve_rooted(owner)

    assert owner.ticket == 0
    assert owner._escrow_armed_ticket == 0
    assert stale_ticket > 0
    assert escrow.publish_rooted(stale_ticket, owner)
    assert not escrow.overflowed
    snap = escrow.capacity_snapshot()
    assert snap.active == 0
    assert snap.invariant_ok


def test_recycle_pending_is_recoverable_capacity_not_overflow(monkeypatch) -> None:
    import schema_sanitizer.core_impl.finalizer_escrow as module
    from schema_sanitizer.core_impl.rooted_finalizer import RootedFinalizerAuthority

    escrow: module.ReservedFinalizerEscrow[RootedFinalizerAuthority] = (
        module.ReservedFinalizerEscrow(1)
    )
    owner = RootedFinalizerAuthority(lambda _owner: None)
    ticket = escrow.reserve_rooted(owner)
    assert ticket is not None
    assert escrow.publish_rooted(ticket, owner)

    original = module.ReservedFinalizerEscrow._recycle_one_pending_locked

    def interrupt(self: module.ReservedFinalizerEscrow[RootedFinalizerAuthority]) -> bool:
        if self is escrow:
            raise KeyboardInterrupt("reserved-escrow-same-owner-has-one recycle")
        return original(self)

    monkeypatch.setattr(module.ReservedFinalizerEscrow, "_recycle_one_pending_locked", interrupt)
    assert escrow.process_one(lambda _ticket, value: value.run())

    snap = escrow.capacity_snapshot()
    assert snap.active == 0
    assert snap.available == 0
    assert snap.recycle_pending == 1
    assert not snap.overflowed
    assert snap.invariant_ok


def test_path_claim_admission_finalizer_is_replay_idempotent() -> None:
    from schema_sanitizer.core_impl import path_identity as module

    before = module._PATH_CLAIM_ADMISSION_OWNERS.exact_active_count()
    admission = module._acquire_path_claim_admission()
    authority = admission.finalizer_owner
    assert module._PATH_CLAIM_ADMISSION_OWNERS.exact_active_count() == before + 1

    module._run_path_claim_admission_finalizer(authority)
    assert module._PATH_CLAIM_ADMISSION_OWNERS.exact_active_count() == before
    # Simulate callback replay before process_one could publish PROCESSED.
    module._run_path_claim_admission_finalizer(authority)
    assert module._PATH_CLAIM_ADMISSION_OWNERS.exact_active_count() == before

    admission.counted = False
    admission.released = True
    module._PATH_CLAIM_FINALIZER_ESCROW.release_rooted_owner(authority)


def test_operation_memory_exact_release_replay_is_ack(monkeypatch) -> None:
    from schema_sanitizer.core_impl import memory_budget as module

    ledger = object.__new__(module.OperationMemoryLedger)
    ledger._lock = Lock()
    ledger._python_leases = {}
    ledger._unknown_python_lease_releases = 0
    ledger._post_release_observation_failures = 0
    ledger._cross_process_release_deferred = False
    capability = module.FinalizerReplayCapability()
    lease_id = 17
    owner_id = 23
    entry = module._PythonMemoryLeaseEntry(
        owner_id,
        capability,
        0,
        None,
        physical_released=True,
        physical_size_bytes=0,
        native_receipt=None,
    )
    ledger._python_leases[lease_id] = entry
    monkeypatch.setattr(
        module.OperationMemoryLedger, "_maybe_finish_deferred_close", lambda self: None
    )
    monkeypatch.setattr(
        module.OperationMemoryLedger, "_schedule_deferred_close_cleanup_noexcept", lambda self: None
    )

    ledger._release_python_lease_authority(lease_id, owner_id, capability)
    assert capability.released
    assert lease_id not in ledger._python_leases
    # Same exact capability is an ACK, not an unknown/double release.
    ledger._release_python_lease_authority(lease_id, owner_id, capability)
    assert ledger._unknown_python_lease_releases == 0


def test_temporary_storage_exact_release_replay_is_ack(monkeypatch, tmp_path: Path) -> None:
    from schema_sanitizer.core_impl import temporary_storage as module

    pool = module.TemporaryStoragePermitPool(None)
    capability = module.FinalizerReplayCapability()
    lease_id = 9
    owner_id = 11
    entry = module._StorageLeaseEntry(
        owner_id,
        capability,
        0,
        0,
        tmp_path,
        0,
        module.ProcessTemporaryStorageCapability(module._PROCESS_TEMPORARY_STORAGE),
        None,
        process_released=True,
        local_released=True,
    )
    pool._leases[lease_id] = entry
    monkeypatch.setattr(
        module.TemporaryStoragePermitPool, "_finish_pending_resize_locked", lambda self, entry: None
    )

    pool._release_lease_authority(lease_id, owner_id, capability)
    assert capability.released
    assert lease_id not in pool._leases
    pool._release_lease_authority(lease_id, owner_id, capability)
    assert pool._unknown_lease_releases == 0


def test_direct_cross_memory_exact_release_replay_is_ack() -> None:
    from schema_sanitizer.core_impl import cross_process_memory as module

    owner = object()
    free_before = module._DIRECT_LEASE_FREE_COUNT
    registration = module._register_direct_lease(owner)
    lease_id = registration.lease_id
    capability = registration.capability
    module._update_direct_lease_reserved(owner, lease_id, capability, 123)

    assert module._retire_direct_lease_authority(id(owner), lease_id, capability) == 123
    assert capability.released
    assert module._DIRECT_LEASE_FREE_COUNT == free_before
    unknown_before = module._DIRECT_LEASE_UNKNOWN_RELEASES
    assert module._retire_direct_lease_authority(id(owner), lease_id, capability) == 0
    assert module._DIRECT_LEASE_UNKNOWN_RELEASES == unknown_before


def test_finalizer_admission_snapshot_counts_recycle_pending(monkeypatch) -> None:
    import schema_sanitizer.core_impl.finalizer_escrow as module
    from schema_sanitizer.core_impl.finalizer_admission import _domain
    from schema_sanitizer.core_impl.rooted_finalizer import RootedFinalizerAuthority

    escrow: module.ReservedFinalizerEscrow[RootedFinalizerAuthority] = (
        module.ReservedFinalizerEscrow(1)
    )
    owner = RootedFinalizerAuthority(lambda _owner: None)
    ticket = escrow.reserve_rooted(owner)
    assert ticket is not None
    assert escrow.publish_rooted(ticket, owner)

    original = module.ReservedFinalizerEscrow._recycle_one_pending_locked
    monkeypatch.setattr(
        module.ReservedFinalizerEscrow,
        "_recycle_one_pending_locked",
        lambda self: (
            (_ for _ in ()).throw(KeyboardInterrupt("pending"))
            if self is escrow
            else original(self)
        ),
    )
    assert escrow.process_one(lambda _ticket, value: value.run())
    domain = _domain("reserved-escrow-same-owner-has-one", escrow)  # type: ignore[arg-type]
    assert domain.recycle_pending == 1
    assert domain.invariant_ok


def test_source_contract_replay_capabilities_and_exact_arms() -> None:
    root = Path(__file__).resolve().parents[2] / "src/schema_sanitizer/core_impl"
    escrow = (root / "finalizer_escrow.py").read_text(encoding="utf-8")
    rooted = (root / "rooted_finalizer.py").read_text(encoding="utf-8")
    memory = (root / "memory_budget.py").read_text(encoding="utf-8")
    storage = (root / "temporary_storage.py").read_text(encoding="utf-8")
    cross = (root / "cross_process_memory.py").read_text(encoding="utf-8")
    path = (root / "path_identity.py").read_text(encoding="utf-8")

    assert "is_armed_for" in rooted
    assert "reserved finalizer owner already has an active generation" in escrow
    assert "recycle_pending" in escrow
    assert "FinalizerReplayCapability()" in memory
    assert "FinalizerReplayCapability()" in storage
    assert "FinalizerReplayCapability()" in cross
    assert "_PATH_CLAIM_ADMISSION_OWNERS = BoundedGenerationPool" in path
    assert "_PATH_CLAIM_ADMISSION_OWNERS_FORK_FRESH" in path


def test_default_control_plane_capacity_leaves_dynamic_headroom() -> None:
    from schema_sanitizer.core_impl.control_plane_budget import (
        _DEFAULT_CAPACITY_BYTES,
        _MAX_CAPACITY_BYTES,
        process_control_plane_snapshot,
        release_control_plane,
        reserve_control_plane,
    )

    snapshot = process_control_plane_snapshot()
    assert snapshot.static_baseline_bytes < _DEFAULT_CAPACITY_BYTES
    assert snapshot.capacity_bytes == _DEFAULT_CAPACITY_BYTES
    assert _DEFAULT_CAPACITY_BYTES <= _MAX_CAPACITY_BYTES

    ticket = reserve_control_plane("reserved-escrow-same-owner-has-one_default_headroom", 256)
    assert release_control_plane(ticket)
