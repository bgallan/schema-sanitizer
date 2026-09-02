"""Tests owner-first publication through interrupted acquire, store handoff, post-commit
release retry, reserved-escrow failures, mirror loss, and production finalizer handoff.
Ownership is rooted before ticket visibility, rollback restores a publishable owner, and
exact physical or logical slots retire even when dictionary mirrors disappear."""

from __future__ import annotations

from pathlib import Path

import pytest


def _root() -> Path:
    """Return the repository root used by source-contract checks."""
    return Path(__file__).resolve().parents[2]


class _Owner:
    def __init__(self) -> None:
        """Initialize the owner test double."""
        self.ticket = 0
        self._escrow_armed_ticket = 0

    def arm_for_ticket(self, ticket: int) -> None:
        """Record the finalizer ticket currently armed on this owner."""
        self._escrow_armed_ticket = int(ticket)

    def disarm_ticket(self, ticket: int | None = None) -> None:
        """Clear the armed ticket when it matches the requested ticket."""
        if ticket is None or self._escrow_armed_ticket == int(ticket):
            self._escrow_armed_ticket = 0


def test_generation_owner_first_acquire_rolls_back_after_internal_interrupt() -> None:
    """Verify generation owner first acquire rolls back after internal interrupt."""
    from schema_sanitizer.core_impl.bounded_generation import BoundedGenerationPool

    pool = BoundedGenerationPool(1)
    owner = object()

    class InsertThenInterrupt(dict[int, int]):
        """Interrupt after the derived hint is inserted post-authority."""

        def __setitem__(self, key: int, value: int) -> None:
            """Insert the hint, then interrupt the owner-first commit tail."""
            super().__setitem__(key, value)
            assert pool._owners[value] is owner
            raise KeyboardInterrupt("generation-owner-first-acquire-rolls-back acquire post-owner")

    pool._owner_slots = InsertThenInterrupt()
    with pytest.raises(KeyboardInterrupt, match="post-owner"):
        pool.acquire_for(owner)

    assert not pool.owns_owner(owner)
    assert pool.exact_active_count() == 0
    snap = pool.snapshot()
    assert snap.active == 0
    assert snap.available == 1


def test_generation_owner_assignment_commit_rolls_back_before_flag_store() -> None:
    """Roll back when exact owner assignment commits before its local flag."""
    from schema_sanitizer.core_impl.bounded_generation import BoundedGenerationPool

    pool = BoundedGenerationPool(1)
    owner = object()

    class AssignThenInterrupt(list[object | None]):
        """Interrupt after the authoritative owner list accepts the owner."""

        failed = False

        def __setitem__(self, key: int, value: object | None) -> None:
            """Commit the exact owner assignment, then interrupt once."""
            super().__setitem__(key, value)
            if value is owner and not self.failed:
                self.failed = True
                raise KeyboardInterrupt("generation-owner-assignment postcommit")

    pool._owners = AssignThenInterrupt(pool._owners)
    with pytest.raises(KeyboardInterrupt, match="owner-assignment postcommit"):
        pool.acquire_for(owner)

    assert all(candidate is not owner for candidate in pool._owners)
    assert not pool.owns_owner(owner)
    assert pool.exact_active_count() == 0
    snap = pool.snapshot()
    assert snap.active == 0
    assert snap.available == 1


def test_generation_owner_identity_closes_return_to_store_handoff_gap() -> None:
    """Verify generation owner identity closes return to store handoff gap."""
    from schema_sanitizer.core_impl.bounded_generation import BoundedGenerationPool

    pool = BoundedGenerationPool(1)
    owner = object()
    with pytest.raises(KeyboardInterrupt, match="handoff"):
        try:
            # Simulate an async exception after CALL acquire_for returned but
            # before a caller could durably publish/store the integer token.
            pool.acquire_for(owner)
            raise KeyboardInterrupt("generation-owner-first-acquire-rolls-back handoff")
        except BaseException:
            # Cleanup does not need the lost integer; exact owner identity is enough.
            assert pool.release_for(owner)
            raise

    assert pool.exact_active_count() == 0
    assert pool.snapshot().available == 1


def test_generation_release_postcommit_is_retry_idempotent() -> None:
    """Verify generation release postcommit is retry idempotent."""
    from schema_sanitizer.core_impl.bounded_generation import BoundedGenerationPool

    pool = BoundedGenerationPool(1)
    owner = object()
    token = pool.acquire_for(owner)
    assert token is not None

    class PopThenInterrupt(dict[int, int]):
        """Interrupt after removing the derived hint post-retirement."""

        def pop(self, key: int, default: object = None) -> int | object:
            """Remove the hint, then interrupt after exact owner retirement."""
            super().pop(key, default)
            assert owner not in pool._owners
            raise KeyboardInterrupt("generation-owner-first-acquire-rolls-back release postcommit")

    pool._owner_slots = PopThenInterrupt(pool._owner_slots)
    with pytest.raises(KeyboardInterrupt, match="postcommit"):
        pool.release_for(owner)

    assert not pool.owns_owner(owner)
    # A retry by identity observes the retirement as already committed.
    assert pool.release_for(owner)
    assert pool.exact_active_count() == 0
    assert pool.snapshot().available == 1


def test_generation_normal_path_never_scans_capacity(monkeypatch) -> None:
    """Keep ordinary generation acquisition and release on derived O(1) structures."""
    from schema_sanitizer.core_impl.bounded_generation import BoundedGenerationPool

    pool = BoundedGenerationPool(8_192)

    def unexpected_rebuild(_self: BoundedGenerationPool) -> None:
        """Fail if a clean normal-path operation attempts full reconstruction."""
        raise AssertionError("clean bounded-generation path rebuilt capacity")

    monkeypatch.setattr(BoundedGenerationPool, "_rebuild_derived_from_owners", unexpected_rebuild)
    for _ in range(32):
        owner = object()
        assert pool.acquire_for(owner) is not None
        assert pool.release_for(owner)


def test_generation_dirty_identity_hint_rebuilds_from_exact_authority() -> None:
    """Recover a lost derived identity hint without rekeying its exact owner."""
    from schema_sanitizer.core_impl.bounded_generation import BoundedGenerationPool

    pool = BoundedGenerationPool(4)
    owner = object()
    token = pool.acquire_for(owner)
    assert token is not None
    pool._owner_slots.clear()
    pool._states[:] = b"\x00" * 4
    pool._derived_dirty = True

    assert pool.owns_owner(owner, token)
    assert pool.snapshot().active == 1
    assert pool.release_for(owner)


def test_reserved_escrow_owner_first_reservation_rolls_back_without_ticket_handoff(
    monkeypatch,
) -> None:
    """Verify reserved escrow owner first reservation rolls back without ticket handoff."""
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    escrow: ReservedFinalizerEscrow[_Owner] = ReservedFinalizerEscrow(1)
    owner = _Owner()
    original = ReservedFinalizerEscrow._bump_progress
    failed = False

    def flaky(self: ReservedFinalizerEscrow[_Owner]) -> None:
        """Inject the flaky failure at the controlled test point."""
        nonlocal failed
        if self is escrow and not failed:
            failed = True
            raise KeyboardInterrupt("generation-owner-first-acquire-rolls-back rooted reservation")
        original(self)

    monkeypatch.setattr(ReservedFinalizerEscrow, "_bump_progress", flaky)
    with pytest.raises(KeyboardInterrupt, match="rooted reservation"):
        escrow.reserve_rooted(owner)

    assert escrow.active_count() == 0
    assert escrow.reserved_count() == 0
    assert escrow.release_rooted_owner(owner)


def test_reserved_escrow_claim_failure_restores_publishable_owner() -> None:
    """Verify reserved escrow claim failure restores publishable owner."""
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    escrow: ReservedFinalizerEscrow[_Owner] = ReservedFinalizerEscrow(1)
    owner = _Owner()
    ticket = escrow.reserve_rooted(owner)
    assert ticket is not None
    owner.arm_for_ticket(ticket)

    calls = 0

    def interrupt(_ticket: int, value: _Owner) -> None:
        """Inject the interruption at the controlled handoff point."""
        nonlocal calls
        calls += 1
        assert value is owner
        raise KeyboardInterrupt("generation-owner-first-acquire-rolls-back claimed")

    with pytest.raises(KeyboardInterrupt, match="claimed"):
        escrow.process_one(interrupt)
    assert calls == 1
    # CLAIMED must have been restored to a processable state.
    assert escrow.active_count() == 1

    seen: list[_Owner] = []
    assert escrow.process_one(lambda _ticket, value: seen.append(value))
    assert seen == [owner]
    assert escrow.active_count() == 0


def test_reserved_escrow_processed_marker_prevents_callback_replay(monkeypatch) -> None:
    """Verify reserved escrow processed marker prevents callback replay."""
    import schema_sanitizer.core_impl.finalizer_escrow as module

    escrow: module.ReservedFinalizerEscrow[_Owner] = module.ReservedFinalizerEscrow(1)
    owner = _Owner()
    ticket = escrow.reserve_rooted(owner)
    assert ticket is not None
    owner.arm_for_ticket(ticket)

    original = module.ReservedFinalizerEscrow._bump_progress
    failed = False
    calls = 0

    def flaky(self: module.ReservedFinalizerEscrow[_Owner]) -> None:
        """Inject the flaky failure at the controlled test point."""
        nonlocal failed
        if self is escrow and not failed and module._PROCESSED in self._states:
            failed = True
            raise KeyboardInterrupt(
                "generation-owner-first-acquire-rolls-back processed bookkeeping"
            )
        original(self)

    def processor(_ticket: int, value: _Owner) -> None:
        """Process the queued owner through the controlled path."""
        nonlocal calls
        calls += 1
        assert value is owner

    monkeypatch.setattr(module.ReservedFinalizerEscrow, "_bump_progress", flaky)
    with pytest.raises(KeyboardInterrupt, match="processed bookkeeping"):
        escrow.process_one(processor)
    assert calls == 1
    assert module._PROCESSED in escrow._states

    # A safe point retires PROCESSED without invoking processor again.
    assert escrow.process_one(processor)
    assert calls == 1
    assert escrow.active_count() == 0


def test_physical_claim_target_zero_retires_slot_even_after_dict_mirror_was_lost() -> None:
    """Verify physical claim target zero retires slot even after dict mirror was lost."""
    from schema_sanitizer.core_impl import process_resources as module

    module.drain_finalizer_cleanup()
    key = ("declared", ("generation-owner-first-acquire-rolls-back", "physical-lost-mirror"))
    before = module._EXTERNAL_RUNTIME_CLAIM_SLOTS.exact_active_count()
    permit = module._SharedExternalRuntimeNativePermit(key, 0)
    claim_id = module._EXTERNAL_RUNTIME_CLAIM_SLOTS.acquire_for(permit)
    assert claim_id is not None
    permit._bind_claim_id(claim_id)
    module._EXTERNAL_RUNTIME_POOL_COORDINATOR[key] = module._ExternalRuntimePoolCoordinatorEntry(
        runtime=None, runtime_key=key, physical_amount=0, physical_claims={}
    )
    assert module._EXTERNAL_RUNTIME_CLAIM_SLOTS.exact_active_count() == before + 1

    permit.release()
    assert module._EXTERNAL_RUNTIME_CLAIM_SLOTS.exact_active_count() == before
    assert not module._EXTERNAL_RUNTIME_CLAIM_SLOTS.owns_owner(permit)


def test_logical_claim_target_zero_retires_slot_even_after_dict_mirror_was_lost() -> None:
    """Verify logical claim target zero retires slot even after dict mirror was lost."""
    from schema_sanitizer.core_impl import process_resources as module

    module.drain_finalizer_cleanup()
    key = ("declared", ("generation-owner-first-acquire-rolls-back", "logical-lost-mirror"))
    before = module._EXTERNAL_RUNTIME_CLAIM_SLOTS.exact_active_count()
    claim = module._SharedExternalRuntimeLogicalLease(key, 0, 1)
    claim_id = module._EXTERNAL_RUNTIME_CLAIM_SLOTS.acquire_for(claim)
    assert claim_id is not None
    claim._bind_claim_id(claim_id)
    module._EXTERNAL_RUNTIME_POOL_COORDINATOR[key] = module._ExternalRuntimePoolCoordinatorEntry(
        runtime=None, runtime_key=key, logical_width=0, logical_claims={}
    )
    assert module._EXTERNAL_RUNTIME_CLAIM_SLOTS.exact_active_count() == before + 1

    claim.release()
    assert module._EXTERNAL_RUNTIME_CLAIM_SLOTS.exact_active_count() == before
    assert not module._EXTERNAL_RUNTIME_CLAIM_SLOTS.owns_owner(claim)


def test_production_generation_consumers_are_owner_first() -> None:
    """Verify production generation consumers are owner first."""
    root = _root() / "src/schema_sanitizer"
    expectations = {
        root / "core_impl/process_resources.py": "_EXTERNAL_RUNTIME_CLAIM_SLOTS.acquire_for(",
        root / "core_impl/runtime_registry.py": "_token_pool.acquire_for(entry)",
        root / "core_impl/retry_scheduler.py": "_generation_pool.acquire_for(item)",
        root / "core_impl/cross_process_memory.py": "_generation_pool.acquire_for(",
    }
    for path, marker in expectations.items():
        source = path.read_text(encoding="utf-8")
        assert marker in source

    resources = (root / "core_impl/process_resources.py").read_text(encoding="utf-8")
    assert "_EXTERNAL_RUNTIME_CLAIM_SLOTS.acquire()" not in resources
    registry = (root / "core_impl/runtime_registry.py").read_text(encoding="utf-8")
    assert "_token_pool.acquire()" not in registry
    cross = (root / "core_impl/cross_process_memory.py").read_text(encoding="utf-8")
    assert "_generation_pool.acquire()" not in cross


def test_production_rooted_finalizers_reserve_owner_before_ticket_handoff() -> None:
    """Verify production rooted finalizers reserve owner before ticket handoff."""
    root = _root() / "src/schema_sanitizer"
    paths = (
        root / "core_impl/memory_budget.py",
        root / "core_impl/temporary_storage.py",
        root / "core_impl/cross_process_memory.py",
        root / "core_impl/path_identity.py",
        root / "api_impl/operation_context.py",
        root / "pipeline/partition_lookahead.py",
    )
    for path in paths:
        source = path.read_text(encoding="utf-8")
        assert ".reserve_rooted(" in source
        # Every construction roots authority in the same operation.
        assert "reserve_ticket()\n" not in source
