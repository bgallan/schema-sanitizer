"""Regression coverage for memory generation owner first acquire rolls back after internal interrupt."""

from __future__ import annotations

from pathlib import Path

import pytest


def _root() -> Path:
    return Path(__file__).resolve().parents[2]


class _Owner:
    def __init__(self) -> None:
        self.ticket = 0
        self._escrow_armed_ticket = 0

    def arm_for_ticket(self, ticket: int) -> None:
        self._escrow_armed_ticket = int(ticket)

    def disarm_ticket(self, ticket: int | None = None) -> None:
        if ticket is None or self._escrow_armed_ticket == int(ticket):
            self._escrow_armed_ticket = 0


def test_generation_owner_first_acquire_rolls_back_after_internal_interrupt(monkeypatch) -> None:
    from schema_sanitizer.core_impl.bounded_generation import BoundedGenerationPool

    pool = BoundedGenerationPool(1)
    owner = object()
    original = BoundedGenerationPool._rebuild_derived_from_owners
    calls = 0

    def flaky(self: BoundedGenerationPool) -> None:
        nonlocal calls
        calls += 1
        if self is pool and calls == 2:
            raise KeyboardInterrupt("generation-owner-first-acquire-rolls-back acquire post-owner")
        original(self)

    monkeypatch.setattr(BoundedGenerationPool, "_rebuild_derived_from_owners", flaky)
    with pytest.raises(KeyboardInterrupt, match="post-owner"):
        pool.acquire_for(owner)

    assert not pool.owns_owner(owner)
    assert pool.exact_active_count() == 0
    snap = pool.snapshot()
    assert snap.active == 0
    assert snap.available == 1


def test_generation_owner_identity_closes_return_to_store_handoff_gap() -> None:
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


def test_generation_release_postcommit_is_retry_idempotent(monkeypatch) -> None:
    from schema_sanitizer.core_impl.bounded_generation import BoundedGenerationPool

    pool = BoundedGenerationPool(1)
    owner = object()
    token = pool.acquire_for(owner)
    assert token is not None

    original = BoundedGenerationPool._rebuild_derived_from_owners
    calls = 0

    def flaky(self: BoundedGenerationPool) -> None:
        nonlocal calls
        calls += 1
        if self is pool and calls == 2:
            raise KeyboardInterrupt("generation-owner-first-acquire-rolls-back release postcommit")
        original(self)

    monkeypatch.setattr(BoundedGenerationPool, "_rebuild_derived_from_owners", flaky)
    with pytest.raises(KeyboardInterrupt, match="postcommit"):
        pool.release_for(owner)

    assert not pool.owns_owner(owner)
    # A retry by identity observes the retirement as already committed.
    assert pool.release_for(owner)
    assert pool.exact_active_count() == 0
    assert pool.snapshot().available == 1


def test_reserved_escrow_owner_first_reservation_rolls_back_without_ticket_handoff(
    monkeypatch,
) -> None:
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    escrow: ReservedFinalizerEscrow[_Owner] = ReservedFinalizerEscrow(1)
    owner = _Owner()
    original = ReservedFinalizerEscrow._bump_progress
    failed = False

    def flaky(self: ReservedFinalizerEscrow[_Owner]) -> None:
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
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    escrow: ReservedFinalizerEscrow[_Owner] = ReservedFinalizerEscrow(1)
    owner = _Owner()
    ticket = escrow.reserve_rooted(owner)
    assert ticket is not None
    owner.arm_for_ticket(ticket)

    calls = 0

    def interrupt(_ticket: int, value: _Owner) -> None:
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
        nonlocal failed
        if self is escrow and not failed and module._PROCESSED in self._states:
            failed = True
            raise KeyboardInterrupt(
                "generation-owner-first-acquire-rolls-back processed bookkeeping"
            )
        original(self)

    def processor(_ticket: int, value: _Owner) -> None:
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
