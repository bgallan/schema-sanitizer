from __future__ import annotations

import ast
import gc
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src" / "schema_sanitizer"


def test_orphaned_prepared_capsule_can_die_and_arm_separate_root() -> None:
    from schema_sanitizer.core_impl import finalizer_cleanup as module

    escrow = module._PREPARED_FINALIZER_ESCROW
    baseline = escrow.active_count()
    calls: list[int] = []

    capsule = module.reserve_finalizer_cleanup(lambda _owner: calls.append(1))
    ticket = capsule.ticket
    authority = capsule._authority
    slot = escrow._ticket_slots[ticket]
    assert escrow._slots[slot] is authority
    assert escrow._slots[slot] is not capsule

    del capsule
    gc.collect()

    assert authority._escrow_armed is True
    assert escrow.active_count() == baseline + 1
    assert module.drain_finalizer_cleanup() >= 1
    assert calls == [1]
    assert escrow.active_count() <= baseline


def test_cancel_release_exception_is_irreversibly_ack_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import finalizer_cleanup as module
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    calls: list[int] = []
    capsule = module.reserve_finalizer_cleanup(lambda _owner: calls.append(1))
    target = module._PREPARED_FINALIZER_ESCROW
    original = ReservedFinalizerEscrow.release_ticket
    failed = False

    def raising(self: object, ticket: int) -> bool:
        nonlocal failed
        if self is target and not failed:
            failed = True
            raise MemoryError("pass75 injected retirement fault")
        return original(self, ticket)  # type: ignore[arg-type]

    monkeypatch.setattr(ReservedFinalizerEscrow, "release_ticket", raising)
    module.cancel_prepared_finalizer_cleanup(capsule)
    assert capsule.ticket == 0
    module.drain_finalizer_cleanup()
    assert calls == []


def test_aggregate_cross_memory_rejection_does_not_publish_false_overflow() -> None:
    import schema_sanitizer.core_impl.cross_process_memory as module
    from schema_sanitizer.errors import SchemaSanitizerResourceError

    coordinator = module._ProcessCrossMemoryCoordinator(16 << 20)
    before = module._PROCESS_FINALIZER_RELEASE_OVERFLOWS
    try:
        with pytest.raises(SchemaSanitizerResourceError):
            coordinator.acquire(20 << 20)
        assert coordinator._contributions == {}
        assert coordinator._finalizer_releases.active_count() == 0
        assert module._PROCESS_FINALIZER_RELEASE_OVERFLOWS == before
    finally:
        coordinator._physical.release()


def test_aggregate_cross_memory_fallback_is_ack_before_publication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl.cross_process_memory import _ProcessCrossMemoryCoordinator
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    coordinator = _ProcessCrossMemoryCoordinator(16 << 20)
    reservation = coordinator.acquire(0)
    owner = reservation._finalizer_owner
    ticket = reservation._finalizer_ticket
    coordinator.release(
        reservation._token, id(reservation), reservation._capability, nonblocking=False
    )
    assert owner._primary_released is False

    original = ReservedFinalizerEscrow.release_ticket
    failed = False

    def flaky(self: object, value: int) -> bool:
        nonlocal failed
        if self is coordinator._finalizer_releases and not failed:
            failed = True
            return False
        return original(self, value)  # type: ignore[arg-type]

    monkeypatch.setattr(ReservedFinalizerEscrow, "release_ticket", flaky)
    assert coordinator.release_finalizer_ticket(ticket, owner=owner)
    assert owner._primary_released is True
    reservation._released = True
    reservation._reserved = 0
    reservation._finalizer_ticket = -1
    coordinator.reconcile_pending()
    assert coordinator._finalizer_releases.active_count() == 0
    coordinator._physical.release()


def test_bounded_generation_underflow_pins_namespace_corrupted() -> None:
    from schema_sanitizer.core_impl.bounded_generation import BoundedGenerationPool

    pool = BoundedGenerationPool(2)
    token = pool.acquire()
    assert token is not None
    pool._active = 0
    assert pool.release(token) is False
    snapshot = pool.snapshot()
    assert snapshot.corrupted is True
    assert snapshot.available == 1
    assert pool.acquire() is None


def test_control_plane_deferred_drain_reconciles_dirty_authoritative_counters() -> None:
    from schema_sanitizer.core_impl.control_plane_budget import _ProcessControlPlaneBudget

    budget = _ProcessControlPlaneBudget(include_static_baseline=False)
    ticket = budget.reserve("pass75_dirty", 512)
    assert budget.request_retirement(ticket)
    budget._reserved = 0
    budget._active = 0
    budget._counters_dirty = True

    assert budget.drain_requested_retirements(limit=1) == 1
    assert ticket.released is True
    assert ticket.token not in budget._owners


def test_path_claim_escrow_roots_authority_not_destructed_owner() -> None:
    import schema_sanitizer.core_impl.path_identity as module

    baseline = module._PATH_CLAIM_FINALIZER_ESCROW.active_count()
    owner = module.PathClaimOwner(None, None, None)
    ticket = owner.finalizer_ticket
    authority = owner.finalizer_owner
    assert authority is not None
    slot = module._PATH_CLAIM_FINALIZER_ESCROW._ticket_slots[ticket]
    assert module._PATH_CLAIM_FINALIZER_ESCROW._slots[slot] is authority
    assert module._PATH_CLAIM_FINALIZER_ESCROW._slots[slot] is not owner

    del owner
    gc.collect()
    assert authority._escrow_armed is True
    assert module._drain_path_claim_finalizers(limit=8) >= 1
    assert module._PATH_CLAIM_FINALIZER_ESCROW.active_count() <= baseline


def test_direct_cross_memory_escrow_roots_authority_not_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import schema_sanitizer.core_impl.cross_process_memory as module

    escrow = module.ReservedFinalizerEscrow(8)
    monkeypatch.setattr(module, "_DIRECT_CROSS_MEMORY_FINALIZER_ESCROW", escrow)
    baseline = escrow.active_count()
    lease = module.CrossProcessMemoryLease(16 << 20, 0)
    ticket = lease._finalizer_ticket
    authority = lease._finalizer_owner
    assert authority is not None
    slot = escrow._ticket_slots[ticket]
    assert escrow._slots[slot] is authority
    assert escrow._slots[slot] is not lease

    del lease
    gc.collect()
    assert authority._escrow_armed is True
    assert module.drain_direct_cross_process_memory_finalizers() >= 1
    assert escrow.active_count() <= baseline


def test_specialized_finalizer_destructors_never_block_or_publish_self() -> None:
    paths = [
        SRC / "core_impl" / "memory_budget.py",
        SRC / "core_impl" / "temporary_storage.py",
        SRC / "core_impl" / "cross_process_memory.py",
        SRC / "core_impl" / "path_identity.py",
        SRC / "api_impl" / "operation_context.py",
        SRC / "pipeline" / "partition_lookahead.py",
    ]
    for path in paths:
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "__del__":
                body = ast.get_source_segment(source, node) or ""
                assert "release_ticket(" not in body, f"blocking release in {path}:{node.lineno}"
                assert "publish_reserved(ticket, self" not in body


def test_pass75_admission_byte_counters_never_saturate_downward() -> None:
    temporary = (SRC / "core_impl" / "temporary_storage.py").read_text(encoding="utf-8")
    retry = (SRC / "core_impl" / "retry_scheduler.py").read_text(encoding="utf-8")
    bounded = (SRC / "core_impl" / "bounded_generation.py").read_text(encoding="utf-8")

    assert "_pending_resize_growth = max(0" not in temporary
    for name in ("_pending_bytes", "_ready_bytes", "_emergency_bytes", "_successor_bytes"):
        assert f"{name} = max(0" not in retry
    assert "next_active = max(0" not in bounded
    assert "self._active += 1" not in bounded
    assert "self._retired += 1" not in bounded


def test_specialized_escrows_use_separate_rooted_authority() -> None:
    expected = {
        SRC / "core_impl" / "memory_budget.py": "RootedFinalizerAuthority",
        SRC / "core_impl" / "temporary_storage.py": "RootedFinalizerAuthority",
        SRC / "core_impl" / "cross_process_memory.py": "RootedFinalizerAuthority",
        SRC / "core_impl" / "path_identity.py": "RootedFinalizerAuthority",
        SRC / "api_impl" / "operation_context.py": "RootedFinalizerAuthority",
        SRC / "pipeline" / "partition_lookahead.py": "RootedFinalizerAuthority",
    }
    for path, marker in expected.items():
        source = path.read_text(encoding="utf-8")
        assert marker in source
        assert ".reserve_rooted(" in source
