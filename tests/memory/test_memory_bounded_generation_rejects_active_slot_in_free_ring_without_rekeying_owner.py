"""Regression coverage for memory bounded generation rejects active slot in free ring without rekeying owner."""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src" / "schema_sanitizer"


def test_bounded_generation_rejects_active_slot_in_free_ring_without_rekeying_owner() -> None:
    from schema_sanitizer.core_impl.bounded_generation import BoundedGenerationPool

    pool = BoundedGenerationPool(2)
    first = pool.acquire()
    assert first is not None
    slot = first & pool._slot_mask
    pool._free[pool._head] = slot

    assert pool.acquire() is None
    assert pool.snapshot().corrupted is True
    assert pool.owns(first) is True
    # Corruption closes admission but exact cleanup remains possible.
    assert pool.release(first) is True
    assert pool.owns(first) is False


def test_bounded_generation_counter_corruption_quarantines_exact_slot() -> None:
    from schema_sanitizer.core_impl.bounded_generation import BoundedGenerationPool

    pool = BoundedGenerationPool(1)
    token = pool.acquire()
    assert token is not None
    pool._active = 0
    assert pool.release(token) is False
    assert pool.owns(token) is False
    assert pool.snapshot().corrupted is True


def test_stage_construction_escrow_roots_separate_authority() -> None:
    from schema_sanitizer.core_impl import memory_budget as module
    from schema_sanitizer.core_impl.rooted_finalizer import RootedFinalizerAuthority

    ticket, authority = module._reserve_stage_admission_construction_authority()
    slot = module._STAGE_ADMISSION_CONSTRUCTION_ESCROW._ticket_slots[ticket]
    try:
        assert isinstance(authority, RootedFinalizerAuthority)
        assert module._STAGE_ADMISSION_CONSTRUCTION_ESCROW._slots[slot] is authority
        assert module._retire_stage_admission_construction_ticket(ticket, authority)
    finally:
        module.drain_abandoned_memory_finalizers()


def test_path_claim_admission_is_pre_rooted_and_gc_tail_is_nonblocking() -> None:
    from schema_sanitizer.core_impl import path_identity as module

    admission = module._acquire_path_claim_admission()
    ticket = admission.finalizer_ticket
    authority = admission.finalizer_owner
    slot = module._PATH_CLAIM_FINALIZER_ESCROW._ticket_slots[ticket]
    assert module._PATH_CLAIM_FINALIZER_ESCROW._slots[slot] is authority
    assert module._PATH_CLAIM_FINALIZER_ESCROW._slots[slot] is not admission
    admission.release()
    module._drain_path_claim_finalizers(limit=4)

    source = (SRC / "core_impl" / "path_identity.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    cls = next(
        n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "_PathClaimAdmission"
    )
    destructor = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == "__del__")
    body = ast.get_source_segment(source, destructor) or ""
    assert "release_ticket(" not in body
    assert "_retire_path_claim_finalizer_ticket" not in body
    assert "arm_rooted_finalizer_authority" in body


def test_async_terminal_building_state_is_recoverable_after_publication_fault() -> None:
    from schema_sanitizer.core_impl import async_scheduler as scheduler

    class DoneTask:
        def done(self) -> bool:
            return True

    class FaultingSet(set):
        fail = True

        def __iter__(self):
            iterator = super().__iter__()
            yielded = False
            while True:
                try:
                    item = next(iterator)
                except StopIteration:
                    return
                if yielded and self.fail:
                    raise MemoryError(
                        "bounded-generation-rejects-active-slot-in injected task-bank publication fault"
                    )
                yielded = True
                yield item

    tasks = FaultingSet({DoneTask(), DoneTask()})
    admission = scheduler._AsyncSchedulerAdmission(0)
    before = scheduler._ASYNC_TERMINAL_DEBT_COUNT
    assert scheduler._park_async_terminal_debt(tasks, admission, None)
    assert scheduler._ASYNC_TERMINAL_DEBT_COUNT == before + 1
    building = next(
        debt
        for debt in scheduler._ASYNC_TERMINAL_DEBTS
        if debt.state == scheduler._ASYNC_DEBT_BUILDING
    )
    assert building.building_tasks is tasks
    tasks.fail = False
    assert scheduler._reap_one_async_terminal_debt()
    assert scheduler._ASYNC_TERMINAL_DEBT_COUNT == before


def test_terminal_ownership_underflow_quarantines_but_exact_slot_cleanup_survives() -> None:
    from schema_sanitizer.core_impl.terminal_ownership import TerminalOwnershipLedger

    ledger = TerminalOwnershipLedger(capacity=1)
    assert ledger.publish("bounded-generation-rejects-active-slot-in", 1, retained_bytes=64)
    ledger._owners = 0
    ledger.retire("bounded-generation-rejects-active-slot-in", 1)
    assert not any(slot.active for slot in ledger._slots)
    snapshot = ledger.snapshot()
    assert snapshot.owners == 0
    assert snapshot.rejected >= 1
    assert snapshot.corrupted is True
    assert (
        ledger.publish("bounded-generation-rejects-active-slot-in", 2, retained_bytes=64) is False
    )


def test_terminal_ownership_publication_prepares_counter_before_commit() -> None:
    source = (SRC / "core_impl" / "terminal_ownership.py").read_text(encoding="utf-8")
    start = source.index("    def publish(")
    end = source.index("\n    def retire(", start)
    body = source[start:end]
    assert body.index("next_owners = self._owners + 1") < body.index("slot.active = True")
    assert "self._owners += 1" not in body
    assert "max(0, self._owners -" not in source


def test_static_control_plane_never_clamps_exact_rollback() -> None:
    source = (SRC / "core_impl" / "static_control_plane.py").read_text(encoding="utf-8")
    start = source.index("def rollback_static_control_plane")
    end = source.index("\ndef register_static_control_plane", start)
    body = source[start:end]
    assert "max(0, _TOTAL - amount)" not in body
    assert "if _TOTAL < amount" in body


def test_static_footprint_guard_has_no_destructor() -> None:
    source = (SRC / "core_impl" / "finalizer_escrow.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    guard = next(
        n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "_StaticFootprintGuard"
    )
    assert all(not (isinstance(n, ast.FunctionDef) and n.name == "__del__") for n in guard.body)
    assert any(isinstance(n, ast.FunctionDef) and n.name == "rollback_now" for n in guard.body)


def test_provider_over_release_does_not_train_aimd_state() -> None:
    from schema_sanitizer.remote_impl import provider_throttle as module

    governor = module.ProviderThrottleGovernor(max_tracked_keys=4)
    state = module._State()
    state.in_flight = 0
    state.successes = 7
    state.window = 3
    governor._states["x"] = state
    with governor._condition:
        governor._release_locked("x", outcome="success", throttled=False, retry_after_seconds=None)
    assert state.in_flight == 0
    assert state.successes == 7
    assert state.window == 3
    assert governor._over_release_count == 1


def test_remote_and_provider_prepare_for_fork_select_preallocated_banks() -> None:
    for path in (
        SRC / "remote_impl" / "io_permits.py",
        SRC / "remote_impl" / "provider_throttle.py",
    ):
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        methods = [
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.FunctionDef) and n.name == "prepare_for_fork"
        ]
        assert methods
        for method in methods:
            body = ast.get_source_segment(source, method) or ""
            assert "Lock()" not in body
            assert "Condition()" not in body
            assert "local()" not in body
            assert "OrderedDict()" not in body
            assert "_ExpiryHeap()" not in body
            assert "_fork_banks" in body


def test_native_source_plan_has_no_unreserved_legacy_finalizer_fallback() -> None:
    source = (SRC / "input_impl" / "source_plan.py").read_text(encoding="utf-8")
    assert "defer_finalizer_cleanup(self)" not in source
    assert "defer_prepared_finalizer_cleanup(capsule)" in source


def test_authoritative_paths_have_no_new_saturating_decrements() -> None:
    checks = {
        SRC / "core_impl" / "bounded_generation.py": ("max(0, self._active -",),
        SRC / "core_impl" / "terminal_ownership.py": ("max(0, self._owners -",),
        SRC / "remote_impl" / "provider_throttle.py": ("next_in_flight = max(0,",),
    }
    for path, forbidden in checks.items():
        source = path.read_text(encoding="utf-8")
        for marker in forbidden:
            assert marker not in source
