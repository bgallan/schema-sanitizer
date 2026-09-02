"""Stress-tests fixed-width finalizer epochs alongside reserved rings, registry freeze,
fork reset, composite admission, native and Python reserve transactions, cross-process
pruning, shutdown barriers, pair observations, and control-plane shadow charges. Epochs
saturate without wrap; all recovery uses preallocated slots or scratch, and allocation
or snapshot failures roll back gates and resource domains exactly."""

from __future__ import annotations

from pathlib import Path
from threading import Event, Thread

import pytest
from _support.synchronization import join_thread_or_fail

ROOT = Path(__file__).resolve().parents[2]


def test_fixed_finalizer_epoch_saturates_instead_of_wrapping() -> None:
    """Verify fixed finalizer epoch saturates instead of wrapping."""
    from schema_sanitizer.core_impl.finalizer_registry import _fixed_increment

    counter = bytearray([255, 255])
    assert _fixed_increment(counter) is False
    assert counter == bytearray([255, 255])


def test_atomic_epoch_fork_locks_advance_without_reusing_an_ancestor_lock() -> None:
    """Use each preallocated child fallback lock once and then fail closed."""
    from schema_sanitizer.core_impl.atomic_epoch import AtomicEpoch

    epoch = AtomicEpoch()
    locks = epoch._fork_locks
    assert epoch._lock is locks[0]
    assert epoch.reset_after_fork()
    assert epoch._lock is locks[1]
    assert epoch.reset_after_fork()
    assert epoch._lock is locks[2]
    assert not epoch.reset_after_fork()
    assert epoch._lock is locks[2]
    epoch.replenish_fork_locks()
    assert epoch._fork_locks[0] is locks[2]
    assert epoch.reset_after_fork()
    assert epoch.reset_after_fork()
    assert not epoch.reset_after_fork()


def test_native_atomic_epoch_exact_store_rejects_uint64_wraparound() -> None:
    """Reject native exact-store inputs that would otherwise wrap modulo uint64."""
    from schema_sanitizer.core_impl.native_runtime import native_core

    counter = native_core.atomic_epoch_create()
    for invalid in (-1, 1 << 64):
        with pytest.raises(OverflowError):
            native_core.atomic_epoch_set_exact(counter, invalid)
    native_core.atomic_epoch_set_exact(counter, (1 << 64) - 1)
    assert native_core.atomic_epoch_value(counter) == (1 << 64) - 1


def test_native_atomic_epoch_marked_increment_commits_exactly_once() -> None:
    """Commit one counter event and its retry marker in a single native call."""
    from schema_sanitizer.core_impl.native_runtime import native_core

    counter = native_core.atomic_epoch_create()
    markers = bytearray([1, 0])
    assert native_core.atomic_epoch_increment_marked(counter, markers, 0)
    assert markers == bytearray([2, 0])
    assert native_core.atomic_epoch_value(counter) == 1
    assert native_core.atomic_epoch_increment_marked(counter, markers, 0)
    assert native_core.atomic_epoch_value(counter) == 1
    assert not native_core.atomic_epoch_increment_marked(counter, markers, 1)
    assert native_core.atomic_epoch_value(counter) == 1


def test_native_atomic_epoch_marked_increment_records_saturation() -> None:
    """Advance the retry marker when the visible failure count is saturated."""
    from schema_sanitizer.core_impl.native_runtime import native_core

    counter = native_core.atomic_epoch_create()
    native_core.atomic_epoch_set_exact(counter, (1 << 64) - 1)
    markers = bytearray([1])
    assert native_core.atomic_epoch_increment_marked(counter, markers, 0)
    assert markers == bytearray([2])
    assert native_core.atomic_epoch_value(counter) == (1 << 64) - 1


def test_atomic_epoch_marked_increment_trusts_committed_postcondition() -> None:
    """Acknowledge a native pair committed before an asynchronous exception."""
    from schema_sanitizer.core_impl.atomic_epoch import AtomicEpoch

    counter = AtomicEpoch()
    original_increment_marked = counter._inc_marked
    assert original_increment_marked is not None

    def commit_then_interrupt(capsule: object, markers: bytearray, index: int) -> object:
        """Raise after the real native counter-and-marker commit returns."""
        original_increment_marked(capsule, markers, index)
        raise KeyboardInterrupt("post marked increment")

    counter._inc_marked = commit_then_interrupt
    markers = bytearray([1])
    assert counter.increment_marked(markers, 0)
    assert markers == bytearray([2])
    assert counter.value() == 1
    assert counter.increment_marked(markers, 0)
    assert counter.value() == 1


def test_reserved_finalizer_admission_uses_preallocated_ring_with_bounded_recovery() -> None:
    """Verify reserved finalizer admission uses preallocated ring with bounded recovery."""
    source = (ROOT / "src/schema_sanitizer/core_impl/finalizer_escrow.py").read_text()
    start = source.index(
        "    def reserve_ticket(self)", source.index("class ReservedFinalizerEscrow")
    )
    end = source.index("\n    def release_ticket", start)
    body = source[start:end]
    prepare = body.index("_ring_prepare_pop_locked()")
    recover = body.index("_recycle_one_pending_locked()", prepare)
    commit = body.index("_ring_commit_pop_locked", recover)
    assert prepare < recover < commit
    # The normal path is an O(1) free-ring pop. A bounded fixed-capacity scan is
    # permitted only when that ring is empty, before a new owner/ticket commits.
    assert "for " not in body[:recover]
    assert "_ticket_slots[ticket] = slot" in body[commit:]


def test_reserved_finalizer_capacity_snapshot_uses_exact_fixed_slot_authority() -> None:
    """Verify reserved finalizer capacity snapshot uses exact fixed slot authority."""
    source = (ROOT / "src/schema_sanitizer/core_impl/finalizer_escrow.py").read_text()
    start = source.index(
        "    def capacity_snapshot(self)", source.index("class ReservedFinalizerEscrow")
    )
    end = source.index("\n    def write_activity_into", start)
    body = source[start:end]
    assert "active = self.active_count()" in body
    assert "retired = self.retired_count()" in body
    assert "for slot in range(self._capacity)" in body
    assert "available = self._free_count" in body


def test_quiescence_buffer_rejects_stable_reserved_owner_without_allocating_token() -> None:
    """Verify quiescence buffer rejects stable reserved owner without allocating token."""
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow
    from schema_sanitizer.core_impl.finalizer_registry import (
        _reset_finalizer_registry_for_tests,
        finalizer_activity_buffer_size,
        freeze_finalizer_registry,
        register_finalizer_domain,
        write_finalizer_activity_into,
    )

    _reset_finalizer_registry_for_tests()
    escrow: ReservedFinalizerEscrow[object] = ReservedFinalizerEscrow(1)
    name = f"fixed-finalizer-epoch-saturates-instead-of-buffer-{id(escrow)}"
    register_finalizer_domain(name, drain=lambda: 0, snapshot=lambda: (), escrows=((name, escrow),))
    ticket = escrow.reserve_ticket()
    assert ticket is not None
    freeze_finalizer_registry()
    first = bytearray(finalizer_activity_buffer_size())
    second = bytearray(len(first))
    assert write_finalizer_activity_into(first) is False
    assert write_finalizer_activity_into(second) is False
    assert first == second
    escrow.release_ticket(ticket)
    # Other process-global domains may legitimately own cleanup while this
    # regression runs as part of the full native suite. Prove the local owner
    # became quiescent without assuming the entire runtime is idle.
    assert escrow.activity_is_quiescent() is True
    _reset_finalizer_registry_for_tests()


def test_frozen_activity_capsules_survive_forced_capacity_rebuild() -> None:
    """Keep shutdown's cached native counters live through recovery rebuild."""
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow
    from schema_sanitizer.core_impl.finalizer_registry import (
        _reset_finalizer_registry_for_tests,
        finalizer_activity_buffer_size,
        finalizer_domains,
        freeze_finalizer_registry,
        register_finalizer_domain,
        write_finalizer_activity_into,
    )

    _reset_finalizer_registry_for_tests()
    escrow: ReservedFinalizerEscrow[object] = ReservedFinalizerEscrow(1)
    name = f"fixed-finalizer-epoch-rebuild-capsules-{id(escrow)}"
    register_finalizer_domain(name, drain=lambda: 0, snapshot=lambda: (), escrows=((name, escrow),))
    freeze_finalizer_registry()
    counters = escrow.activity_counters()
    capsules = tuple(counter.native_capsule for counter in counters)
    assert all(capsule is not None for capsule in capsules)

    ticket = escrow.reserve_ticket()
    assert ticket is not None
    with escrow._reserve_lock:
        escrow._capacity_mirrors_dirty = True
        escrow._rebuild_capacity_mirrors_locked()

    assert escrow.activity_counters() == counters
    assert tuple(counter.native_capsule for counter in escrow.activity_counters()) == capsules
    activity = bytearray(finalizer_activity_buffer_size())
    assert write_finalizer_activity_into(activity) is False
    offset = 9
    for domain in finalizer_domains():
        for _escrow_name, registered_escrow in domain.escrows:
            if registered_escrow is escrow:
                assert int.from_bytes(activity[offset : offset + 8], "little") == 1
                break
            offset += 32
        else:
            continue
        break
    else:
        raise AssertionError("frozen registry lost the rebuilt escrow")
    assert escrow.release_ticket(ticket)
    _reset_finalizer_registry_for_tests()


def test_frozen_activity_stays_conservative_before_rooted_owner_commit() -> None:
    """Observe the active counter while rooted admission pauses before authority."""
    import schema_sanitizer.core_impl.finalizer_escrow as module
    from schema_sanitizer.core_impl.finalizer_registry import (
        _reset_finalizer_registry_for_tests,
        finalizer_activity_buffer_size,
        finalizer_domains,
        freeze_finalizer_registry,
        register_finalizer_domain,
        write_finalizer_activity_into,
    )
    from schema_sanitizer.core_impl.rooted_finalizer import RootedFinalizerAuthority

    _reset_finalizer_registry_for_tests()
    escrow: module.ReservedFinalizerEscrow[RootedFinalizerAuthority] = (
        module.ReservedFinalizerEscrow(1)
    )
    name = f"fixed-finalizer-epoch-preowner-activity-{id(escrow)}"
    register_finalizer_domain(name, drain=lambda: 0, snapshot=lambda: (), escrows=((name, escrow),))
    freeze_finalizer_registry()
    owner = RootedFinalizerAuthority(lambda _owner: None)
    owner_install_started = Event()
    finish_owner_install = Event()

    class PausingSlots(list[object]):
        """Pause the exact authority assignment after conservative admission."""

        def __setitem__(self, index, value) -> None:
            """Expose the pre-owner activity window deterministically."""
            if value is owner and not owner_install_started.is_set():
                owner_install_started.set()
                assert finish_owner_install.wait(30)
            super().__setitem__(index, value)

    escrow._slots = PausingSlots(escrow._slots)
    result: dict[str, int | None] = {}
    thread = Thread(target=lambda: result.setdefault("ticket", escrow.reserve_rooted(owner)))
    thread.start()
    assert owner_install_started.wait(30)

    activity = bytearray(finalizer_activity_buffer_size())
    write_finalizer_activity_into(activity)
    offset = 9
    for domain in finalizer_domains():
        for _escrow_name, registered_escrow in domain.escrows:
            if registered_escrow is escrow:
                assert int.from_bytes(activity[offset : offset + 8], "little") == 1
                break
            offset += 32
        else:
            continue
        break
    else:
        raise AssertionError("frozen registry lost the pre-owner escrow")

    finish_owner_install.set()
    join_thread_or_fail(thread)
    assert result["ticket"] is not None
    assert escrow.release_rooted_owner(owner)
    _reset_finalizer_registry_for_tests()


def test_finalizer_registry_rejects_genuinely_different_duplicate_hooks() -> None:
    """Verify finalizer registry rejects genuinely different duplicate hooks."""
    from schema_sanitizer.core_impl.finalizer_registry import (
        _reset_finalizer_registry_for_tests,
        register_finalizer_domain,
    )

    _reset_finalizer_registry_for_tests()
    name = "fixed-finalizer-epoch-saturates-instead-of-different-domain"

    def drain_a() -> int:
        """Drain callbacks from the first finalizer generation."""
        return 0

    def drain_b() -> int:
        """Drain callbacks from the second finalizer generation."""
        return 1

    def snapshot() -> tuple[()]:
        """Return the authoritative state snapshot expected by the test."""
        return ()

    register_finalizer_domain(name, drain=drain_a, snapshot=snapshot)
    with pytest.raises(RuntimeError):
        register_finalizer_domain(name, drain=drain_b, snapshot=snapshot)


def test_finalizer_registry_freeze_rejects_new_domains() -> None:
    """Verify finalizer registry freeze rejects new domains."""
    from schema_sanitizer.core_impl.finalizer_registry import (
        _reset_finalizer_registry_for_tests,
        freeze_finalizer_registry,
        register_finalizer_domain,
    )

    _reset_finalizer_registry_for_tests()
    freeze_finalizer_registry()
    with pytest.raises(RuntimeError):
        register_finalizer_domain(
            "fixed-finalizer-epoch-saturates-instead-of-late-domain",
            drain=lambda: 0,
            snapshot=lambda: (),
        )
    _reset_finalizer_registry_for_tests()


def test_reserved_fork_reset_is_idempotent_for_duplicate_child_callbacks() -> None:
    """Verify reserved fork reset is idempotent for duplicate child callbacks."""
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    escrow: ReservedFinalizerEscrow[object] = ReservedFinalizerEscrow(2)
    escrow.prepare_for_fork()
    escrow.reset_after_fork()
    first_slots = escrow._slots
    first_fresh = escrow._fork_fresh
    escrow.reset_after_fork()
    assert escrow._slots is first_slots
    assert escrow._fork_fresh is first_fresh


def test_prepared_fork_reset_preserves_frozen_counter_capsule_identity() -> None:
    """Keep a frozen registry's native counter view valid in a prepared child."""
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow
    from schema_sanitizer.core_impl.finalizer_registry import (
        _reset_finalizer_registry_for_tests,
        finalizer_activity_buffer_size,
        finalizer_domains,
        freeze_finalizer_registry,
        register_finalizer_domain,
        write_finalizer_activity_into,
    )

    _reset_finalizer_registry_for_tests()
    escrow: ReservedFinalizerEscrow[object] = ReservedFinalizerEscrow(1)
    name = f"fixed-finalizer-epoch-fork-capsules-{id(escrow)}"
    register_finalizer_domain(name, drain=lambda: 0, snapshot=lambda: (), escrows=((name, escrow),))
    freeze_finalizer_registry()
    counters = escrow.activity_counters()
    capsules = tuple(counter.native_capsule for counter in counters)

    escrow.prepare_for_fork()
    escrow.reset_after_fork()

    assert escrow.activity_counters() == counters
    assert tuple(counter.native_capsule for counter in escrow.activity_counters()) == capsules
    activity = bytearray(finalizer_activity_buffer_size())
    write_finalizer_activity_into(activity)
    offset = 9
    for domain in finalizer_domains():
        for _escrow_name, registered_escrow in domain.escrows:
            if registered_escrow is escrow:
                assert int.from_bytes(activity[offset : offset + 8], "little") == 0
                break
            offset += 32
        else:
            continue
        break
    else:
        raise AssertionError("frozen registry lost the prepared escrow")
    _reset_finalizer_registry_for_tests()


def test_fork_quarantine_preserves_previous_generation_until_safe_point() -> None:
    """Verify fork quarantine preserves previous generation until safe point."""
    from schema_sanitizer.core_impl.finalizer_escrow import (
        _FORK_ROOTS_PER_GENERATION,
        ReservedFinalizerEscrow,
    )

    escrow: ReservedFinalizerEscrow[object] = ReservedFinalizerEscrow(1)
    ticket = escrow.reserve_ticket()
    assert ticket is not None
    owner = object()
    assert escrow.publish_reserved(ticket, owner)
    prepared_bank = escrow._fork_fresh
    escrow.prepare_for_fork()
    escrow.reset_after_fork()
    first_root = escrow._fork_roots[0]
    assert first_root is not None
    assert escrow._fork_roots[_FORK_ROOTS_PER_GENERATION - 1] is prepared_bank
    escrow.prepare_for_fork()
    assert escrow._fork_roots[0] is first_root
    # Exact ticket metadata belongs to each quarantined generation.
    assert escrow._fork_roots[_FORK_ROOTS_PER_GENERATION] is escrow._slots
    escrow.reset_after_fork()
    assert escrow._fork_root_count == 2
    escrow.capacity_snapshot()  # explicit normal-runtime safe point
    assert escrow._fork_root_count == 0
    assert all(root is None for root in escrow._fork_roots)


def test_fork_prepare_exhaustion_never_raises_inside_atfork_callback() -> None:
    """Verify fork prepare exhaustion never raises inside atfork callback."""
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    escrow: ReservedFinalizerEscrow[object] = ReservedFinalizerEscrow(1)
    escrow._fork_fresh = None
    # CPython ignores at-fork callback exceptions and continues the fork.
    # Exhaustion must therefore be represented by an inert child sentinel.
    escrow.prepare_for_fork()
    assert escrow._fork_prepare_index == -2
    assert escrow._fork_prepare_exhausted is True
    escrow.reset_after_fork()
    assert escrow._fork_unusable_after_fork is True
    assert escrow.reserve_ticket() is None
    assert escrow.activity_is_quiescent() is False


def test_ephemeral_escrow_does_not_pollute_static_baseline() -> None:
    """Verify ephemeral escrow does not pollute static baseline."""
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow
    from schema_sanitizer.core_impl.static_control_plane import static_control_plane_entries

    before = static_control_plane_entries()
    ReservedFinalizerEscrow(3)
    after = static_control_plane_entries()
    assert after == before


def test_global_escrows_declare_stable_static_kinds() -> None:
    """Verify global escrows declare stable static kinds."""
    files = (
        "core_impl/finalizer_cleanup.py",
        "core_impl/memory_budget.py",
        "core_impl/temporary_storage.py",
        "core_impl/path_identity.py",
        "api_impl/operation_context.py",
        "pipeline/partition_lookahead.py",
    )
    for relative in files:
        source = (ROOT / "src/schema_sanitizer" / relative).read_text()
        assert "static_kind=" in source, relative


def test_control_plane_reuses_released_exact_token_without_aba() -> None:
    """Verify control plane reuses released exact token without ABA."""
    from schema_sanitizer.core_impl.control_plane_budget import _ProcessControlPlaneBudget

    budget = _ProcessControlPlaneBudget()
    first = budget.reserve("fixed-finalizer-epoch-saturates-instead-of", 256)
    first_token = first.token
    first_capability = first.capability
    budget.release(first)
    second = budget.reserve("fixed-finalizer-epoch-saturates-instead-of", 256)
    assert second.token == first_token
    assert second.capability is not first_capability
    assert budget.release(first) is True  # stale owner cannot retire second
    assert budget.snapshot().active_tickets == 1
    budget.release(second)


def test_composite_close_keeps_execution_owned_until_memory_cleanup_commits() -> None:
    """Verify composite close keeps execution owned until memory cleanup commits."""
    from schema_sanitizer.core_impl.memory_budget import CompositeParallelAdmission

    released: list[str] = []

    class Memory:
        def __init__(self) -> None:
            """Initialize the memory test double."""
            self.calls = 0

        def close(self) -> None:
            """Close the resources owned by the memory test double."""
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("memory cleanup failed")

    class Execution:
        def release(self) -> None:
            """Release the resource held by the execution test double."""
            released.append("execution")

    memory = Memory()
    admission = CompositeParallelAdmission(2, 1024, memory, Execution(), True)
    with pytest.raises(RuntimeError, match="memory cleanup failed"):
        admission.close()
    # Admission acquires resident bytes before workers, so reverse cleanup releases
    # the worker first. If memory cleanup fails, retaining excess bytes is safe.
    assert released == ["execution"]
    assert admission.execution_lease is None
    admission.close()
    assert released == ["execution"]


def test_composite_downshift_returns_surplus_physical_threads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify composite downshift returns surplus physical threads."""
    from schema_sanitizer.core_impl import memory_budget, process_resources
    from schema_sanitizer.errors import SchemaSanitizerResourceError

    shrink_calls: list[int] = []

    class Execution:
        amount = 7

        def shrink(self, amount: int) -> None:
            """Update the execution lease and record the downshift target."""
            self.amount = amount
            shrink_calls.append(amount)

        def release(self) -> None:
            """Release the resource held by the execution test double."""
            self.amount = 0

    class Lease:
        def close(self) -> None:
            """Close the resources owned by the lease test double."""
            pass

    class Ledger:
        calls = 0

        def acquire(self, amount: int, *, stage: str):
            """Acquire the resource represented by the ledger test double."""
            self.calls += 1
            if self.calls == 1:
                raise SchemaSanitizerResourceError("pressure")
            return Lease()

    execution = Execution()
    monkeypatch.setattr(memory_budget, "adaptive_parallel_slots", lambda *_a, **_k: 8)
    monkeypatch.setattr(memory_budget, "current_operation_memory_ledger", lambda: Ledger())
    monkeypatch.setattr(process_resources, "acquire_project_threads", lambda *_a, **_k: execution)
    admission = memory_budget.acquire_parallel_admission(
        8, per_slot_bytes=1024, stage="fixed-finalizer-epoch-saturates-instead-of"
    )
    assert admission.slots == 4
    # Admission halves bytes before it asks for helpers, so only the exact
    # three helpers are acquired and no post-acquisition shrink is needed.
    assert shrink_calls == []
    assert execution.amount == 7
    admission.close()


def test_required_composite_memory_fails_closed_and_returns_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify required composite memory fails closed and returns execution."""
    from schema_sanitizer.core_impl import memory_budget, process_resources

    class Execution:
        amount = 1
        released = False

        def release(self) -> None:
            """Release the resource held by the execution test double."""
            self.released = True

    execution = Execution()
    monkeypatch.setattr(memory_budget, "adaptive_parallel_slots", lambda *_a, **_k: 2)
    monkeypatch.setattr(memory_budget, "current_operation_memory_ledger", lambda: None)
    monkeypatch.setattr(process_resources, "acquire_project_threads", lambda *_a, **_k: execution)
    with pytest.raises(RuntimeError, match="requires an operation memory ledger"):
        memory_budget.acquire_parallel_admission(
            2,
            per_slot_bytes=1024,
            stage="fixed-finalizer-epoch-saturates-instead-of",
            require_memory=True,
        )
    # Required memory now fails before any helper capacity is acquired.
    assert not execution.released


def test_native_reserve_snapshot_rolls_back_if_python_result_allocation_fails() -> None:
    """Verify native reserve snapshot rolls back if Python result allocation fails."""
    source = (ROOT / "cpp/src/api/python_abi3/options/prepare.cc").read_text()
    start = source.index("py_operation_memory_ledger_reserve_snapshot")
    end = source.index("py_operation_memory_ledger_release", start)
    body = source[start:end]
    assert "ledger->Reserve" in body
    assert "Py_BuildValue" in body
    assert "if (result == nullptr)" in body
    assert "ledger->Release(bytes);" in body


def test_python_memory_reserve_uses_transactional_native_primitive() -> None:
    """Verify Python memory reserve uses transactional native primitive."""
    source = (ROOT / "src/schema_sanitizer/core_impl/memory_budget.py").read_text()
    start = source.index("\n    def reserve(\n", source.index("class OperationMemoryLedger"))
    end = source.index("\n    def release", start)
    body = source[start:end]
    assert "operation_memory_ledger_reserve_snapshot" in body


def test_memory_ledger_close_rolls_back_closing_gate_on_snapshot_failure() -> None:
    """Verify memory ledger close rolls back closing gate on snapshot failure."""
    source = (ROOT / "src/schema_sanitizer/core_impl/memory_budget.py").read_text()
    start = source.index(
        "    def close(self) -> None:", source.index("class OperationMemoryLedger")
    )
    end = source.index("\n    def __del__", start)
    body = source[start:end]
    assert "except BaseException:" in body
    assert "self._closing = False" in body
    assert "self._close_condition.notify_all()" in body


def test_cross_process_pruning_uses_fixed_scratch_not_dynamic_stale_list() -> None:
    """Verify cross process pruning uses fixed scratch not dynamic stale list."""
    for relative in ("cross_process_storage.py", "cross_process_memory.py"):
        source = (ROOT / "src/schema_sanitizer/core_impl" / relative).read_text()
        assert "_STALE_KEY_SCRATCH" in source
        assert "_STALE_KEY_SCRATCH_LOCK" in source
        assert "stale_keys: list" not in source


def test_cross_process_scratch_locks_are_replaced_after_fork() -> None:
    """Verify cross process scratch locks are replaced after fork."""
    storage = (ROOT / "src/schema_sanitizer/core_impl/cross_process_storage.py").read_text()
    memory = (ROOT / "src/schema_sanitizer/core_impl/cross_process_memory.py").read_text()
    assert "_STALE_KEY_SCRATCH_FORK_FRESH_LOCK" in storage
    assert "_STALE_KEY_SCRATCH_FORK_FRESH_LOCK" in memory
    assert "after_in_child=_reset_stale_scratch_after_fork" in memory


def test_shutdown_correctness_barrier_uses_preallocated_activity_buffers() -> None:
    """Verify shutdown correctness barrier uses preallocated activity buffers."""
    source = (ROOT / "src/schema_sanitizer/core_impl/runtime_shutdown.py").read_text()
    assert "finalizer_activity_a = bytearray(activity_size)" in source
    assert "finalizer_activity_b = bytearray(activity_size)" in source
    start = source.index("    def drain_finalizer_epoch()")
    end = source.index("\n    finalizer_quiescent =", start)
    body = source[start:end]
    assert "snapshots: list" not in body
    assert "write_finalizer_activity_into(current)" in body
    assert "current == previous and quiescent" in body


def test_shutdown_primary_path_contains_no_dynamic_subsystem_imports() -> None:
    """Verify shutdown primary path contains no dynamic subsystem imports."""
    source = (ROOT / "src/schema_sanitizer/core_impl/runtime_shutdown.py").read_text()
    start = source.index("def _perform_shutdown")
    end = source.index("\ndef shutdown_concurrency_runtime", start)
    body = source[start:end]
    assert "\n    from ." not in body
    assert "__import__" not in body
    assert 'sys.modules.get("schema_sanitizer._core_abi3")' in source


def test_finalizer_and_shutdown_registries_are_fork_safe_and_freezable() -> None:
    """Verify finalizer and shutdown registries are fork safe and freezable."""
    finalizers = (ROOT / "src/schema_sanitizer/core_impl/finalizer_registry.py").read_text()
    shutdown = (ROOT / "src/schema_sanitizer/core_impl/shutdown_observers.py").read_text()
    for source in (finalizers, shutdown):
        assert "register_fork_handler" in source
        assert "after_in_child=" in source
        assert "is frozen" in source


def test_finalizer_epochs_are_fixed_width_not_best_effort_python_integers() -> None:
    """Verify finalizer epochs are fixed width not best effort Python integers."""
    source = (ROOT / "src/schema_sanitizer/core_impl/finalizer_escrow.py").read_text()
    assert "_publication_epoch +=" not in source
    assert "_progress_epoch +=" not in source
    assert "AtomicEpoch()" in source
    assert "_publication_epoch_counter.increment()" in source


def test_finalizer_registry_epoch_is_fixed_width() -> None:
    """Verify finalizer registry epoch is fixed width."""
    source = (ROOT / "src/schema_sanitizer/core_impl/finalizer_registry.py").read_text()
    assert "_REGISTRY_EPOCH = bytearray(8)" in source
    assert "_fixed_increment(_REGISTRY_EPOCH)" in source
    assert "_REGISTRY_EPOCH +=" not in source


def test_real_pair_observation_matrix_records_bound_contracts() -> None:
    # Import implementing modules so the shared contracts are registered.
    """Verify real pair observation matrix records bound contracts."""
    import schema_sanitizer.core_impl.control_plane_budget  # noqa: F401
    import schema_sanitizer.core_impl.memory_budget  # noqa: F401
    from schema_sanitizer.core_impl.concurrency_contracts import (
        activate_runtime_concurrency_pair,
        observe_runtime_concurrency_contract,
        reset_runtime_concurrency_pair,
    )
    from schema_sanitizer.core_impl.concurrency_coverage import observed_concurrency_pair_guarantees

    token = activate_runtime_concurrency_pair("csv", "parquet")
    try:
        for name in (
            "transferable_resident_memory_credit",
            "composite_slot_and_byte_admission",
            "process_control_plane_budget",
        ):
            observe_runtime_concurrency_contract(name)
    finally:
        reset_runtime_concurrency_pair(token)
    evidence = observed_concurrency_pair_guarantees()["csv"]["parquet"]
    assert all(evidence[name] > 0 for name in evidence)


def test_observed_56_pair_validator_is_execution_backed_not_boolean_metadata() -> None:
    """Verify observed 56 pair validator is execution backed not boolean metadata."""
    source = (ROOT / "src/schema_sanitizer/core_impl/concurrency_coverage.py").read_text()
    assert "runtime_pair_contract_observations()" in source
    assert "def validate_observed_concurrency_pair_contracts" in source
    assert "calls <= 0" in source


def test_static_baseline_no_longer_uses_anonymous_eight_mib_constant() -> None:
    """Verify static baseline no longer uses anonymous eight mib constant."""
    source = (ROOT / "src/schema_sanitizer/core_impl/control_plane_budget.py").read_text()
    assert "_STATIC_RUNTIME_BASELINE_BYTES" not in source
    assert "static_control_plane_bytes()" in source


def test_native_transaction_is_exposed_through_abi_table() -> None:
    """Verify native transaction is exposed through ABI table."""
    catalog = (ROOT / "cpp/src/internal/abi/python_abi3/method_catalog.inc").read_text()
    module = (ROOT / "cpp/src/api/python_abi3/_core_abi3_module.cc").read_text()
    assert "operation_memory_ledger_reserve_snapshot" in catalog
    assert 'include "internal/abi/python_abi3/method_catalog.inc"' in module


def test_all_56_pairs_execute_real_shared_concurrency_primitives() -> None:
    """Exercise the three enforcing primitives under every concrete pair identity."""
    from types import ModuleType

    from schema_sanitizer.core_impl.native_runtime import native_core

    if not isinstance(native_core, ModuleType):
        pytest.skip("source-only runtime cannot execute real memory-ledger primitives")

    from schema_sanitizer.core_impl.concurrency_contracts import (
        activate_runtime_concurrency_pair,
        reset_runtime_concurrency_pair,
    )
    from schema_sanitizer.core_impl.concurrency_coverage import (
        INPUT_CONCURRENCY_COVERAGE,
        OUTPUT_CONCURRENCY_COVERAGE,
        validate_observed_concurrency_pair_contracts,
    )
    from schema_sanitizer.core_impl.control_plane_budget import (
        release_control_plane,
        reserve_control_plane,
    )
    from schema_sanitizer.core_impl.memory_budget import (
        OperationMemoryLedger,
        acquire_parallel_admission,
        activate_operation_memory_ledger,
    )

    ledger = OperationMemoryLedger(8 << 20)
    try:
        with activate_operation_memory_ledger(ledger):
            for input_format in INPUT_CONCURRENCY_COVERAGE:
                for output_format in OUTPUT_CONCURRENCY_COVERAGE:
                    pair_token = activate_runtime_concurrency_pair(input_format, output_format)
                    try:
                        control = reserve_control_plane(
                            "fixed-finalizer-epoch-saturates-instead-of_pair_probe", 256
                        )
                        release_control_plane(control)

                        upstream = ledger.acquire(
                            512, stage="fixed-finalizer-epoch-saturates-instead-of_pair_upstream"
                        )
                        downstream = upstream.transfer_stage(
                            "fixed-finalizer-epoch-saturates-instead-of_pair_downstream"
                        )
                        downstream.release()

                        admission = acquire_parallel_admission(
                            1,
                            per_slot_bytes=512,
                            stage="fixed-finalizer-epoch-saturates-instead-of_pair_composite",
                            reserve_bytes=0,
                            require_memory=True,
                        )
                        assert admission.slots == 1
                        admission.close()
                    finally:
                        reset_runtime_concurrency_pair(pair_token)
        assert validate_observed_concurrency_pair_contracts() == 49
    finally:
        ledger.close()


def test_control_plane_is_shadow_charged_into_native_resident_pool_when_available() -> None:
    """A Python control ticket must reduce headroom seen by native-only work."""
    from types import ModuleType

    import schema_sanitizer.core_impl.memory_budget as memory_budget
    from schema_sanitizer.core_impl.control_plane_budget import (
        release_control_plane,
        reserve_control_plane,
        synchronize_control_plane_native_shadow,
    )
    from schema_sanitizer.core_impl.native_runtime import native_core

    if not isinstance(native_core, ModuleType):
        pytest.skip("source-only runtime has no native shared resident pool")
    active, _ = synchronize_control_plane_native_shadow()
    if not active:
        pytest.skip("native shadow ABI is unavailable")
    before = memory_budget._raw_process_resident_memory_snapshot()
    ticket = reserve_control_plane(
        "fixed-finalizer-epoch-saturates-instead-of_native_shadow_probe", 4096
    )
    try:
        after = memory_budget._raw_process_resident_memory_snapshot()
        assert after.reserved_bytes >= before.reserved_bytes + 4096
    finally:
        release_control_plane(ticket)
    restored = memory_budget._raw_process_resident_memory_snapshot()
    assert restored.reserved_bytes == before.reserved_bytes
