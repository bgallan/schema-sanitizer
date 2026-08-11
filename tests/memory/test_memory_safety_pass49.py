"""Regressions for concurrency/memory hardening pass 49."""

from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def test_fixed_finalizer_epoch_saturates_instead_of_wrapping() -> None:
    from schema_sanitizer.core_impl.finalizer_escrow import _fixed_increment

    counter = bytearray([255, 255])
    assert _fixed_increment(counter) is False
    assert counter == bytearray([255, 255])


def test_reserved_finalizer_admission_uses_preallocated_ring_with_bounded_recovery() -> None:
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


def test_legacy_finalizer_activity_uses_exact_slots_not_telemetry_mirrors() -> None:
    source = (ROOT / "src/schema_sanitizer/core_impl/finalizer_escrow.py").read_text()
    start = source.index("    def activity_snapshot(self)", source.index("class FinalizerEscrow"))
    end = source.index("\n    def prepare_for_fork", start)
    body = source[start:end]
    assert "self.active_count()" in body
    active_start = source.index("    def active_count(self)", source.index("class FinalizerEscrow"))
    active_end = source.index("\n    def write_activity_into", active_start)
    active = source[active_start:active_end]
    assert "exact live-slot authority" in active
    assert "for index in range(self._capacity)" in active
    assert "self._slots[index]" in active
    assert "self._states[index]" in active


def test_quiescence_buffer_rejects_stable_reserved_owner_without_allocating_token() -> None:
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
    name = f"pass49-buffer-{id(escrow)}"
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


def test_finalizer_registry_rejects_genuinely_different_duplicate_hooks() -> None:
    from schema_sanitizer.core_impl.finalizer_registry import (
        _reset_finalizer_registry_for_tests,
        register_finalizer_domain,
    )

    _reset_finalizer_registry_for_tests()
    name = "pass49-different-domain"

    def drain_a() -> int:
        return 0

    def drain_b() -> int:
        return 1

    def snapshot() -> tuple[()]:
        return ()

    register_finalizer_domain(name, drain=drain_a, snapshot=snapshot)
    with pytest.raises(RuntimeError):
        register_finalizer_domain(name, drain=drain_b, snapshot=snapshot)


def test_finalizer_registry_freeze_rejects_new_domains() -> None:
    from schema_sanitizer.core_impl.finalizer_registry import (
        _reset_finalizer_registry_for_tests,
        freeze_finalizer_registry,
        register_finalizer_domain,
    )

    _reset_finalizer_registry_for_tests()
    freeze_finalizer_registry()
    with pytest.raises(RuntimeError):
        register_finalizer_domain("pass49-late-domain", drain=lambda: 0, snapshot=lambda: ())
    _reset_finalizer_registry_for_tests()


def test_reserved_fork_reset_is_idempotent_for_duplicate_child_callbacks() -> None:
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    escrow: ReservedFinalizerEscrow[object] = ReservedFinalizerEscrow(2)
    escrow.prepare_for_fork()
    escrow.reset_after_fork()
    first_slots = escrow._slots
    first_fresh = escrow._fork_fresh
    escrow.reset_after_fork()
    assert escrow._slots is first_slots
    assert escrow._fork_fresh is first_fresh


def test_fork_quarantine_preserves_previous_generation_until_safe_point() -> None:
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    escrow: ReservedFinalizerEscrow[object] = ReservedFinalizerEscrow(1)
    ticket = escrow.reserve_ticket()
    assert ticket is not None
    owner = object()
    assert escrow.publish_reserved(ticket, owner)
    escrow.prepare_for_fork()
    escrow.reset_after_fork()
    first_root = escrow._fork_roots[0]
    assert first_root is not None
    escrow.prepare_for_fork()
    assert escrow._fork_roots[0] is first_root
    # Pass71 adds exact ticket metadata to each quarantined generation, so the
    # fixed root stride expands from 14 to 16 while preserving prior roots.
    assert escrow._fork_roots[16] is escrow._slots
    escrow.reset_after_fork()
    assert escrow._fork_root_count == 2
    escrow.capacity_snapshot()  # explicit normal-runtime safe point
    assert escrow._fork_root_count == 0
    assert all(root is None for root in escrow._fork_roots)


def test_fork_prepare_exhaustion_never_raises_inside_atfork_callback() -> None:
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
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow
    from schema_sanitizer.core_impl.static_control_plane import static_control_plane_entries

    before = static_control_plane_entries()
    ReservedFinalizerEscrow(3)
    after = static_control_plane_entries()
    assert after == before


def test_global_escrows_declare_stable_static_kinds() -> None:
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


def test_static_baseline_includes_spare_finalizer_banks_conservatively() -> None:
    source = (ROOT / "src/schema_sanitizer/core_impl/finalizer_escrow.py").read_text()
    assert "capacity * 256" in source
    assert "self._capacity * 384" in source


def test_control_plane_reuses_released_exact_token_without_aba() -> None:
    from schema_sanitizer.core_impl.control_plane_budget import _ProcessControlPlaneBudget

    budget = _ProcessControlPlaneBudget()
    first = budget.reserve("pass49", 256)
    first_token = first.token
    first_capability = first.capability
    budget.release(first)
    second = budget.reserve("pass49", 256)
    assert second.token == first_token
    assert second.capability is not first_capability
    assert budget.release(first) is True  # stale owner cannot retire second
    assert budget.snapshot().active_tickets == 1
    budget.release(second)


def test_composite_close_keeps_execution_owned_until_memory_cleanup_commits() -> None:
    from schema_sanitizer.core_impl.memory_budget import CompositeParallelAdmission

    released: list[str] = []

    class Memory:
        def __init__(self) -> None:
            self.calls = 0

        def close(self) -> None:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("memory cleanup failed")

    class Execution:
        def release(self) -> None:
            released.append("execution")

    memory = Memory()
    admission = CompositeParallelAdmission(2, 1024, memory, Execution(), True)
    with pytest.raises(RuntimeError, match="memory cleanup failed"):
        admission.close()
    # Pass56 acquires resident bytes before workers, so reverse cleanup releases
    # the worker first. If memory cleanup fails, retaining excess bytes is safe.
    assert released == ["execution"]
    assert admission.execution_lease is None
    admission.close()
    assert released == ["execution"]


def test_composite_downshift_returns_surplus_physical_threads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import memory_budget, process_resources
    from schema_sanitizer.errors import SchemaSanitizerResourceError

    shrink_calls: list[int] = []

    class Execution:
        amount = 7

        def shrink(self, amount: int) -> None:
            self.amount = amount
            shrink_calls.append(amount)

        def release(self) -> None:
            self.amount = 0

    class Lease:
        def close(self) -> None:
            pass

    class Ledger:
        calls = 0

        def acquire(self, amount: int, *, stage: str):
            self.calls += 1
            if self.calls == 1:
                raise SchemaSanitizerResourceError("pressure")
            return Lease()

    execution = Execution()
    monkeypatch.setattr(memory_budget, "adaptive_parallel_slots", lambda *_a, **_k: 8)
    monkeypatch.setattr(memory_budget, "current_operation_memory_ledger", lambda: Ledger())
    monkeypatch.setattr(process_resources, "acquire_project_threads", lambda *_a, **_k: execution)
    admission = memory_budget.acquire_parallel_admission(8, per_slot_bytes=1024, stage="pass49")
    assert admission.slots == 4
    # Pass56 halves byte admission before it asks for helpers, so only the exact
    # three helpers are acquired and no post-acquisition shrink is needed.
    assert shrink_calls == []
    assert execution.amount == 7
    admission.close()


def test_required_composite_memory_fails_closed_and_returns_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import memory_budget, process_resources

    class Execution:
        amount = 1
        released = False

        def release(self) -> None:
            self.released = True

    execution = Execution()
    monkeypatch.setattr(memory_budget, "adaptive_parallel_slots", lambda *_a, **_k: 2)
    monkeypatch.setattr(memory_budget, "current_operation_memory_ledger", lambda: None)
    monkeypatch.setattr(process_resources, "acquire_project_threads", lambda *_a, **_k: execution)
    with pytest.raises(RuntimeError, match="requires an operation memory ledger"):
        memory_budget.acquire_parallel_admission(
            2, per_slot_bytes=1024, stage="pass49", require_memory=True
        )
    # Required memory now fails before any helper capacity is acquired.
    assert not execution.released


def test_native_reserve_snapshot_rolls_back_if_python_result_allocation_fails() -> None:
    source = (ROOT / "cpp/src/api/python_abi3/options/prepare.cc").read_text()
    start = source.index("py_operation_memory_ledger_reserve_snapshot")
    end = source.index("py_operation_memory_ledger_release", start)
    body = source[start:end]
    assert "ledger->Reserve" in body
    assert "Py_BuildValue" in body
    assert "if (result == nullptr)" in body
    assert "ledger->Release(bytes);" in body


def test_python_memory_reserve_uses_transactional_native_primitive() -> None:
    source = (ROOT / "src/schema_sanitizer/core_impl/memory_budget.py").read_text()
    start = source.index("\n    def reserve(\n", source.index("class OperationMemoryLedger"))
    end = source.index("\n    def release", start)
    body = source[start:end]
    assert "operation_memory_ledger_reserve_snapshot" in body
    assert "fallible Python snapshot exists after the commit" in body


def test_memory_ledger_close_rolls_back_closing_gate_on_snapshot_failure() -> None:
    source = (ROOT / "src/schema_sanitizer/core_impl/memory_budget.py").read_text()
    start = source.index(
        "    def close(self) -> None:", source.index("class OperationMemoryLedger")
    )
    end = source.index("\n    def __del__", start)
    body = source[start:end]
    assert "except BaseException:" in body
    assert "self._closing = False" in body
    assert "self._close_condition.notify_all()" in body


def test_legacy_native_completion_bypass_helpers_are_not_public_api() -> None:
    header = (ROOT / "cpp/src/internal/runtime/operation_task_arena.hh").read_text()
    assert "CanRetainCompletionBytesAfterTransfer" not in header
    assert "RetainCompletionBytes(" not in header
    assert "TryTransferActiveToCompletion" in header


def test_cross_process_pruning_uses_fixed_scratch_not_dynamic_stale_list() -> None:
    for relative in ("cross_process_storage.py", "cross_process_memory.py"):
        source = (ROOT / "src/schema_sanitizer/core_impl" / relative).read_text()
        assert "_STALE_KEY_SCRATCH" in source
        assert "_STALE_KEY_SCRATCH_LOCK" in source
        assert "stale_keys: list" not in source


def test_cross_process_scratch_locks_are_replaced_after_fork() -> None:
    storage = (ROOT / "src/schema_sanitizer/core_impl/cross_process_storage.py").read_text()
    memory = (ROOT / "src/schema_sanitizer/core_impl/cross_process_memory.py").read_text()
    assert "_STALE_KEY_SCRATCH_FORK_FRESH_LOCK" in storage
    assert "_STALE_KEY_SCRATCH_FORK_FRESH_LOCK" in memory
    assert "after_in_child=_reset_stale_scratch_after_fork" in memory


def test_shutdown_correctness_barrier_uses_preallocated_activity_buffers() -> None:
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
    source = (ROOT / "src/schema_sanitizer/core_impl/runtime_shutdown.py").read_text()
    start = source.index("def _perform_shutdown")
    end = source.index("\ndef shutdown_concurrency_runtime", start)
    body = source[start:end]
    assert "\n    from ." not in body
    assert "__import__" not in body
    assert 'sys.modules.get("schema_sanitizer._core_abi3")' in source


def test_finalizer_and_shutdown_registries_are_fork_safe_and_freezable() -> None:
    finalizers = (ROOT / "src/schema_sanitizer/core_impl/finalizer_registry.py").read_text()
    shutdown = (ROOT / "src/schema_sanitizer/core_impl/shutdown_observers.py").read_text()
    for source in (finalizers, shutdown):
        assert "os.register_at_fork" in source
        assert "after_in_child=" in source
        assert "is frozen" in source


def test_finalizer_epochs_are_fixed_width_not_best_effort_python_integers() -> None:
    source = (ROOT / "src/schema_sanitizer/core_impl/finalizer_escrow.py").read_text()
    assert "_publication_epoch +=" not in source
    assert "_progress_epoch +=" not in source
    assert "bytearray(8)" in source
    assert "Saturating increment" in source


def test_finalizer_registry_epoch_is_fixed_width() -> None:
    source = (ROOT / "src/schema_sanitizer/core_impl/finalizer_registry.py").read_text()
    assert "_REGISTRY_EPOCH = bytearray(8)" in source
    assert "_fixed_increment(_REGISTRY_EPOCH)" in source
    assert "_REGISTRY_EPOCH +=" not in source


def test_real_pair_observation_matrix_records_bound_contracts() -> None:
    # Import implementing modules so the shared contracts are registered.
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


def test_public_conversion_routes_activate_actual_input_output_pair_context() -> None:
    analytical = (ROOT / "src/schema_sanitizer/api_impl/analytical.py").read_text()
    converters = (ROOT / "src/schema_sanitizer/api_impl/file_conversion/converters.py").read_text()
    for source in (analytical, converters):
        assert "activate_runtime_concurrency_pair" in source
        assert "reset_runtime_concurrency_pair" in source
    assert "prepared_input.format" in analytical
    assert "prepared_input.format" in converters


def test_observed_56_pair_validator_is_execution_backed_not_boolean_metadata() -> None:
    source = (ROOT / "src/schema_sanitizer/core_impl/concurrency_coverage.py").read_text()
    assert "runtime_pair_contract_observations()" in source
    assert "def validate_observed_concurrency_pair_contracts" in source
    assert "calls <= 0" in source


def test_static_baseline_no_longer_uses_anonymous_eight_mib_constant() -> None:
    source = (ROOT / "src/schema_sanitizer/core_impl/control_plane_budget.py").read_text()
    assert "_STATIC_RUNTIME_BASELINE_BYTES" not in source
    assert "static_control_plane_bytes()" in source


def test_pass49_native_transaction_is_exposed_through_abi_table() -> None:
    methods = (ROOT / "cpp/src/internal/abi/python_abi3/methods.hh").read_text()
    module = (ROOT / "cpp/src/api/python_abi3/_core_abi3_module.cc").read_text()
    assert "py_operation_memory_ledger_reserve_snapshot" in methods
    assert '"operation_memory_ledger_reserve_snapshot"' in module


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
                        control = reserve_control_plane("pass49_pair_probe", 256)
                        release_control_plane(control)

                        upstream = ledger.acquire(512, stage="pass49_pair_upstream")
                        downstream = upstream.transfer_stage("pass49_pair_downstream")
                        downstream.release()

                        admission = acquire_parallel_admission(
                            1,
                            per_slot_bytes=512,
                            stage="pass49_pair_composite",
                            reserve_bytes=0,
                            require_memory=True,
                        )
                        assert admission.slots == 1
                        admission.close()
                    finally:
                        reset_runtime_concurrency_pair(pair_token)
        assert validate_observed_concurrency_pair_contracts() == 56
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
    ticket = reserve_control_plane("pass49_native_shadow_probe", 4096)
    try:
        after = memory_budget._raw_process_resident_memory_snapshot()
        assert after.reserved_bytes >= before.reserved_bytes + 4096
    finally:
        release_control_plane(ticket)
    restored = memory_budget._raw_process_resident_memory_snapshot()
    assert restored.reserved_bytes == before.reserved_bytes
