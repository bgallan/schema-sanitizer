"""Regression coverage for memory ephemeral reserved escrow is not rooted by at fork registry."""

from __future__ import annotations

import gc
import threading
import weakref
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def test_ephemeral_reserved_escrow_is_not_rooted_by_at_fork_registry() -> None:
    from schema_sanitizer.core_impl.finalizer_escrow import ReservedFinalizerEscrow

    escrow = ReservedFinalizerEscrow(2)
    ref = weakref.ref(escrow)
    del escrow
    gc.collect()
    assert ref() is None


def test_atomic_epoch_is_exact_under_concurrent_publishers() -> None:
    from schema_sanitizer.core_impl.atomic_epoch import AtomicEpoch

    counter = AtomicEpoch()
    workers = 8
    per_worker = 1000
    threads = [
        threading.Thread(target=lambda: [counter.increment() for _ in range(per_worker)])
        for _ in range(workers)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert counter.value() == workers * per_worker


def test_reserved_owner_activity_is_published_before_slot_visibility() -> None:
    source = (ROOT / "src/schema_sanitizer/core_impl/finalizer_escrow.py").read_text()
    start = source.index(
        "    def reserve_ticket(self)", source.index("class ReservedFinalizerEscrow")
    )
    end = source.index("\n    def release_ticket", start)
    body = source[start:end]
    assert body.index("self._active_counter.increment()") < body.index(
        "self._states[slot] = _RESERVED"
    )


def test_transfer_stage_rolls_back_every_failure_after_ticket_reservation() -> None:
    source = (ROOT / "src/schema_sanitizer/core_impl/memory_budget.py").read_text()
    start = source.index("    def transfer_stage(self")
    end = source.index("\n    def ", start + 8)
    body = source[start:end]
    reserve = body.index("reserve_rooted(successor_owner)")
    successor = body.index("object.__new__", reserve)
    assert reserve < successor
    # Owner-first admission has an identity rollback even if the returned ticket
    # is interrupted before caller publication; post-construction transfer uses
    # the authenticated retirement helper.
    assert "release_rooted_owner(successor_owner)" in body[reserve:successor]
    assert "retire_or_ack_rooted_finalizer_authority(" in body[successor:]


def test_process_resource_release_and_shrink_prepare_before_capability_commit() -> None:
    source = (ROOT / "src/schema_sanitizer/core_impl/process_resources.py").read_text()
    release_start = source.index("    def _release_lease_entry")
    release_end = source.index("\n    def ", release_start + 8)
    release = source[release_start:release_end]
    assert release.index("next_in_use") < release.index("self._active_leases.pop")
    shrink_start = source.index("    def _shrink_lease")
    shrink_end = source.index("\n    def ", shrink_start + 8)
    shrink = source[shrink_start:shrink_end]
    assert shrink.index("next_in_use") < shrink.index("entry.amount =")


def test_retry_subsystem_charge_rolls_back_partial_mapping_and_ticket() -> None:
    source = (ROOT / "src/schema_sanitizer/core_impl/retry_scheduler.py").read_text()
    start = source.index("    def _add_subsystem_charge_locked")
    end = source.index("\n    @staticmethod", start)
    body = source[start:end]
    assert "self._subsystem_counts.pop" in body
    assert "self._subsystem_bytes.pop" in body
    assert "release_control_plane(ticket)" in body


def test_required_memory_is_acquired_before_thread_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import memory_budget as module
    from schema_sanitizer.core_impl import process_resources
    from schema_sanitizer.errors import SchemaSanitizerResourceError

    class Lease:
        def __init__(self, owner: "Ledger") -> None:
            self.owner = owner

        def resize(self, amount: int) -> None:
            self.owner.resizes.append(amount)

        def close(self) -> None:
            pass

    class Ledger:
        def __init__(self) -> None:
            self.calls: list[int] = []
            self.resizes: list[int] = []

        def acquire(self, amount: int, *, stage: str):
            self.calls.append(amount)
            return Lease(self)

    ledger = Ledger()
    monkeypatch.setattr(module, "adaptive_parallel_slots", lambda *a, **k: 2)
    monkeypatch.setattr(module, "current_operation_memory_ledger", lambda: ledger)

    def fail_threads(*_a, **_k):
        raise SchemaSanitizerResourceError("threads exhausted")

    monkeypatch.setattr(process_resources, "acquire_project_threads", fail_threads)
    admission = module.acquire_parallel_admission(
        2, per_slot_bytes=128, stage="ephemeral-reserved-escrow-is-not-rooted", require_memory=True
    )
    try:
        assert admission.slots == 1
        assert admission.memory_lease is not None
        assert ledger.calls == [256]
        assert ledger.resizes == [128]
    finally:
        admission.close()


def test_cross_process_direct_lease_uses_bounded_slots_and_mutable_entry() -> None:
    source = (ROOT / "src/schema_sanitizer/core_impl/cross_process_memory.py").read_text()
    assert "_MAX_DIRECT_LEASES = 4096" in source
    assert "_DIRECT_LEASE_FREE" in source
    assert "entry.reserved = reserved" in source
    assert "_DIRECT_LEASE_SEQUENCE" not in source


def test_cross_process_constructor_is_terminal_safe_before_finalizer_ticket() -> None:
    source = (ROOT / "src/schema_sanitizer/core_impl/cross_process_memory.py").read_text()
    start = source.index("class CrossProcessMemoryLease")
    init = source.index("    def __init__", start)
    end = source.index("\n    @property", init)
    body = source[init:end]
    ticket = body.index("reserve_rooted(owner)")
    assert body.index("self._released = True") < ticket
    assert body.index("self._lease_id = 0") < ticket
    # Owner is installed before the integer handoff; constructor rollback can
    # authenticate and retire the exact rooted authority.
    assert body.index("self._finalizer_owner = owner") < ticket
    assert "release_ticket(ticket)" in body[ticket:]


def test_finalizer_freeze_builds_complete_view_before_publication() -> None:
    source = (ROOT / "src/schema_sanitizer/core_impl/finalizer_registry.py").read_text()
    start = source.index("def freeze_finalizer_registry")
    end = source.index("\ndef ", start + 5)
    body = source[start:end]
    assert body.index("frozen_escrows") < body.index("_FROZEN_DOMAINS = domains")
    assert body.index("_FROZEN_DOMAINS = domains") < body.index("_REGISTRY_FROZEN = True")


def test_static_registration_computes_total_before_publish_and_rolls_back_shadow_failure() -> None:
    source = (ROOT / "src/schema_sanitizer/core_impl/static_control_plane.py").read_text()
    start = source.index("def reserve_static_control_plane")
    end = source.index("\ndef rollback_static_control_plane", start)
    body = source[start:end]
    assert body.index("next_total = authoritative + amount") < body.index("_ENTRIES[kind] = amount")
    assert "_ENTRIES.pop(kind, None)" in body
    assert "sync_locked()" in body


def test_resident_snapshot_is_pure_and_admission_serialized() -> None:
    source = (ROOT / "src/schema_sanitizer/core_impl/memory_budget.py").read_text()
    start = source.index("def process_resident_memory_snapshot")
    end = source.index("\ndef ", start + 5)
    body = source[start:end]
    assert "with _GOVERNED_MEMORY_ADMISSION_LOCK" in body
    assert "synchronize_control_plane_native_shadow" not in body


def test_control_plane_release_recycles_token_without_growable_append() -> None:
    source = (ROOT / "src/schema_sanitizer/core_impl/control_plane_budget.py").read_text()
    release_start = source.index(
        "    def _release_capability(", source.index("class _ProcessControlPlaneBudget")
    )
    release_end = source.index("\n    def ", release_start + 8)
    body = source[release_start:release_end]
    assert "_free_tokens.append" not in body
    assert "self._free_tokens[recycle_tail] = token" in body


def test_registry_and_sequence_caps_are_explicit() -> None:
    finalizers = (ROOT / "src/schema_sanitizer/core_impl/finalizer_registry.py").read_text()
    shutdown = (ROOT / "src/schema_sanitizer/core_impl/shutdown_observers.py").read_text()
    contracts = (ROOT / "src/schema_sanitizer/core_impl/concurrency_contracts.py").read_text()
    io = (ROOT / "src/schema_sanitizer/remote_impl/io_permits.py").read_text()
    provider = (ROOT / "src/schema_sanitizer/remote_impl/provider_throttle.py").read_text()
    assert "_MAX_FINALIZER_DOMAINS" in finalizers
    assert "_MAX_SHUTDOWN_OBSERVERS" in shutdown
    assert "_MAX_CONCURRENCY_CONTRACTS" in contracts
    assert "remote I/O capability generation exhausted" in io
    assert "remote I/O registration generation exhausted" in io
    assert "next_reusable_token(self._lease_sequence, self._active_leases)" in provider


def test_fork_manager_is_single_bounded_dispatch_registry_for_static_escrows() -> None:
    manager = (ROOT / "src/schema_sanitizer/core_impl/fork_manager.py").read_text()
    escrow = (ROOT / "src/schema_sanitizer/core_impl/finalizer_escrow.py").read_text()
    assert "_MAX_FORK_HANDLERS" in manager
    assert "os.register_at_fork" in manager
    assert "register_fork_handler(" in escrow
    assert "os.register_at_fork" not in escrow


def test_janitor_child_reset_uses_prepared_bank_not_new_sync_objects() -> None:
    source = (ROOT / "src/schema_sanitizer/core_impl/temporary_janitor.py").read_text()
    start = source.index("    def reset_after_fork(self)")
    end = source.index("\n\n_JANITOR =", start)
    body = source[start:end]
    assert "prepared = self._fork_prepared" in body
    assert "threading.Lock()" not in body
    assert "threading.Condition(" not in body
    assert "threading.Event()" not in body


def test_governed_headroom_does_not_double_subtract_control_plane() -> None:
    source = (ROOT / "src/schema_sanitizer/core_impl/memory_budget.py").read_text()
    start = source.index("def process_memory_pressure_snapshot")
    end = source.index("\ndef ", start + 5)
    body = source[start:end]
    assert "exact.capacity_bytes - exact.reserved_bytes" in body


def test_real_public_conversion_entrypoints_use_exact_pair_admission_scope() -> None:
    converter = (ROOT / "src/schema_sanitizer/api_impl/file_conversion/converters.py").read_text()
    analytical = (ROOT / "src/schema_sanitizer/api_impl/analytical.py").read_text()
    for source in (converter, analytical):
        assert "activate_runtime_concurrency_pair_admission" in source
        assert "memory_ledger=operation_context.memory_ledger" in source
        assert "transfer_to_output()" in source
        assert "pair_scope.close()" in source


def test_all_56_pairs_use_the_exact_production_pair_admission_when_native_available() -> None:
    from types import ModuleType

    from schema_sanitizer.core_impl.native_runtime import native_core

    if not isinstance(native_core, ModuleType):
        pytest.skip("source-only runtime cannot execute the real resident ledger")

    from schema_sanitizer.core_impl.concurrency_contracts import (
        activate_runtime_concurrency_pair_admission,
    )
    from schema_sanitizer.core_impl.concurrency_coverage import (
        INPUT_CONCURRENCY_COVERAGE,
        OUTPUT_CONCURRENCY_COVERAGE,
        validate_observed_concurrency_pair_contracts,
    )
    from schema_sanitizer.core_impl.memory_budget import OperationMemoryLedger

    ledger = OperationMemoryLedger(8 << 20)
    try:
        for input_format in INPUT_CONCURRENCY_COVERAGE:
            for output_format in OUTPUT_CONCURRENCY_COVERAGE:
                scope = activate_runtime_concurrency_pair_admission(
                    input_format, output_format, memory_ledger=ledger
                )
                try:
                    scope.transfer_to_output()
                finally:
                    scope.close()
        assert validate_observed_concurrency_pair_contracts() == 49
    finally:
        ledger.close()


def test_callable_contract_distinguishes_defaults_and_captured_owner_identity() -> None:
    from schema_sanitizer.core_impl.callable_contract import callable_contract

    def default_one(value: int = 1) -> int:
        return value

    def default_two(value: int = 2) -> int:
        return value

    assert callable_contract(default_one) != callable_contract(default_two)

    class Owner:
        pass

    def make(owner: Owner):
        def callback() -> object:
            return owner

        return callback

    assert callable_contract(make(Owner())) != callable_contract(make(Owner()))


def test_fork_manager_requires_explicit_opt_in_for_unprepared_child_callbacks() -> None:
    source = (ROOT / "src/schema_sanitizer/core_impl/fork_manager.py").read_text()
    assert "child_safe_without_prepare" in source
    assert "if callback is None:" in source
    assert 'handler.mode == "child_safe" and handler.child_safe_without_prepare' in source
    assert "contract_generation" in source


def test_static_escrow_footprint_is_reserved_before_allocating_slot_banks() -> None:
    source = (ROOT / "src/schema_sanitizer/core_impl/finalizer_escrow.py").read_text()
    start = source.index(
        "    def __init__(self, capacity: int, *, static_kind",
        source.index("class ReservedFinalizerEscrow"),
    )
    end = source.index("\n    def _make_fresh_bank", start)
    body = source[start:end]
    assert body.index("static_guard = _static_footprint_guard") < body.index(
        "self._slots: list[object]"
    )
    assert body.index("static_guard.commit()") > body.index(
        "self._fork_spare2 = self._make_fresh_bank()"
    )


def test_production_pair_boundary_releases_base_credit_before_result_diagnostics() -> None:
    for relative in (
        "src/schema_sanitizer/api_impl/file_conversion/converters.py",
        "src/schema_sanitizer/api_impl/analytical.py",
    ):
        source = (ROOT / relative).read_text()
        transfer = source.index("pair_scope.transfer_to_output()")
        close = source.index("pair_scope.close()", transfer)
        assert transfer < close
        assert "pair_scope = None" in source[close : close + 500]


def test_cross_process_resize_commits_host_state_into_preallocated_mutable_entry() -> None:
    source = (ROOT / "src/schema_sanitizer/core_impl/cross_process_memory.py").read_text()
    start = source.index("    def resize(self", source.index("class CrossProcessMemoryLease"))
    end = source.index("\n    def ", start + 8)
    body = source[start:end]
    assert "_update_direct_lease_reserved" in body
    update_start = source.index("def _update_direct_lease_reserved")
    update_end = source.index("\ndef ", update_start + 5)
    update = source[update_start:update_end]
    assert "entry.reserved = reserved" in update
    assert "(" not in update[update.index("entry.reserved = reserved") :].splitlines()[0]
