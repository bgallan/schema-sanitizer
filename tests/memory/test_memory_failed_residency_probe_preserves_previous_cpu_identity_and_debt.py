"""Tests conservative recovery from failed or unstable residency probes, generation
exhaustion, physical tombstones, control-plane faults, deferred close, descriptor debt,
ABI result construction, and mirror reconciliation. The last known CPU identity and
stack debt remain authoritative, while exact slots and rooted ledger capability repair
partial commits without treating mirrors as corruption authority."""

from __future__ import annotations

import gc
import os
import re
from pathlib import Path
from threading import Condition, Lock

import pytest


def _root() -> Path:
    """Return the repository root used by source-contract checks."""
    return Path(__file__).resolve().parents[2]


def _reset_external(module) -> None:
    """Reset cached external-runtime state between lifecycle checks."""
    module.drain_finalizer_cleanup()
    module._EXTERNAL_RUNTIME_POOL_COORDINATOR.clear()
    module._EXTERNAL_RUNTIME_TOTAL_PHYSICAL_CLAIMS = 0
    module._EXTERNAL_RUNTIME_TOTAL_LOGICAL_CLAIMS = 0


def test_failed_residency_probe_preserves_previous_cpu_identity_and_debt() -> None:
    """Verify failed residency probe preserves previous CPU identity and debt."""
    from schema_sanitizer.core_impl import process_resources as module

    _reset_external(module)
    key = ("declared", ("failed-residency-probe-preserves-previous-cpu", "probe-failure"))

    class Runtime:
        def schema_sanitizer_resident_thread_count(self) -> int:
            """Raise the deliberate failure for the schema sanitizer resident thread count path."""
            raise RuntimeError("transient probe fault")

    class Native:
        supports_atomic_residency_update = True
        supports_resident_attribution = True
        supports_stack_debt = True

        def __init__(self) -> None:
            """Initialize the native test double."""
            self.identity = 8
            self.debt = 8
            self.updates: list[tuple[int, int]] = []

        def external_runtime_residency_update(self, identity_delta: int, debt_delta: int) -> None:
            """Record and apply identity and stack-debt deltas."""
            self.updates.append((int(identity_delta), int(debt_delta)))
            self.identity += int(identity_delta)
            self.debt += int(debt_delta)

    native = Native()
    entry = module._ExternalRuntimePoolCoordinatorEntry(
        runtime=None,
        runtime_key=key,
        resident_width=8,
        resident_stack_debt=8,
        resident_native=native,
    )
    module._EXTERNAL_RUNTIME_POOL_COORDINATOR[key] = entry

    module._refresh_external_runtime_residency_stable(Runtime(), native, key)
    assert entry.resident_width == 8
    assert entry.resident_stack_debt == 8
    assert native.identity == 8
    assert native.debt == 8
    assert all(delta == (0, 0) for delta in native.updates)


def test_unstable_residency_generation_is_bounded_and_fails_closed() -> None:
    """Verify unstable residency generation is bounded and fails closed."""
    from schema_sanitizer.core_impl import process_resources as module
    from schema_sanitizer.errors import SchemaSanitizerResourceError

    _reset_external(module)
    key = ("declared", ("failed-residency-probe-preserves-previous-cpu", "unstable-probe"))
    calls = 0

    class Runtime:
        def schema_sanitizer_resident_thread_count(self) -> int:
            """Advance the configuration generation during each residency probe."""
            nonlocal calls
            calls += 1
            with module._EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION:
                entry = module._EXTERNAL_RUNTIME_POOL_COORDINATOR[key]
                entry.config_generation += 1
            return 1

    class Native:
        supports_atomic_residency_update = True
        supports_resident_attribution = True
        supports_stack_debt = True

        def __init__(self) -> None:
            """Initialize the native test double."""
            self.identity = 4
            self.debt = 4

        def external_runtime_residency_update(self, identity_delta: int, debt_delta: int) -> None:
            """Apply identity and stack-debt deltas to the native counters."""
            self.identity += int(identity_delta)
            self.debt += int(debt_delta)

    native = Native()
    module._EXTERNAL_RUNTIME_POOL_COORDINATOR[key] = module._ExternalRuntimePoolCoordinatorEntry(
        runtime=None,
        runtime_key=key,
        resident_width=4,
        resident_stack_debt=4,
        resident_native=native,
    )

    with pytest.raises(SchemaSanitizerResourceError, match="stable configuration generation"):
        module._refresh_external_runtime_residency_stable(Runtime(), native, key)
    assert calls == module._MAX_EXTERNAL_RUNTIME_STABLE_PROBE_RETRIES
    assert native.identity == 4
    assert native.debt == 4


def test_external_configuration_generation_exhaustion_fails_before_callbacks() -> None:
    """Verify external configuration generation exhaustion fails before callbacks."""
    from schema_sanitizer.core_impl import process_resources as module
    from schema_sanitizer.errors import SchemaSanitizerResourceError

    _reset_external(module)
    calls = {"get": 0, "set": 0}

    class Runtime:
        def cpu_count(self) -> int:
            """Count the runtime callback invocation and report one CPU."""
            calls["get"] += 1
            return 1

        def set_cpu_count(self, _value: int) -> None:
            """Record the CPU count selected by the controlled runtime."""
            calls["set"] += 1

    runtime = Runtime()
    key = module._external_runtime_pool_identity_key(runtime)
    module._EXTERNAL_RUNTIME_POOL_COORDINATOR[key] = module._ExternalRuntimePoolCoordinatorEntry(
        runtime=runtime,
        runtime_key=key,
        config_generation=module._MAX_EXTERNAL_RUNTIME_CONFIG_GENERATION,
    )

    with pytest.raises(SchemaSanitizerResourceError, match="generation exhausted"):
        module.constrain_external_runtime_worker_pool(runtime, 1)
    assert calls == {"get": 0, "set": 0}


def test_armed_physical_tombstone_retains_finalizer_until_actual_cleanup() -> None:
    """Verify armed physical tombstone retains finalizer until actual cleanup."""
    from schema_sanitizer.core_impl import process_resources as module

    _reset_external(module)
    key = ("declared", ("failed-residency-probe-preserves-previous-cpu", "retry-tombstone"))

    class Receipt:
        amount = 1

    class Native:
        supports_exact_permit_lease = True

        def exact_permit_lease_amount(self, receipt: Receipt) -> int:
            """Return the exact permit amount tracked by the fake lease."""
            return receipt.amount

        def resize_exact_permit_lease(self, receipt: Receipt, target: int) -> int:
            """Resize the fake exact-permit lease to the requested amount."""
            receipt.amount = int(target)
            return receipt.amount

    permit = module._SharedExternalRuntimeNativePermit(key, 0)
    claim_id = module._EXTERNAL_RUNTIME_CLAIM_SLOTS.acquire_for(permit)
    assert claim_id is not None
    permit._bind_claim_id(claim_id)
    receipt = Receipt()
    entry = module._ExternalRuntimePoolCoordinatorEntry(
        runtime=None,
        runtime_key=key,
        native=Native(),
        native_lease=receipt,
        physical_amount=1,
        physical_claims={claim_id: 1},
        config_inflight=True,
        config_owner_thread_id=123456,
    )
    module._EXTERNAL_RUNTIME_POOL_COORDINATOR[key] = entry
    capsule = permit._finalizer_capsule
    assert capsule is not None
    ticket = capsule.ticket
    authority = capsule._authority
    assert module.defer_prepared_finalizer_cleanup(capsule)
    permit._finalizer_ticket = 0
    permit._finalizer_capsule = None

    module.drain_finalizer_cleanup()
    assert entry.physical_claims == {claim_id: 0}
    assert receipt.amount == 1
    # Callback raised the internal retry signal; this exact generation remains
    # armed regardless of unrelated owners entering or leaving the global escrow.
    assert authority.is_armed_for(ticket)
    assert authority._escrow_armed_ticket == ticket

    with module._EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION:
        entry.config_inflight = False
        entry.config_owner_thread_id = None
        module._EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION.notify_all()
    module.drain_finalizer_cleanup()
    assert receipt.amount == 0
    assert claim_id not in entry.physical_claims
    assert module._EXTERNAL_RUNTIME_CLAIM_SLOTS.owner_for(claim_id) is None
    assert not authority.is_armed_for(ticket)
    assert authority._escrow_armed_ticket == 0


def test_manual_target_zero_hands_off_retry_instead_of_waiting() -> None:
    """Verify manual target zero hands off retry instead of waiting."""
    from schema_sanitizer.core_impl import process_resources as module

    _reset_external(module)
    key = ("declared", ("failed-residency-probe-preserves-previous-cpu", "manual-nonblocking"))

    class Receipt:
        amount = 1

    class Native:
        supports_exact_permit_lease = True

        def exact_permit_lease_amount(self, receipt: Receipt) -> int:
            """Return the exact permit amount tracked by the fake lease."""
            return receipt.amount

        def resize_exact_permit_lease(self, receipt: Receipt, target: int) -> int:
            """Resize the fake exact-permit lease to the requested amount."""
            receipt.amount = int(target)
            return receipt.amount

    permit = module._SharedExternalRuntimeNativePermit(key, 0)
    claim_id = module._EXTERNAL_RUNTIME_CLAIM_SLOTS.acquire_for(permit)
    assert claim_id is not None
    permit._bind_claim_id(claim_id)
    receipt = Receipt()
    entry = module._ExternalRuntimePoolCoordinatorEntry(
        runtime=None,
        runtime_key=key,
        native=Native(),
        native_lease=receipt,
        physical_amount=1,
        physical_claims={claim_id: 1},
        config_inflight=True,
        config_owner_thread_id=999999,
    )
    module._EXTERNAL_RUNTIME_POOL_COORDINATOR[key] = entry
    # Must not wait for the third-party configuration owner. The wrapper hands
    # exact retry authority to escrow and returns after publishing target zero.
    permit.release()
    assert permit._released is True
    assert permit._finalizer_ticket == 0
    assert entry.physical_claims == {claim_id: 0}
    assert receipt.amount == 1

    with module._EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION:
        entry.config_inflight = False
        entry.config_owner_thread_id = None
        module._EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION.notify_all()
    module.drain_finalizer_cleanup()
    assert receipt.amount == 0
    assert module._EXTERNAL_RUNTIME_CLAIM_SLOTS.owner_for(claim_id) is None


def test_control_plane_wrapper_loss_requests_exact_capability_retirement() -> None:
    """Verify control plane wrapper loss requests exact capability retirement."""
    from schema_sanitizer.core_impl.control_plane_budget import _ProcessControlPlaneBudget

    budget = _ProcessControlPlaneBudget(include_static_baseline=False)
    ticket = budget.reserve("failed-residency-probe-preserves-previous-cpu-lost-wrapper", 256)
    token = ticket.token
    capability = ticket.capability
    del ticket
    gc.collect()

    assert capability.retire_requested is True
    assert token in budget._owners
    assert budget.drain_requested_retirements(limit=1) == 1
    assert token not in budget._owners
    assert budget.snapshot().reserved_bytes == 0


def test_control_plane_insert_then_raise_keeps_exact_owner_recoverable() -> None:
    """Verify control plane insert then raise keeps exact owner recoverable."""
    from schema_sanitizer.core_impl.control_plane_budget import _ProcessControlPlaneBudget

    class InsertThenRaise(dict):
        fail = True

        def __setitem__(self, key, value):
            """Store the requested value in the insert then raise test double."""
            super().__setitem__(key, value)
            if self.fail:
                self.fail = False
                raise KeyboardInterrupt("after owner-map commit")

    budget = _ProcessControlPlaneBudget(include_static_baseline=False)
    budget._owners = InsertThenRaise()
    with pytest.raises(KeyboardInterrupt):
        budget.reserve("failed-residency-probe-preserves-previous-cpu-insert-fault", 256)

    assert len(budget._owners) == 1
    token, entry = next(iter(budget._owners.items()))
    assert entry.capability.token == token
    assert entry.capability.retire_requested is True
    assert budget.drain_requested_retirements(limit=1) == 1
    assert budget.snapshot().reserved_bytes == 0
    assert budget._corrupted is False


def test_control_plane_pop_then_raise_is_idempotent_release_not_quarantine() -> None:
    """Verify control plane pop then raise is idempotent release not quarantine."""
    from schema_sanitizer.core_impl.control_plane_budget import _ProcessControlPlaneBudget

    class PopThenRaise(dict):
        fail = True

        def pop(self, key, default=None):
            """Remove an owner-map entry, then inject the configured interrupt."""
            value = super().pop(key, default)
            if self.fail:
                self.fail = False
                raise KeyboardInterrupt("after owner-map removal")
            return value

    budget = _ProcessControlPlaneBudget(include_static_baseline=False)
    ticket = budget.reserve("failed-residency-probe-preserves-previous-cpu-pop-fault", 256)
    budget._owners = PopThenRaise(budget._owners)

    assert budget.release(ticket) is True
    assert ticket.released is True
    assert budget.snapshot().reserved_bytes == 0
    assert budget._corrupted is False


def test_memory_deferred_close_tail_retries_from_rooted_ledger_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify memory deferred close tail retries from rooted ledger authority."""
    from schema_sanitizer.core_impl import memory_budget as module
    from schema_sanitizer.core_impl.rooted_finalizer import RootedFinalizerAuthority

    module.drain_abandoned_memory_finalizers()

    class Native:
        def operation_memory_ledger_snapshot(self, _capsule):
            """Return the current operation-memory ledger snapshot."""
            return (1024, 0, 16)

    class Cross:
        def __init__(self) -> None:
            """Initialize the cross test double."""
            self.calls = 0

        def release(self) -> None:
            """Release the resource held by the cross test double."""
            self.calls += 1
            if self.calls == 1:
                raise OSError("first cross-process tail fails")

    ledger = object.__new__(module.OperationMemoryLedger)
    ledger._pid = os.getpid()
    ledger._native = Native()
    ledger._capsule = object()
    ledger._cross_process = Cross()
    ledger._lock = Lock()
    ledger._cross_process_io_lock = Lock()
    ledger._close_condition = Condition(ledger._lock)
    ledger._python_leases = {}
    ledger._close_started = True
    ledger._closing = False
    ledger._closed = False
    ledger._cross_process_release_deferred = True
    ledger._cross_process_pending_bytes = 0
    ledger._cross_process_release_failures = 0
    ledger._post_release_observation_failures = 0
    ledger._deferred_close_cleanup_armed = False
    ledger._close_advisory_recorded = False
    ledger._close_peak_bytes = 0
    owner = RootedFinalizerAuthority(module._run_operation_memory_ledger_finalizer)
    owner.arg0 = ledger._native
    owner.arg1 = ledger._capsule
    owner.arg2 = ledger._cross_process
    ticket = module._MEMORY_LEDGER_FINALIZER_ESCROW.reserve_ticket()
    assert ticket is not None
    owner.ticket = ticket
    assert module._MEMORY_LEDGER_FINALIZER_ESCROW.root_reserved(ticket, owner)
    ledger._finalizer_owner = owner
    ledger._finalizer_ticket = ticket
    monkeypatch.setattr(
        module.OperationMemoryLedger, "_record_close_advisory", staticmethod(lambda _peak: None)
    )

    assert ledger._schedule_deferred_close_cleanup_noexcept() is True
    assert ledger._finalizer_ticket is None
    assert module.drain_abandoned_memory_finalizers() == 0
    assert ledger._cross_process.calls == 1
    assert ledger._cross_process_release_deferred is True
    assert ledger._deferred_close_cleanup_armed is True

    assert module.drain_abandoned_memory_finalizers() == 1
    assert ledger._cross_process.calls == 2
    assert ledger._closed is True
    assert ledger._cross_process_release_deferred is False
    assert ledger._deferred_close_cleanup_armed is False


def test_uncertain_fd_exact_slot_beats_stale_high_counter(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify uncertain FD exact slot beats stale high counter."""
    from schema_sanitizer.core_impl import process_resources as module

    governor = module._Governor(2, "failed-residency-probe-preserves-previous-cpu-uncertain-fd")
    monkeypatch.setattr(module, "_FD_GOVERNOR", governor)
    monkeypatch.setattr(
        module,
        "_UNCERTAIN_FD_CLOSE_DEBTS",
        [module._UncertainFdCloseDebtSlot() for _ in range(governor.capacity)],
    )
    monkeypatch.setattr(module, "publish_terminal_owner", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(module, "diagnostic_transition", lambda: None)
    module._UNCERTAIN_FD_CLOSE_COUNT = governor.capacity  # stale-high derived mirror

    lease = governor.try_acquire_up_to(1, minimum=1)
    assert (
        module.retain_uncertain_fd_close(
            lease, label="failed-residency-probe-preserves-previous-cpu"
        )
        is True
    )
    assert module._UNCERTAIN_FD_CLOSE_COUNT == 1
    assert any(slot.lease is lease for slot in module._UNCERTAIN_FD_CLOSE_DEBTS)


def _assert_preallocated_before_commit(source: str, function_name: str, commit_marker: str) -> None:
    """Assert that storage is preallocated before publication commits."""
    signature = re.compile(rf"(?m)^PyObject\s*\*\s*{re.escape(function_name)}\s*\(")
    match = signature.search(source)
    assert match is not None, f"missing ABI function {function_name}"
    next_match = re.search(r"(?m)^PyObject\s*\*\s*py_\w+\s*\(", source[match.end() :])
    end = len(source) if next_match is None else match.end() + next_match.start()
    body = source[match.start() : end]
    allocation = body.index("PyTuple_New")
    commit = body.index(commit_marker)
    assert allocation < commit
    assert "PyTuple_New" not in body[commit:]
    assert "PyLong_From" not in body[commit:]


def test_exact_abi_builds_all_python_results_before_native_commit() -> None:
    """Verify exact ABI builds all Python results before native commit."""
    prepare = (_root() / "cpp/src/api/python_abi3/options/prepare.cc").read_text()
    probe = (_root() / "cpp/src/api/python_abi3/runtime/ordered_executor_probe.cc").read_text()

    _assert_preallocated_before_commit(
        prepare,
        "py_operation_memory_reservation_resize",
        "reservation->ledger->Reserve",
    )
    _assert_preallocated_before_commit(
        prepare,
        "py_operation_memory_reservation_release",
        "reservation->release()",
    )
    _assert_preallocated_before_commit(
        probe,
        "py_process_external_runtime_thread_permit_lease_resize",
        "receipt->lease.shrink",
    )
    _assert_preallocated_before_commit(
        probe,
        "py_process_file_descriptor_permit_lease_resize",
        "receipt->lease.shrink",
    )
    _assert_preallocated_before_commit(
        probe,
        "py_process_file_descriptor_permit_lease_mark_opened",
        "receipt->lease.mark_opened",
    )
    _assert_preallocated_before_commit(
        probe,
        "py_process_file_descriptor_permit_lease_mark_closed",
        "receipt->lease.mark_closed",
    )


def test_external_claim_cardinality_uses_bounded_exact_slots_not_dict_scan() -> None:
    """Verify external claim cardinality uses bounded exact slots not dict scan."""
    resources = (_root() / "src/schema_sanitizer/core_impl/process_resources.py").read_text()
    start = resources.index("def _external_runtime_total_claims_locked")
    end = resources.index("\ndef ", start + 5)
    body = resources[start:end]
    assert "_EXTERNAL_RUNTIME_CLAIM_SLOTS.exact_active_count()" in body
    assert ".values()" not in body
    assert "BoundedGenerationPool" in resources


def test_control_plane_mirror_reconciliation_is_not_corruption_authority() -> None:
    """Verify control plane mirror reconciliation is not corruption authority."""
    from schema_sanitizer.core_impl.control_plane_budget import _ProcessControlPlaneBudget

    budget = _ProcessControlPlaneBudget(include_static_baseline=False)
    ticket = budget.reserve("failed-residency-probe-preserves-previous-cpu-mirror", 256)
    budget._reserved = 0
    budget._active = 0
    snap = budget.snapshot()
    assert snap.reserved_bytes == 256
    assert snap.active_tickets == 1
    assert budget._corrupted is False
    assert budget.release(ticket)
