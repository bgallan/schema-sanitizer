"""Tests repair of stale physical or logical aggregate counters alongside changing
residency generations, deferred-close tails, duplicate uncertain descriptor debt,
opening attempts, external resize, and ABI mutators. Exact slot membership and returned
post-commit generation state override stale mirrors, preventing replay while repairing
terminal publication."""

from __future__ import annotations

import os
from pathlib import Path
from threading import Lock
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.usefixtures("isolated_external_runtime_coordinator")


def _root() -> Path:
    """Return the repository root used by source-contract checks."""
    return Path(__file__).resolve().parents[2]


def _reset_external(module) -> None:
    """Reset cached external-runtime state between lifecycle checks."""
    module.drain_finalizer_cleanup()
    module._EXTERNAL_RUNTIME_POOL_COORDINATOR.clear()
    module._EXTERNAL_RUNTIME_TOTAL_PHYSICAL_CLAIMS = 0
    module._EXTERNAL_RUNTIME_TOTAL_LOGICAL_CLAIMS = 0


def test_physical_claim_cleanup_repairs_stale_low_aggregate_counter() -> None:
    """Verify physical claim cleanup repairs stale low aggregate counter."""
    from schema_sanitizer.core_impl import process_resources as module

    class Receipt:
        amount = 2

    class Native:
        supports_exact_permit_lease = True

        def exact_permit_lease_amount(self, receipt: Receipt) -> int:
            """Return the exact permit amount tracked by the fake lease."""
            return receipt.amount

        def resize_exact_permit_lease(self, receipt: Receipt, target: int) -> int:
            """Resize the fake exact-permit lease to the requested amount."""
            receipt.amount = int(target)
            return receipt.amount

    _reset_external(module)
    key = ("declared", ("physical-claim-cleanup-repairs-stale-low", "physical-counter-repair"))
    receipt = Receipt()
    entry = module._ExternalRuntimePoolCoordinatorEntry(
        runtime=None,
        runtime_key=key,
        native=Native(),
        native_lease=receipt,
        physical_amount=2,
        physical_claims={1: 2},
    )
    module._EXTERNAL_RUNTIME_POOL_COORDINATOR[key] = entry
    module._EXTERNAL_RUNTIME_TOTAL_PHYSICAL_CLAIMS = 0  # injected split-publication fault

    module._resize_shared_external_native_thread_claim(key, 1, 0)
    assert receipt.amount == 0
    assert module._EXTERNAL_RUNTIME_TOTAL_PHYSICAL_CLAIMS == 0
    assert key not in module._EXTERNAL_RUNTIME_POOL_COORDINATOR


def test_logical_claim_cleanup_repairs_stale_low_aggregate_counter() -> None:
    """Verify logical claim cleanup repairs stale low aggregate counter."""
    from schema_sanitizer.core_impl import process_resources as module

    class Lease:
        amount = 2
        _released = False

        def release(self) -> None:
            """Release the resource held by the lease test double."""
            self.amount = 0
            self._released = True

        def shrink(self, target: int) -> None:
            """Set the lease to the requested target amount."""
            self.amount = int(target)

    _reset_external(module)
    key = ("declared", ("physical-claim-cleanup-repairs-stale-low", "logical-counter-repair"))
    entry = module._ExternalRuntimePoolCoordinatorEntry(
        runtime=None,
        runtime_key=key,
        logical_lease=Lease(),  # type: ignore[arg-type]
        logical_width=2,
        logical_claims={7: 2},
    )
    module._EXTERNAL_RUNTIME_POOL_COORDINATOR[key] = entry
    module._EXTERNAL_RUNTIME_TOTAL_LOGICAL_CLAIMS = 0

    module._resize_shared_external_logical_thread_claim(key, 7, 0)
    assert module._EXTERNAL_RUNTIME_TOTAL_LOGICAL_CLAIMS == 0
    assert key not in module._EXTERNAL_RUNTIME_POOL_COORDINATOR


def test_residency_probe_retries_when_config_generation_changes() -> None:
    """Verify residency probe retries when config generation changes."""
    from schema_sanitizer.core_impl import process_resources as module

    _reset_external(module)
    key = ("declared", ("physical-claim-cleanup-repairs-stale-low", "stable-probe"))
    calls = {"probe": 0}

    class Runtime:
        def schema_sanitizer_resident_thread_count(self) -> int:
            """Force one generation change, then report a stable width on retry."""
            calls["probe"] += 1
            if calls["probe"] == 1:
                # Simulate a complete reconfiguration while the arbitrary runtime
                # callback executes outside the coordinator lock.
                with module._EXTERNAL_RUNTIME_POOL_COORDINATOR_CONDITION:
                    entry = module._EXTERNAL_RUNTIME_POOL_COORDINATOR[key]
                    entry.config_generation += 1
                return 1
            return 8

    class Native:
        supports_atomic_residency_update = True
        supports_resident_attribution = True
        supports_stack_debt = True

        def __init__(self) -> None:
            """Initialize the native test double."""
            self.identity = 0
            self.debt = 0

        def external_runtime_residency_update(self, identity_delta: int, debt_delta: int) -> None:
            """Apply identity and stack-debt deltas to the native counters."""
            self.identity += int(identity_delta)
            self.debt += int(debt_delta)

    runtime = Runtime()
    native = Native()
    module._EXTERNAL_RUNTIME_POOL_COORDINATOR[key] = module._ExternalRuntimePoolCoordinatorEntry(
        runtime=None,
        runtime_key=key,
    )
    # Use the declared identity key chosen by the test rather than object identity.
    module._refresh_external_runtime_residency_stable(runtime, native, key)
    assert calls["probe"] == 2
    assert native.identity == 8
    assert native.debt == 8


def test_exact_memory_release_is_not_replayed_when_deferred_close_tail_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify exact memory release is not replayed when deferred close tail fails."""
    from schema_sanitizer.core_impl import memory_budget as module

    ledger = object.__new__(module.OperationMemoryLedger)
    ledger._pid = os.getpid()
    ledger._lock = Lock()
    ledger._unknown_python_lease_releases = 0
    ledger._post_release_observation_failures = 0
    ledger._cross_process_release_deferred = True
    cap = object()
    owner = SimpleNamespace(_lease_id=1, _capability=cap)
    entry = module._PythonMemoryLeaseEntry(
        id(owner), cap, 0, None, physical_size_bytes=0, physical_released=True
    )
    ledger._python_leases = {1: entry}

    monkeypatch.setattr(
        module.OperationMemoryLedger,
        "_maybe_finish_deferred_close",
        lambda self: (_ for _ in ()).throw(OSError("journal fault after child commit")),
    )

    ledger._release_python_lease(owner)
    assert ledger._python_leases == {}
    assert ledger._unknown_python_lease_releases == 0
    assert ledger._post_release_observation_failures == 1
    assert ledger._cross_process_release_deferred is True


def test_uncertain_fd_duplicate_repairs_count_and_terminal_publication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify uncertain FD duplicate repairs count and terminal publication."""
    from schema_sanitizer.core_impl import process_resources as module

    governor = module._Governor(4, "physical-claim-cleanup-repairs-stale-low-uncertain-fd")
    monkeypatch.setattr(module, "_FD_GOVERNOR", governor)
    lease = governor.try_acquire_up_to(1, minimum=1)
    monkeypatch.setattr(
        module,
        "_UNCERTAIN_FD_CLOSE_DEBTS",
        [module._UncertainFdCloseDebtSlot() for _ in range(governor.capacity)],
    )
    monkeypatch.setattr(module, "_UNCERTAIN_FD_CLOSE_COUNT", 0)
    published: list[tuple[str, int]] = []
    monkeypatch.setattr(
        module,
        "publish_terminal_owner",
        lambda domain, key, **_kwargs: published.append((domain, key)),
    )
    monkeypatch.setattr(module, "diagnostic_transition", lambda: None)

    assert module.retain_uncertain_fd_close(lease, label="physical-claim-cleanup-repairs-stale-low")
    # Inject loss of the derived counter after exact slot publication.
    monkeypatch.setattr(module, "_UNCERTAIN_FD_CLOSE_COUNT", 0)
    assert module.retain_uncertain_fd_close(lease, label="physical-claim-cleanup-repairs-stale-low")
    assert module._UNCERTAIN_FD_CLOSE_COUNT == 1
    assert published[-1] == ("uncertain_fd_close", id(lease))


def test_fd_opening_attempt_membership_is_authoritative() -> None:
    """Verify FD opening attempt membership is authoritative."""
    resources = (_root() / "src/schema_sanitizer/core_impl/process_resources.py").read_text()
    assert "_opening_attempts" in resources
    assert "self._opening_attempts.discard(attempt)" in resources
    assert "if self._opening_attempts:" in resources
    assert "external runtime aggregate physical claim underflow" not in resources
    assert "external runtime aggregate logical claim underflow" not in resources


def test_external_exact_resize_consumes_post_commit_state_without_second_metadata_read() -> None:
    """Verify external exact resize consumes post commit state without second metadata read."""
    from schema_sanitizer.core_impl import process_resources as module

    calls = {"metadata": 0}
    lease = object()

    def metadata(_lease):
        """Build the metadata snapshot used by the cleanup assertion."""
        calls["metadata"] += 1
        return (11, 7, 3)

    core = SimpleNamespace(
        process_external_runtime_thread_permit_lease_metadata=metadata,
        process_external_runtime_thread_permit_lease_resize=lambda _lease, target, generation: (
            generation + 1,
            target,
        ),
    )
    authority = module._ExternalNativeThreadAuthority(core)
    assert authority.resize_exact_permit_lease(lease, 2) == 2
    assert calls["metadata"] == 1


def test_fd_exact_mutator_consumes_post_commit_state() -> None:
    """Verify FD exact mutator consumes post commit state."""
    from schema_sanitizer.core_impl import process_resources as module

    calls = {"metadata": 0}

    class Native:
        def process_file_descriptor_permit_lease_metadata(self, _receipt):
            """Count the metadata lookup and return the fixed native receipt tuple."""
            calls["metadata"] += 1
            return (4, 9, 2, 0)

        def process_file_descriptor_permit_lease_resize(self, _receipt, target, generation):
            """Resize the controlled FD permit lease."""
            return generation + 1, int(target), 0

    state = module._native_fd_exact_resize(Native(), object(), 1)
    assert state == (10, 1, 0)
    assert calls["metadata"] == 1


def test_exact_abi_mutators_return_post_commit_generation_state() -> None:
    """Verify exact ABI mutators return post commit generation state."""
    prepare = (_root() / "cpp/src/api/python_abi3/options/prepare.cc").read_text()
    probe = (_root() / "cpp/src/api/python_abi3/runtime/ordered_executor_probe.cc").read_text()

    assert "PyLong_FromUnsignedLongLong(reservation->generation)" in prepare
    assert "PyLong_FromLongLong(static_cast<long long>(reservation->bytes))" in prepare
    assert "PyLong_FromUnsignedLongLong(receipt->generation)" in probe
    assert "PyLong_FromSize_t(receipt->lease.opened())" in probe
