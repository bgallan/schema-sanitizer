"""Regression coverage for memory fd native bridge uses exact raii receipt."""

from __future__ import annotations

from pathlib import Path

import pytest


def _root() -> Path:
    return Path(__file__).resolve().parents[2]


def test_fd_native_bridge_uses_exact_raii_receipt() -> None:
    header = (_root() / "cpp/src/internal/runtime/process_fd_governor.hh").read_text()
    probe = (_root() / "cpp/src/api/python_abi3/runtime/ordered_executor_probe.cc").read_text()
    catalog = (_root() / "cpp/src/internal/abi/python_abi3/method_catalog.inc").read_text()
    resources = (_root() / "src/schema_sanitizer/core_impl/process_resources.py").read_text()

    assert "TryAcquireUpToWait" in header
    assert "bool shrink(std::size_t target)" in header
    assert "kFdPermitLeaseCapsuleName" in probe
    assert "process_file_descriptor_permit_lease_acquire_wait" in catalog
    assert "native_fd_lease: object | None = None" in resources
    assert "_native_fd_exact_resize" in resources


class _Receipt:
    def __init__(self, amount: int) -> None:
        self.receipt_id = id(self)
        self.generation = 1
        self.amount = amount
        self.opened = 0


class _ExactFdNative:
    def __init__(self) -> None:
        self.fail_after_resize = False
        self.receipts: list[_Receipt] = []

    def process_file_descriptor_permit_lease_acquire_wait(
        self, desired: int, minimum: int, timeout_ms: int
    ) -> tuple[_Receipt, int] | None:
        del timeout_ms
        if desired < minimum:
            return None
        receipt = _Receipt(desired)
        self.receipts.append(receipt)
        return receipt, desired

    def process_file_descriptor_permit_lease_metadata(
        self, receipt: _Receipt
    ) -> tuple[int, int, int, int]:
        return receipt.receipt_id, receipt.generation, receipt.amount, receipt.opened

    def process_file_descriptor_permit_lease_resize(
        self, receipt: _Receipt, target: int, generation: int
    ) -> tuple[int, int, int]:
        assert generation == receipt.generation
        if target > receipt.amount:
            raise ValueError("cannot grow")
        receipt.amount = target
        receipt.generation += 1
        if self.fail_after_resize:
            self.fail_after_resize = False
            raise KeyboardInterrupt("fault after exact native FD commit")
        return receipt.generation, receipt.amount, receipt.opened


def _fresh_fd_governor(module):
    return module._Governor(16, "test_fds", teardown_reserve=2)


def test_fd_release_retry_after_native_commit_is_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import process_resources as module

    native = _ExactFdNative()
    governor = _fresh_fd_governor(module)
    monkeypatch.setattr(module, "_FD_GOVERNOR", governor)
    monkeypatch.setattr(module, "_native_file_descriptor_api", lambda: native)
    monkeypatch.setattr(module, "_refresh_fd_governor_capacity", lambda: None)

    lease = module._acquire_file_descriptor_lease(2, timeout_seconds=1.0, teardown=False)
    receipt = native.receipts[-1]
    entry = governor._active_leases[lease.lease_id]
    assert entry.native_fd_lease is receipt
    assert receipt.amount == 2

    native.fail_after_resize = True
    with pytest.raises(KeyboardInterrupt):
        lease.release()

    # Native commit happened, but Python authority remains rooted for retry.
    entry = governor._active_leases[lease.lease_id]
    assert receipt.amount == 0
    assert entry.native_fd_amount == 2
    assert not entry.resource_released

    lease.release()
    assert receipt.amount == 0
    assert lease.lease_id not in governor._active_leases


def test_fd_shrink_retry_targets_final_width_not_stale_delta(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import process_resources as module

    native = _ExactFdNative()
    governor = _fresh_fd_governor(module)
    monkeypatch.setattr(module, "_FD_GOVERNOR", governor)
    monkeypatch.setattr(module, "_native_file_descriptor_api", lambda: native)
    monkeypatch.setattr(module, "_refresh_fd_governor_capacity", lambda: None)

    lease = module._acquire_file_descriptor_lease(2, timeout_seconds=1.0, teardown=False)
    receipt = native.receipts[-1]
    native.fail_after_resize = True
    with pytest.raises(KeyboardInterrupt):
        lease.shrink(1)

    assert receipt.amount == 1
    assert governor._active_leases[lease.lease_id].amount == 2

    # Retry must be a native no-op at target=1, not subtract another permit.
    lease.shrink(1)
    assert receipt.amount == 1
    assert governor._active_leases[lease.lease_id].amount == 1
    lease.release()


def test_external_shared_claim_rollback_retires_partial_exact_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import process_resources as module

    class Injected(RuntimeError):
        pass

    class FailCommitDict(dict[int, int]):
        def __setitem__(self, key: int, value: int) -> None:
            if int(value) > 0:
                raise Injected("fault before physical claim commit")
            super().__setitem__(key, value)

    class Native:
        supports_exact_permit_lease = True
        supports_stack_debt = False
        supports_resident_attribution = False
        supports_atomic_residency_update = False

        def __init__(self) -> None:
            self.receipt: _Receipt | None = None

        def acquire_exact_permit_lease(self, desired: int, minimum: int):
            assert desired >= minimum
            self.receipt = _Receipt(desired)
            return self.receipt, desired

        def resize_exact_permit_lease(self, lease: _Receipt, target: int) -> int:
            lease.amount = target
            return target

        def exact_permit_lease_amount(self, lease: _Receipt) -> int:
            return lease.amount

    original_entry_type = module._ExternalRuntimePoolCoordinatorEntry

    def make_entry(*, runtime=None, runtime_key=None, **kwargs):
        return original_entry_type(
            runtime=runtime,
            runtime_key=runtime_key,
            physical_claims=FailCommitDict(),
            **kwargs,
        )

    native = Native()
    module._EXTERNAL_RUNTIME_POOL_COORDINATOR.clear()
    module._EXTERNAL_RUNTIME_TOTAL_PHYSICAL_CLAIMS = 0
    module._EXTERNAL_RUNTIME_TOTAL_LOGICAL_CLAIMS = 0
    monkeypatch.setattr(module, "_ExternalRuntimePoolCoordinatorEntry", make_entry)
    monkeypatch.setattr(module, "_native_external_thread_api", lambda: native)
    monkeypatch.setattr(module, "_reported_external_runtime_resident_width", lambda _r: None)
    monkeypatch.setattr(module, "_reported_external_runtime_stack_debt_width", lambda _r, _w: None)

    class Runtime:
        pass

    with pytest.raises(Injected):
        module._acquire_shared_external_native_thread_permits(Runtime(), 2)

    assert native.receipt is not None
    assert native.receipt.amount == 0
    assert module._EXTERNAL_RUNTIME_TOTAL_PHYSICAL_CLAIMS == 0
    assert not module._EXTERNAL_RUNTIME_POOL_COORDINATOR


def test_nonshared_external_runtime_path_prefers_exact_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import process_resources as module

    class Native:
        supports_exact_permit_lease = True

        def __init__(self) -> None:
            self.receipt = _Receipt(0)

        def acquire_exact_permit_lease(self, desired: int, minimum: int):
            assert desired == minimum
            self.receipt.amount = desired
            return self.receipt, desired

        def resize_exact_permit_lease(self, lease: _Receipt, target: int) -> None:
            lease.amount = target

        def exact_permit_lease_amount(self, lease: _Receipt) -> int:
            return lease.amount

    native = Native()
    monkeypatch.setattr(module, "_native_external_thread_api", lambda: native)
    result = module._acquire_external_native_thread_permits(3)
    assert isinstance(result.owner, module._ExactExternalRuntimeNativePermit)
    assert result.amount == 3
    result.owner.resize_physical_thread_permits(0)
    assert native.receipt.amount == 0


def test_receipt_mutators_reject_inherited_process_and_memory_has_provenance() -> None:
    prepare = (_root() / "cpp/src/api/python_abi3/options/prepare.cc").read_text()
    probe = (_root() / "cpp/src/api/python_abi3/runtime/ordered_executor_probe.cc").read_text()

    assert "reservation_id" in prepare
    assert "generation" in prepare
    assert "owner_process" in prepare
    assert "owner_process_matches()" in prepare
    assert "operation memory reservation cannot be resized after fork" in prepare
    assert "operation memory reservation cannot be released after fork" in prepare
    assert "external runtime permit lease cannot be mutated after fork" in probe
    assert "file descriptor permit lease cannot be mutated after fork" in probe


def test_nonshared_external_runtime_shrink_retry_is_target_idempotent() -> None:
    from schema_sanitizer.core_impl import process_resources as module

    class Native:
        supports_exact_permit_lease = True

        def __init__(self) -> None:
            self.receipt = _Receipt(4)
            self.fail_after_resize = True

        def exact_permit_lease_amount(self, lease: _Receipt) -> int:
            return lease.amount

        def resize_exact_permit_lease(self, lease: _Receipt, target: int) -> None:
            lease.amount = target
            if self.fail_after_resize:
                self.fail_after_resize = False
                raise KeyboardInterrupt("fault after external exact shrink commit")

    native = Native()
    owner = module._ExactExternalRuntimeNativePermit(native, native.receipt)
    runtime_lease = module.ExternalRuntimeConcurrencyLease(
        None, workers=4, parallel=True, native=owner
    )

    with pytest.raises(KeyboardInterrupt):
        runtime_lease.shrink_to(2)
    assert native.receipt.amount == 2
    assert owner.amount == 2

    runtime_lease.shrink_to(2)
    assert native.receipt.amount == 2
    assert owner.amount == 2
    runtime_lease.close()


def test_external_cleanup_state_uses_owner_object_as_authority() -> None:
    resources = (_root() / "src/schema_sanitizer/core_impl/process_resources.py").read_text()
    assert "native: Any | None = None" in resources
    assert "native.resize_physical_thread_permits(0)" in resources
    assert "state.native = self._native" in resources
