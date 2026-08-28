"""Regression coverage for memory native memory receipt closes commit publication window."""

from __future__ import annotations

from pathlib import Path


def _root() -> Path:
    return Path(__file__).resolve().parents[2]


def test_native_memory_receipt_closes_commit_publication_window() -> None:
    source = (_root() / "cpp/src/api/python_abi3/options/prepare.cc").read_text()
    catalog = (_root() / "cpp/src/internal/abi/python_abi3/method_catalog.inc").read_text()
    memory = (_root() / "src/schema_sanitizer/core_impl/memory_budget.py").read_text()

    assert "kOperationMemoryReservationCapsuleName" in source
    assert "reservation->bytes = bytes;" in source
    assert "reservation->bytes = requested;" in source
    assert "reservation->release();" in source
    assert "operation_memory_reservation_create," in catalog
    assert "operation_memory_reservation_resize," in catalog
    assert "_prepare_python_lease" in memory
    assert "_commit_python_lease_reservation" in memory
    assert "entry.native_receipt = receipt" in memory


def test_memory_release_uses_receipt_not_mirrored_amount() -> None:
    memory = (_root() / "src/schema_sanitizer/core_impl/memory_budget.py").read_text()
    start = memory.index("    def _release_python_lease_authority(")
    end = memory.index("    def _release_python_lease(", start)
    block = memory[start:end]
    assert "self._native_reservation_release(receipt)" in block
    assert "current.native_receipt = None" in block
    assert "if amount == 0:" in block


def test_external_runtime_permits_have_native_raii_receipt() -> None:
    header = (_root() / "cpp/src/internal/runtime/operation_task_arena.hh").read_text()
    probe = (_root() / "cpp/src/api/python_abi3/runtime/ordered_executor_probe.cc").read_text()
    resources = (_root() / "src/schema_sanitizer/core_impl/process_resources.py").read_text()

    assert "class ProcessExternalRuntimeThreadPermitLease final" in header
    assert "~ProcessExternalRuntimeThreadPermitLease() noexcept { reset(); }" in header
    assert "process_external_runtime_thread_permit_lease_acquire" in probe
    assert "process_external_runtime_thread_permit_lease_resize" in probe
    assert "native_lease: object | None = None" in resources
    assert "_sync_external_native_lease_amount_locked" in resources


def test_start_governed_native_thread_uses_raii_owner() -> None:
    arena = (_root() / "cpp/src/internal/runtime/operation_task_arena.cc").read_text()
    start = arena.index("[[nodiscard]] std::thread StartGovernedNativeThread")
    end = arena.index("bool TryAcquireReaperThreadPermit", start)
    block = arena[start:end]
    assert "ProcessPhysicalThreadPermitLease permit(1U);" in block
    assert "permit = std::move(permit)" in block
    assert "ReleaseNativePhysicalThreadPermit();" not in block
    assert "TryAcquireNativePhysicalThreadPermit()" not in block


def test_external_retry_resynchronizes_from_native_owner() -> None:
    from schema_sanitizer.core_impl import process_resources as module

    class Native:
        supports_exact_permit_lease = True

        def exact_permit_lease_amount(self, lease: object) -> int:
            return int(getattr(lease, "amount"))

    class Lease:
        amount = 2

    entry = module._ExternalRuntimePoolCoordinatorEntry(runtime=None)
    entry.native = Native()
    entry.native_lease = Lease()
    entry.physical_amount = 9
    module._sync_external_native_lease_amount_locked(entry)
    assert entry.physical_amount == 2
