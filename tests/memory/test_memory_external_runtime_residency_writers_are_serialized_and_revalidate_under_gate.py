"""Regression coverage for memory external runtime residency writers are serialized and revalidate under gate."""

from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CPP = ROOT / "cpp" / "src"
SRC = ROOT / "src" / "schema_sanitizer"


def test_external_runtime_residency_writers_are_serialized_and_revalidate_under_gate() -> None:
    arena = (CPP / "internal/runtime/operation_task_arena.cc").read_text(encoding="utf-8")
    assert "std::atomic_flag g_external_runtime_residency_writer" in arena
    assert "class ExternalRuntimeResidencyWriterGuard final" in arena
    body = arena[arena.index("void update_process_external_runtime_residency") :]
    gate = body.index("ExternalRuntimeResidencyWriterGuard writer")
    current = body.index("const auto current_identity")
    validate = body.index("target_debt < target_identity")
    publish = body.index("g_process_external_runtime_stack_debt_threads.store")
    assert gate < current < validate < publish
    assert body.index("ExternalRuntimeResidencyHealthy()", gate) < current


def test_external_runtime_individual_debits_validate_inside_writer_transaction() -> None:
    arena = (CPP / "internal/runtime/operation_task_arena.cc").read_text(encoding="utf-8")
    resident = arena[
        arena.index("void release_process_external_runtime_resident_threads") : arena.index(
            "void add_process_external_runtime_stack_debt_threads"
        )
    ]
    debt = arena[
        arena.index("void release_process_external_runtime_stack_debt_threads") : arena.index(
            "void update_process_external_runtime_residency"
        )
    ]
    assert resident.index("ExternalRuntimeResidencyWriterGuard writer") < resident.index(
        "amount > current"
    )
    assert "QuarantineExternalRuntimeResidency" in resident
    assert debt.index("ExternalRuntimeResidencyWriterGuard writer") < debt.index(
        "debt - amount < identity"
    )
    assert "SaturatingAtomicSubtract" not in resident
    assert "SaturatingAtomicSubtract" not in debt


def test_native_worker_uses_raii_physical_thread_permit_owner() -> None:
    header = (CPP / "internal/runtime/operation_task_arena.hh").read_text(encoding="utf-8")
    worker = (CPP / "internal/runtime/ordered_executor_workers.cc.inc").read_text(encoding="utf-8")
    assert "class ProcessPhysicalThreadPermitLease final" in header
    assert "~ProcessPhysicalThreadPermitLease() noexcept { reset(); }" in header
    assert "ProcessPhysicalThreadPermitLease permit(1U);" in worker
    assert "permit = std::move(permit)" in worker
    assert "release_process_physical_thread_permits(1U)" not in worker


def test_production_memory_resize_cannot_fall_back_to_amount_authority() -> None:
    source = (SRC / "core_impl/memory_budget.py").read_text(encoding="utf-8")
    resize = source[
        source.index("    def resize(self, size_bytes: int)") : source.index(
            "    def transfer_stage", source.index("    def resize(self, size_bytes: int)")
        )
    ]
    assert "_requires_exact_python_lease_authority" in resize
    assert "production operation memory lease lost exact resize authority" in resize


def test_failed_memory_registration_retries_exact_authority_before_amount_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import memory_budget as module

    class Ledger:
        def __init__(self) -> None:
            self.reserved = 0
            self.release_calls = 0
            self.register_calls = 0
            self.entries: dict[int, tuple[int, object, int]] = {}

        def reserve(self, amount: int, *, stage: str) -> None:
            self.reserved += amount

        def _register_python_lease(self, owner, size):
            self.register_calls += 1
            if self.register_calls == 1:
                raise MemoryError("registration fault")
            capability = object()
            self.entries[1] = (id(owner), capability, size)
            result = module._MemoryLeaseRegistration()
            result.lease_id = 1
            result.capability = capability
            return result

        def release(self, amount: int, **_kwargs) -> None:
            self.release_calls += 1
            if self.release_calls == 1:
                raise MemoryError("rollback fault")
            self.reserved -= amount

        def _release_python_lease_authority(self, lease_id, owner_id, capability):
            current_owner, current_capability, amount = self.entries.pop(lease_id)
            assert current_owner == owner_id
            assert current_capability is capability
            self.release(amount)

    ledger = Ledger()
    with pytest.raises(MemoryError, match="registration fault"):
        module.OperationMemoryLease(ledger, 77, "external-runtime-residency-writers-are-serialized")
    module.drain_abandoned_memory_finalizers()
    assert ledger.register_calls == 2
    assert ledger.reserved == 0
    assert ledger.release_calls == 2
