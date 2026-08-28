"""Verifies that external-runtime residency updates and individual debits are serialized
under a revalidated writer transaction. Native workers use RAII physical-thread permits
so observation and capacity changes cannot race outside the gate."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CPP = ROOT / "cpp" / "src"
SRC = ROOT / "src" / "schema_sanitizer"


def test_external_runtime_residency_writers_are_serialized_and_revalidate_under_gate() -> None:
    """Verify external runtime residency writers are serialized and revalidate under gate."""
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
    """Verify external runtime individual debits validate inside writer transaction."""
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
    """Verify native worker uses RAII physical thread permit owner."""
    header = (CPP / "internal/runtime/operation_task_arena.hh").read_text(encoding="utf-8")
    worker = (CPP / "internal/runtime/ordered_executor_workers.cc.inc").read_text(encoding="utf-8")
    assert "class ProcessPhysicalThreadPermitLease final" in header
    assert "~ProcessPhysicalThreadPermitLease() noexcept { reset(); }" in header
    assert "ProcessPhysicalThreadPermitLease permit(1U);" in worker
    assert "permit = std::move(permit)" in worker
    assert "release_process_physical_thread_permits(1U)" not in worker
