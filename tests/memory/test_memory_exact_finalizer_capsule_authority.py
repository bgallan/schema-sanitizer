"""Regression coverage for exact finalizer capsule authority."""

from __future__ import annotations

import ast
import inspect
import sys
import tempfile
import types
from pathlib import Path
from types import SimpleNamespace

import pytest

SRC = Path(__file__).resolve().parents[2] / "src" / "schema_sanitizer"
CPP = Path(__file__).resolve().parents[2] / "cpp" / "src"


def test_production_finalizer_callsites_use_capsule_as_single_authority() -> None:
    violations: list[str] = []
    for path in SRC.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = (
                func.id
                if isinstance(func, ast.Name)
                else (func.attr if isinstance(func, ast.Attribute) else "")
            )
            if (
                name in {"cancel_prepared_finalizer_cleanup", "defer_prepared_finalizer_cleanup"}
                and len(node.args) > 1
            ):
                violations.append(f"{path.relative_to(SRC)}:{node.lineno}:{name}")
    assert violations == []


def test_reserved_finalizer_escrow_uses_predecoded_exact_ticket_metadata() -> None:
    from schema_sanitizer.core_impl import finalizer_escrow as module

    publish = inspect.getsource(module.ReservedFinalizerEscrow.publish_reserved)
    release = inspect.getsource(module.ReservedFinalizerEscrow.release_ticket)
    reserve = inspect.getsource(module.ReservedFinalizerEscrow.reserve_ticket)
    assert "_decode_ticket" not in publish
    assert "_decode_ticket" not in release
    assert "_ticket_slots.get" in publish
    assert "_ticket_slots.get" in release
    assert "_ticket_slots[ticket] = slot" in reserve


def test_process_temporary_storage_capability_is_exact_and_non_replayable() -> None:
    from schema_sanitizer.core_impl.temporary_storage_governor import (
        ProcessTemporaryStorageCapability,
        _ProcessTemporaryStorageGovernor,
    )

    governor = _ProcessTemporaryStorageGovernor()
    path = tempfile.gettempdir()
    first = governor.reserve_capability(
        1, path=path, label="exact-finalizer-capsule-authority-first"
    )
    second = governor.reserve_capability(
        1, path=path, label="exact-finalizer-capsule-authority-second"
    )
    assert governor.authoritative_snapshot().reserved_bytes == 2
    assert governor.release_capability(first) is True
    assert governor.release_capability(first) is False
    assert governor.authoritative_snapshot().reserved_bytes == 1

    forged = ProcessTemporaryStorageCapability(governor)
    forged.token = second.token
    forged.device = second.device
    forged.reserved_bytes = second.reserved_bytes
    forged.active = True
    assert governor.release_capability(forged) is False
    assert governor.authoritative_snapshot().reserved_bytes == 1
    assert governor.release_capability(second) is True
    assert governor.authoritative_snapshot().reserved_bytes == 0


def test_temporary_storage_production_paths_use_exact_process_capabilities() -> None:
    source = (SRC / "core_impl/temporary_storage.py").read_text(encoding="utf-8")
    governor = (SRC / "core_impl/temporary_storage_governor.py").read_text(encoding="utf-8")
    assert "reserve_capability(" in source
    assert "release_capability(" in source
    assert "resize_capability(" in source
    assert "self._capabilities.get(capability.token) is not capability" in governor
    assert "_prepublish_capability" in governor
    assert "    def reserve(" not in governor
    assert "    def release(" not in governor
    assert "legacy_reserved" not in governor
    assert "_PROCESS_TEMPORARY_STORAGE.reserve(" not in source
    assert "_PROCESS_TEMPORARY_STORAGE.release(" not in source


def test_remote_io_release_precomputes_critical_accounting_before_owner_pop() -> None:
    from schema_sanitizer.remote_impl import io_permits as module

    source = inspect.getsource(module.RemoteIoPermitGovernor._release_permit_capability)
    pop_at = source.index("_permit_owners.pop")
    for marker in ("next_in_use =", "next_over_count =", "next_over_weight ="):
        assert source.index(marker) < pop_at


def test_fake_pyarrow_name_cannot_inherit_sealed_pool_identity() -> None:
    from schema_sanitizer.core_impl import process_resources as module

    class Fake:
        __name__ = "pyarrow"

    class Fake2:
        __name__ = "pyarrow"

    assert module._external_runtime_integration(Fake) is None
    assert module._external_runtime_pool_identity_key(
        Fake
    ) != module._external_runtime_pool_identity_key(Fake2)


def test_canonical_runtime_integration_uses_configured_width_as_stack_debt_only() -> None:
    from schema_sanitizer.core_impl import process_resources as module

    canonical = types.ModuleType("pyarrow")
    canonical.cpu_count = lambda: 4  # type: ignore[attr-defined]
    canonical.set_cpu_count = lambda value: None  # type: ignore[attr-defined]
    prior = sys.modules.get("pyarrow")
    sys.modules["pyarrow"] = canonical
    try:
        assert module._external_runtime_integration(canonical) is not None
        assert module._external_runtime_pool_identity_key(canonical)[0] == "integration"
        assert module._reported_external_runtime_resident_width(canonical) is None
        assert module._reported_external_runtime_stack_debt_width(canonical, None) == 4
    finally:
        if prior is None:
            sys.modules.pop("pyarrow", None)
        else:
            sys.modules["pyarrow"] = prior


def test_external_residency_fallback_order_is_always_memory_conservative() -> None:
    from schema_sanitizer.core_impl import process_resources as module

    events: list[tuple[str, int]] = []

    class Native:
        supports_resident_attribution = True
        supports_stack_debt = True
        supports_atomic_residency_update = False

        def external_runtime_stack_debt_threads_add(self, amount: int) -> None:
            events.append(("debt-add", amount))

        def external_runtime_stack_debt_threads_release(self, amount: int) -> None:
            events.append(("debt-release", amount))

        def external_runtime_resident_threads_add(self, amount: int) -> None:
            events.append(("identity-add", amount))

        def external_runtime_resident_threads_release(self, amount: int) -> None:
            events.append(("identity-release", amount))

    entry = module._ExternalRuntimePoolCoordinatorEntry(runtime=object())
    module._set_external_runtime_resident_width_locked(entry, Native(), 3, stack_debt_target=3)
    assert events[:2] == [("debt-add", 3), ("identity-add", 3)]
    events.clear()
    module._set_external_runtime_resident_width_locked(entry, Native(), 0, stack_debt_target=0)
    assert events[:2] == [("identity-release", 3), ("debt-release", 3)]


def test_external_runtime_configuration_reentrancy_fails_closed_without_deadlock() -> None:
    from schema_sanitizer.core_impl import process_resources as module
    from schema_sanitizer.errors import SchemaSanitizerResourceError

    class Runtime:
        value = 4
        entered = False

        @classmethod
        def cpu_count(cls) -> int:
            if not cls.entered:
                cls.entered = True
                module.constrain_external_runtime_worker_pool(cls, 2)
            return cls.value

        @classmethod
        def set_cpu_count(cls, value: int) -> None:
            cls.value = value

    with pytest.raises(SchemaSanitizerResourceError):
        module.constrain_external_runtime_worker_pool(Runtime, 2)
    key = module._external_runtime_pool_identity_key(Runtime)
    with module._EXTERNAL_RUNTIME_POOL_COORDINATOR_LOCK:
        assert key not in module._EXTERNAL_RUNTIME_POOL_COORDINATOR


def test_external_runtime_claim_totals_are_o1_not_global_scan() -> None:
    from schema_sanitizer.core_impl import process_resources as module

    source = inspect.getsource(module._external_runtime_total_claims_locked)
    assert ".values()" not in source
    assert "_EXTERNAL_RUNTIME_CLAIM_SLOTS.exact_active_count()" in source
    assert "_EXTERNAL_RUNTIME_TOTAL_PHYSICAL_CLAIMS" not in source
    assert "_EXTERNAL_RUNTIME_TOTAL_LOGICAL_CLAIMS" not in source


def test_native_fd_fifo_capacity_is_ticket_backlog_bounded() -> None:
    source = (CPP / "internal/runtime/operation_task_arena.cc").read_text(encoding="utf-8")
    assert "bool TryReserveFdTicket" in source
    assert "const auto backlog = next - serving" in source
    assert "backlog >= static_cast<std::uint64_t>(kProcessFdTicketSlots - 1U)" in source
    assert "if (!TryReserveFdTicket(&ticket))" in source


def test_native_external_residency_has_joint_epoch_update() -> None:
    arena = (CPP / "internal/runtime/operation_task_arena.cc").read_text(encoding="utf-8")
    abi = (CPP / "api/python_abi3/runtime/ordered_executor_probe.cc").read_text(encoding="utf-8")
    assert "update_process_external_runtime_residency" in arena
    body = arena[arena.index("void update_process_external_runtime_residency") :]
    assert body.index("BeginThreadPermitLedgerMutation") < body.index(
        "EndThreadPermitLedgerMutation"
    )
    assert "process_external_runtime_residency_update" in abi


def test_linux_rlimit_nproc_uses_same_uid_thread_headroom() -> None:
    source = (CPP / "internal/runtime/operation_task_arena.cc").read_text(encoding="utf-8")
    assert "ProcessRlimitThreadHeadroom" in source
    assert '::opendir("/proc")' in source
    assert 'std::strncmp(line, "Uid:"' in source
    assert 'std::strncmp(line, "Threads:"' in source
    assert "total_reserved + *rlimit_headroom" in source


def test_remote_io_fault_before_commit_preserves_authoritative_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.remote_impl import io_permits as module

    governor = module.RemoteIoPermitGovernor(2)
    capability = object()
    governor._permit_owners[1] = module._CapabilityEntry(1, capability, 1)
    governor._in_use = 1

    def fail_max(*_args: object, **_kwargs: object) -> int:
        raise MemoryError("exact-finalizer-capsule-authority injected precommit OOM")

    monkeypatch.setattr(module, "max", fail_max, raising=False)
    with pytest.raises(MemoryError, match="exact-finalizer-capsule-authority"):
        governor._release_permit_capability(1, capability)
    assert governor._permit_owners[1].capability is capability
    assert governor._in_use == 1
    monkeypatch.delattr(module, "max", raising=False)
    governor._release_permit_capability(1, capability)
    assert governor._in_use == 0
    assert not governor._permit_owners


def test_process_temporary_storage_capabilities_are_quarantined_across_fork_generation() -> None:
    from schema_sanitizer.core_impl.temporary_storage_governor import (
        _ProcessTemporaryStorageGovernor,
    )

    governor = _ProcessTemporaryStorageGovernor()
    capability = governor.reserve_capability(
        1,
        path=tempfile.gettempdir(),
        label="exact-finalizer-capsule-authority-fork",
    )
    governor.prepare_for_fork()
    governor.reset_after_fork()
    assert governor.authoritative_snapshot().reserved_bytes == 0
    assert governor.release_capability(capability) is False


def test_external_runtime_explicit_retirement_clears_known_stack_debt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import process_resources as module
    from schema_sanitizer.core_impl.bounded_generation import BoundedGenerationPool

    events: list[tuple[str, int]] = []

    class Runtime:
        @staticmethod
        def schema_sanitizer_resident_thread_count() -> int:
            return 2

    class Native:
        supports_resident_attribution = True
        supports_stack_debt = True
        supports_atomic_residency_update = False

        def acquire_exact_permit_lease(self, desired: int, minimum: int):
            assert desired >= minimum
            return SimpleNamespace(amount=desired), desired

        @staticmethod
        def resize_exact_permit_lease(lease: object, target: int) -> int:
            previous = lease.amount  # type: ignore[attr-defined]
            lease.amount = target  # type: ignore[attr-defined]
            events.append(("claim-release", previous - target))
            return target

        @staticmethod
        def exact_permit_lease_amount(lease: object) -> int:
            return int(lease.amount)  # type: ignore[attr-defined]

        def external_runtime_stack_debt_threads_add(self, amount: int) -> None:
            events.append(("debt-add", amount))

        def external_runtime_stack_debt_threads_release(self, amount: int) -> None:
            events.append(("debt-release", amount))

        def external_runtime_resident_threads_add(self, amount: int) -> None:
            events.append(("identity-add", amount))

        def external_runtime_resident_threads_release(self, amount: int) -> None:
            events.append(("identity-release", amount))

    native = Native()
    module.drain_finalizer_cleanup()
    monkeypatch.setattr(
        module,
        "_EXTERNAL_RUNTIME_POOL_COORDINATOR",
        module._ExternalRuntimeCoordinator(),
    )
    monkeypatch.setattr(
        module,
        "_EXTERNAL_RUNTIME_CLAIM_SLOTS",
        BoundedGenerationPool(module._MAX_EXTERNAL_RUNTIME_POOL_TOTAL_CLAIMS),
    )
    monkeypatch.setattr(module, "_EXTERNAL_RUNTIME_TOTAL_PHYSICAL_CLAIMS", 0)
    monkeypatch.setattr(module, "_EXTERNAL_RUNTIME_TOTAL_LOGICAL_CLAIMS", 0)
    monkeypatch.setattr(module, "_native_external_thread_api", lambda: native)
    result = module._acquire_shared_external_native_thread_permits(Runtime, 1)
    assert result.owner is not None
    result.owner.resize_physical_thread_permits(0)
    before = module.external_runtime_pool_snapshot()
    assert before["resident_width"] == 2
    assert before["resident_stack_debt"] == 2
    assert module.retire_external_runtime_pool(Runtime) is True
    after = module.external_runtime_pool_snapshot()
    assert after["resident_width"] == 0
    assert after["resident_stack_debt"] == 0
    assert after["coordinator_entries"] == 0


def test_temporary_storage_borrow_state_oom_does_not_strand_capability_inflight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl.temporary_storage_governor import (
        _ProcessTemporaryStorageGovernor,
    )

    governor = _ProcessTemporaryStorageGovernor()
    capability = governor.reserve_capability(
        1,
        path=tempfile.gettempdir(),
        label="exact-finalizer-capsule-authority-borrow-oom",
    )
    original = governor._borrow_state

    def fail_borrow(*_args: object, **_kwargs: object) -> object:
        raise MemoryError("exact-finalizer-capsule-authority borrow-state OOM")

    monkeypatch.setattr(governor, "_borrow_state", fail_borrow)
    with pytest.raises(MemoryError, match="borrow-state"):
        governor.release_capability(capability)
    assert capability.active is True
    assert capability.inflight is False
    monkeypatch.setattr(governor, "_borrow_state", original)
    assert governor.release_capability(capability) is True
