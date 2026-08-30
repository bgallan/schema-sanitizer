"""Tests first admission and commit-critical helpers across external-pool identity,
residency uncertainty, finalizer capsules, staged cleanup, control-plane metadata,
native stack or descriptor snapshots, release evidence, diagnostics, and bounded waiter
polling. The coordinator lock is never held while first admission waits; helpers publish
one preallocated owner and snapshots preserve conservative stack debt and exact domain
visibility."""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.usefixtures("isolated_external_runtime_coordinator")

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src/schema_sanitizer"
CPP = ROOT / "cpp/src"


def _clear_external(module) -> None:
    """Clear cached external-runtime state before the lifecycle check."""
    with module._EXTERNAL_RUNTIME_POOL_COORDINATOR_LOCK:
        module._EXTERNAL_RUNTIME_POOL_COORDINATOR.clear()


def test_logical_pool_first_admission_never_waits_under_coordinator_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify logical pool first admission never waits under coordinator lock."""
    from schema_sanitizer.core_impl import process_resources as module

    _clear_external(module)
    observed = {"lock_free": False, "released": 0}

    class FakeLease:
        amount = 2

        def shrink(self, amount: int) -> None:
            """Set the fake lease to the requested amount."""
            self.amount = amount

        def release(self) -> None:
            """Release the resource held by the fake lease test double."""
            observed["released"] += 1

    def acquire_project_threads(*_args, **_kwargs):
        """Acquire the projected thread permits through the logical pool."""
        acquired = module._EXTERNAL_RUNTIME_POOL_COORDINATOR_LOCK.acquire(blocking=False)
        observed["lock_free"] = acquired
        if acquired:
            module._EXTERNAL_RUNTIME_POOL_COORDINATOR_LOCK.release()
        return FakeLease()

    monkeypatch.setattr(module, "acquire_project_threads", acquire_project_threads)
    runtime = object()
    claim = module._acquire_shared_external_logical_thread_lease(runtime, 2)
    assert observed["lock_free"] is True
    assert claim.amount == 2
    claim.release()
    assert observed["released"] == 1
    assert module.external_runtime_pool_snapshot()["logical_claims"] == 0
    _clear_external(module)


def test_unknown_resident_probe_preserves_identity_and_stack_debt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify unknown resident probe preserves identity and stack debt."""
    from schema_sanitizer.core_impl import process_resources as module

    _clear_external(module)
    state = [3]
    events: list[tuple[str, int]] = []

    class Runtime:
        @staticmethod
        def schema_sanitizer_resident_thread_count() -> int:
            """Return the resident width or raise when the probe is unavailable."""
            if state[0] < 0:
                raise RuntimeError("probe unavailable")
            return state[0]

    class Native:
        supports_resident_attribution = True
        supports_stack_debt = True
        supports_atomic_residency_update = True

        def __init__(self) -> None:
            """Initialize the native test double."""
            self.leases: dict[object, int] = {}

        def acquire_exact_permit_lease(
            self, desired: int, minimum: int
        ) -> tuple[object, int] | None:
            """Acquire the fake exact-permit lease requested by the resource owner."""
            if desired < minimum:
                return None
            receipt = object()
            self.leases[receipt] = desired
            return receipt, desired

        def exact_permit_lease_amount(self, receipt: object) -> int:
            """Return the exact permit amount tracked by the fake lease."""
            return self.leases[receipt]

        def resize_exact_permit_lease(self, receipt: object, target: int) -> int:
            """Resize the fake exact-permit lease to the requested amount."""
            current = self.leases[receipt]
            if current > target:
                events.append(("claim-release", current - target))
            self.leases[receipt] = target
            return target

        def external_runtime_residency_update(self, identity_delta: int, debt_delta: int) -> None:
            """Record identity and stack-debt additions or releases in order."""
            if identity_delta > 0:
                events.append(("identity-add", identity_delta))
            elif identity_delta < 0:
                events.append(("identity-release", -identity_delta))
            if debt_delta > 0:
                events.append(("debt-add", debt_delta))
            elif debt_delta < 0:
                events.append(("debt-release", -debt_delta))

    monkeypatch.setattr(module, "_native_external_thread_api", lambda: Native())

    first = module._acquire_shared_external_native_thread_permits(Runtime, 1)
    assert first.amount == 1 and first.owner is not None
    first.owner.release()
    snap = module.external_runtime_pool_snapshot()
    assert snap["resident_width"] == 3
    assert snap["resident_stack_debt"] == 3

    state[0] = -1
    second = module._acquire_shared_external_native_thread_permits(Runtime, 1)
    assert second.amount == 1 and second.owner is not None
    second.owner.release()
    snap = module.external_runtime_pool_snapshot()
    assert snap["resident_width"] == 3
    assert snap["resident_stack_debt"] == 3
    assert ("identity-release", 3) not in events
    assert ("debt-release", 3) not in events

    state[0] = 0
    third = module._acquire_shared_external_native_thread_permits(Runtime, 1)
    assert third.amount == 1 and third.owner is not None
    third.owner.release()
    assert ("debt-release", 3) in events
    assert module.external_runtime_pool_snapshot()["coordinator_entries"] == 0


def test_external_runtime_identity_is_namespaced_and_known_integrations_are_sealed() -> None:
    """Verify external runtime identity is namespaced and known integrations are sealed."""
    from schema_sanitizer.core_impl import process_resources as module

    class ProviderA:
        @staticmethod
        def schema_sanitizer_thread_pool_identity() -> str:
            """Return the controlled native thread-pool identity."""
            return "global"

    class ProviderB:
        @staticmethod
        def schema_sanitizer_thread_pool_identity() -> str:
            """Return the controlled native thread-pool identity."""
            return "global"

    assert module._external_runtime_pool_identity_key(
        ProviderA
    ) != module._external_runtime_pool_identity_key(ProviderB)

    class FakeModule:
        def __init__(self, name: str) -> None:
            """Initialize the fake module test double."""
            self.__name__ = name

    pyarrow_a = FakeModule("pyarrow")
    pyarrow_b = FakeModule("pyarrow")
    polars = FakeModule("polars")
    # A module name is not authority: arbitrary wrappers that merely claim a
    # well-known __name__ stay isolated by object identity. The registry seals known
    # integrations only to the canonical object registered in sys.modules.
    assert module._external_runtime_pool_identity_key(
        pyarrow_a
    ) != module._external_runtime_pool_identity_key(pyarrow_b)
    assert module._external_runtime_pool_identity_key(
        pyarrow_a
    ) != module._external_runtime_pool_identity_key(polars)
    assert "pyarrow" in module._EXTERNAL_RUNTIME_INTEGRATIONS
    assert "polars" in module._EXTERNAL_RUNTIME_INTEGRATIONS


def test_allocation_after_commit_critical_helpers_return_single_preallocated_owner() -> None:
    """Verify allocation after commit critical helpers return single preallocated owner."""
    resources = (SRC / "core_impl/process_resources.py").read_text(encoding="utf-8")
    memory = (SRC / "core_impl/memory_budget.py").read_text(encoding="utf-8")
    cross = (SRC / "core_impl/cross_process_memory.py").read_text(encoding="utf-8")
    storage = (SRC / "core_impl/temporary_storage.py").read_text(encoding="utf-8")
    remote = (SRC / "remote_impl/io_permits.py").read_text(encoding="utf-8")

    assert "class _ExternalNativePermitAcquisition" in resources
    assert "result = _ExternalNativePermitAcquisition()" in resources
    assert "class _ExternalRuntimeBorrowResult" in resources
    assert "class _StageControlReservation" in memory
    assert "class _MemoryLeaseRegistration" in memory
    assert "class _DirectLeaseRegistration" in cross
    assert "class _StorageLeasePublication" in storage
    assert "class _StorageResizeResult" in storage
    assert "class _CapabilityPublication" in remote

    shared = resources[
        resources.index("def _acquire_shared_external_native_thread_permits") : resources.index(
            "class _SharedExternalRuntimeLogicalLease"
        )
    ]
    assert "return result" in shared
    assert "return permit, granted_width" not in shared
    resize = storage[
        storage.index("    def _resize_lease(\n") : storage.index(
            "    def _release_lease_authority"
        )
    ]
    assert "result = _StorageResizeResult" in resize
    assert "return result" in resize
    assert "return requested, target_key, target_path" not in resize


def test_production_finalizer_paths_use_single_capsule_api() -> None:
    """Verify production finalizer paths use single capsule API."""
    forbidden_calls = {
        "prepare_finalizer_cleanup",
        "prepare_resource_finalizer_cleanup",
        "prepare_detached_resources_finalizer_cleanup",
        "prepare_reference_finalizer_cleanup",
    }
    for path in SRC.rglob("*.py"):
        if path.name == "finalizer_cleanup.py":
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
                continue
            assert node.func.id not in forbidden_calls, f"tuple finalizer call in {path}"

    finalizers = (SRC / "core_impl/finalizer_cleanup.py").read_text(encoding="utf-8")
    reserve = finalizers[
        finalizers.index("def reserve_finalizer_cleanup") : finalizers.index(
            "def reserve_detached_resources_finalizer_cleanup"
        )
    ]
    assert reserve.index("PreparedFinalizerCleanup(callback)") < reserve.index("reserve_rooted(")
    assert "return capsule" in reserve


def test_staged_cleanup_claim_has_no_multivalue_post_commit_return() -> None:
    """Verify staged cleanup claim has no multivalue post commit return."""
    source = (SRC / "remote_impl/staged_ownership.py").read_text(encoding="utf-8")
    body = source[
        source.index("    def _claim_cleanup_locked") : source.index("    def _finish_cleanup")
    ]
    assert "self._cleanup_inflight = True" in body
    assert "return staged" in body
    assert "return staged," not in body
    cleanup = source[source.index("    def _cleanup_published") : source.index("    def publish")]
    assert "generation = self._cleanup_generation" in cleanup


def test_external_pool_static_control_plane_estimates_cover_measured_metadata() -> None:
    """Verify external pool static control plane estimates cover measured metadata."""
    from schema_sanitizer.core_impl import process_resources as module

    entry = module._ExternalRuntimePoolCoordinatorEntry(runtime=None, runtime_key=("runtime", 1))
    measured_entry = sum(
        sys.getsizeof(value)
        for value in (entry, entry.physical_claims, entry.logical_claims, entry.runtime_key)
    )
    assert module._EXTERNAL_RUNTIME_POOL_ENTRY_CONTROL_BYTES >= measured_entry * 2

    empty: dict[int, int] = {}
    before = sys.getsizeof(empty)
    empty[1] = 1
    measured_claim = (sys.getsizeof(empty) - before) + sys.getsizeof(1)
    assert module._EXTERNAL_RUNTIME_POOL_CLAIM_CONTROL_BYTES >= measured_claim


def test_safety_critical_commit_helpers_have_no_multivalue_container_builds() -> None:
    """Verify safety critical commit helpers have no multivalue container builds."""
    import dis

    from schema_sanitizer.core_impl import cross_process_memory as cross
    from schema_sanitizer.core_impl import memory_budget as memory
    from schema_sanitizer.core_impl import process_resources as resources
    from schema_sanitizer.core_impl import temporary_storage as storage
    from schema_sanitizer.remote_impl import io_permits as remote
    from schema_sanitizer.remote_impl import staged_ownership as staged

    functions = (
        resources._acquire_external_native_thread_permits,
        resources._acquire_shared_external_native_thread_permits,
        resources._Lease.borrow_external_runtime_threads,
        memory._reserve_stage_control,
        memory.OperationMemoryLedger._register_python_lease,
        cross._register_direct_lease,
        storage.TemporaryStoragePermitPool._publish_lease_locked,
        storage.TemporaryStoragePermitPool._resize_lease,
        remote.RemoteIoPermitGovernor._publish_capability_locked,
        staged.StagedResultOwnership._claim_cleanup_locked,
    )
    forbidden = {"BUILD_TUPLE", "BUILD_LIST", "BUILD_MAP", "BUILD_SET"}
    for function in functions:
        instructions = tuple(dis.get_instructions(function))
        if function is resources._acquire_shared_external_native_thread_permits:
            # This helper may construct structured diagnostics while waiting for
            # a third-party configuration generation, before it owns anything.
            # The allocation-free region starts when the preallocated claim
            # owner is constructed immediately before slot publication.
            commit_start = next(
                index
                for index, instruction in enumerate(instructions)
                if instruction.opname == "LOAD_GLOBAL"
                and "_SharedExternalRuntimeNativePermit" in instruction.argrepr
            )
            instructions = instructions[commit_start:]
        elif function is storage.TemporaryStoragePermitPool._resize_lease:
            # Limit diagnostics are also prepared before any retry ownership is
            # published.  The first retry-metadata STORE_ATTR is the resize
            # commit boundary guarded by this bytecode contract.
            commit_start = next(
                index
                for index, instruction in enumerate(instructions)
                if instruction.opname == "STORE_ATTR"
                and instruction.argrepr == "resize_target_bytes"
            )
            instructions = instructions[commit_start:]
        bad = [
            instruction.opname for instruction in instructions if instruction.opname in forbidden
        ]
        assert not bad, f"{function.__qualname__} allocates {bad} around ownership commit"


def test_native_stack_pid_memory_fd_and_fifo_contracts() -> None:
    """Verify native stack PID memory FD and fifo contracts."""
    arena = (CPP / "internal/runtime/operation_task_arena.cc").read_text(encoding="utf-8")
    header = (CPP / "internal/runtime/operation_task_arena.hh").read_text(encoding="utf-8")
    probe = (CPP / "api/python_abi3/runtime/ordered_executor_probe.cc").read_text(encoding="utf-8")

    assert "std::max<std::uint64_t>(kDefault" in arena
    assert "ProcessRlimitThreadHeadroom" in arena
    assert 'effective_headroom(\n        "pids", "pids.max", "pids.current")' in arena
    assert "GlobalMemoryStatusEx" in arena
    assert "host_statistics64" in arena
    assert "PROC_PIDTASKALLINFO" in arena
    assert "process_file_descriptor_count" in arena
    assert "g_process_fd_next_ticket" in arena
    assert "g_process_fd_serving_ticket" in arena
    assert "kExternalObservationPoll" in arena
    assert "g_process_external_runtime_stack_debt_threads" in arena
    assert "external_runtime_stack_debt_threads" in header
    assert "kSaneSingleObservation = 65536U" in arena
    resident_add = arena[
        arena.index("void add_process_external_runtime_resident_threads") : arena.index(
            "void release_process_external_runtime_resident_threads"
        )
    ]
    assert resident_add.index("amount > kSaneSingleObservation") < resident_add.index(
        "compare_exchange_weak"
    )
    assert "PyTuple_New(30)" in probe
    assert "process_thread_stack_reservation_bytes" in probe
    assert "py_process_file_descriptor_count" in probe
    resources = (SRC / "core_impl/process_resources.py").read_text(encoding="utf-8")
    open_fd = resources[
        resources.index("def _open_fd_count") : resources.index("def _fd_requested_capacity")
    ]
    assert "process_file_descriptor_count" in open_fd


def test_release_gate_requires_stack_debt_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify release gate requires stack debt schema."""
    from schema_sanitizer.core_impl import concurrency_coverage as coverage
    from schema_sanitizer.core_impl import runtime_diagnostics

    base = {
        "available": True,
        "snapshot_schema_fields": 30,
        "completion_memory_protocol_violations": 0,
        "counter_underflows": 0,
        "native_physical_threads": 2,
        "external_runtime_thread_permits": 1,
        "total_physical_thread_permits": 3,
        "native_physical_thread_capacity": 8,
        "thread_permit_snapshot_stable": 1,
        "external_runtime_resident_protocol_violations": 0,
        "external_runtime_resident_threads": 2,
        "external_runtime_stack_debt_threads": 2,
    }
    monkeypatch.setattr(runtime_diagnostics, "_native_arena_snapshot", lambda: dict(base))
    coverage.validate_native_concurrency_protocol_health()

    missing = dict(base)
    missing.pop("external_runtime_stack_debt_threads")
    monkeypatch.setattr(runtime_diagnostics, "_native_arena_snapshot", lambda: missing)
    with pytest.raises(RuntimeError, match="external_runtime_stack_debt_threads"):
        coverage.validate_native_concurrency_protocol_health()

    underaccounted = dict(base, external_runtime_stack_debt_threads=1)
    monkeypatch.setattr(runtime_diagnostics, "_native_arena_snapshot", lambda: underaccounted)
    with pytest.raises(RuntimeError, match="below resident identity"):
        coverage.validate_native_concurrency_protocol_health()


def test_shutdown_and_debug_snapshots_include_external_runtime_pool_domain() -> None:
    """Verify shutdown and debug snapshots include external runtime pool domain."""
    shutdown = (SRC / "core_impl/runtime_shutdown.py").read_text(encoding="utf-8")
    diagnostics = (SRC / "core_impl/runtime_diagnostics.py").read_text(encoding="utf-8")
    resources = (SRC / "core_impl/process_resources.py").read_text(encoding="utf-8")

    assert '"external_runtime_pools"' in shutdown
    assert "external_runtime_physical_claims" in shutdown
    assert "external_runtime_logical_claims" in shutdown
    assert '"external_runtime_pools": external_pools' in diagnostics
    assert '"version": 8' in diagnostics
    assert '_register_shutdown_observer("external_runtime_pools"' in resources


def test_runtime_diagnostics_accepts_30_field_native_snapshot() -> None:
    """Verify runtime diagnostics accepts 30 field native snapshot."""
    source = (SRC / "core_impl/runtime_diagnostics.py").read_text(encoding="utf-8")
    assert "len(values) != 30" in source
    assert '"external_runtime_stack_debt_threads"' in source


def test_fd_native_waiter_uses_bounded_external_observation_poll() -> None:
    """Verify FD native waiter uses bounded external observation poll."""
    arena = (CPP / "internal/runtime/operation_task_arena.cc").read_text(encoding="utf-8")
    start = arena.index("std::size_t acquire_process_file_descriptor_permits_wait")
    end = arena.index("void release_process_file_descriptor_permits", start)
    body = arena[start:end]
    assert "ticket" in body
    assert "g_process_fd_serving_ticket" in body
    assert "kExternalObservationPoll" in body
    assert "wait_until" in body or "wait_for" in body
