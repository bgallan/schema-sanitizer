"""Validates one atomic total for native thread domains across external-runtime claims,
resident widths, stage rollback, protocol validation, route evidence, Parquet fork
identity, and keepalive tombstones. Reported width is not mistaken for OS-thread
identity; failed release remains retryable and transport-specific ownership stays
verifiable."""

from __future__ import annotations

import weakref
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src" / "schema_sanitizer"
CPP = ROOT / "cpp" / "src"


def test_native_thread_domains_commit_through_one_atomic_total() -> None:
    """Verify native thread domains commit through one atomic total."""
    arena = (CPP / "internal/runtime/operation_task_arena.cc").read_text(encoding="utf-8")
    header = (CPP / "internal/runtime/operation_task_arena.hh").read_text(encoding="utf-8")
    probe = (CPP / "api/python_abi3/runtime/ordered_executor_probe.cc").read_text(encoding="utf-8")

    assert "g_process_total_thread_permits" in arena
    assert "g_process_total_thread_permits.compare_exchange_weak" in arena
    assert "TryAcquireProcessThreadPermitsUpTo" in arena
    assert "g_process_physical_thread_permits.fetch_add" in arena
    assert "g_process_external_runtime_thread_permits.fetch_add" in arena
    assert "const auto total_reserved = current + external_reserved" not in arena
    assert "total_physical_thread_permits" in header
    assert "PyTuple_New(30)" in probe


def test_external_runtime_claims_are_not_os_thread_identity_evidence() -> None:
    """Verify external runtime claims are not OS thread identity evidence."""
    arena = (CPP / "internal/runtime/operation_task_arena.cc").read_text(encoding="utf-8")
    resources = (SRC / "core_impl/process_resources.py").read_text(encoding="utf-8")

    effective = arena[
        arena.index("EffectiveProcessThreadCapacity") : arena.index(
            "TryAcquireProcessThreadPermitsUpTo"
        )
    ]
    assert "g_process_external_runtime_resident_threads" in effective
    assert "g_process_external_runtime_thread_permits" not in effective
    assert "Active claims are reservations, not identity evidence" in effective
    assert "schema_sanitizer_resident_thread_count" in resources
    assert "cpu_count()`` and ``thread_pool_size()`` describe configured capacity" in resources


def test_runtime_reported_resident_width_survives_active_claim_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify runtime reported resident width survives active claim generation."""
    from schema_sanitizer.core_impl import process_resources as module

    with module._EXTERNAL_RUNTIME_POOL_COORDINATOR_LOCK:
        assert module._EXTERNAL_RUNTIME_TOTAL_PHYSICAL_CLAIMS == 0
        assert module._EXTERNAL_RUNTIME_TOTAL_LOGICAL_CLAIMS == 0
        assert all(
            not entry.physical_claims and not entry.logical_claims and not entry.config_inflight
            for entry in module._EXTERNAL_RUNTIME_POOL_COORDINATOR.values()
        )
    monkeypatch.setattr(
        module,
        "_EXTERNAL_RUNTIME_POOL_COORDINATOR",
        module._ExternalRuntimeCoordinator(),
    )

    events: list[tuple[str, int]] = []

    class Runtime:
        @staticmethod
        def schema_sanitizer_resident_thread_count() -> int:
            """Return the controlled resident-thread count."""
            return 3

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
            assert minimum <= desired
            events.append(("claim-acquire", desired))
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

        def external_runtime_residency_update(
            self, identity_delta: int, _stack_debt_delta: int
        ) -> None:
            """Record resident-thread additions or releases."""
            if identity_delta > 0:
                events.append(("resident-add", identity_delta))
            elif identity_delta < 0:
                events.append(("resident-release", -identity_delta))

    native = Native()
    monkeypatch.setattr(module, "_native_external_thread_api", lambda: native)

    acquisition = module._acquire_shared_external_native_thread_permits(Runtime, 4, minimum=2)
    assert acquisition.owner is not None
    assert acquisition.amount == 4
    assert ("resident-add", 3) in events
    assert module.external_runtime_pool_snapshot()["resident_width"] == 3
    assert module.external_runtime_pool_snapshot()["physical_permits"] == 4

    acquisition.owner.release()
    snapshot = module.external_runtime_pool_snapshot()
    assert snapshot["physical_permits"] == 0
    assert snapshot["resident_width"] == 3
    assert snapshot["coordinator_entries"] == 1

    with module._EXTERNAL_RUNTIME_POOL_COORDINATOR_LOCK:
        runtime_id = module._external_runtime_pool_identity_key(Runtime)
        entry = module._external_runtime_entry_locked(Runtime, create=False, runtime_key=runtime_id)
        assert entry is not None
        module._set_external_runtime_resident_width_locked(entry, native, 0)
        module._retire_external_runtime_entry_locked(runtime_id, entry)
    assert ("resident-release", 3) in events
    assert module.external_runtime_pool_snapshot()["coordinator_entries"] == 0


def test_stage_domain_rollback_keeps_failed_release_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify stage domain rollback keeps failed release retryable."""
    from schema_sanitizer.core_impl import memory_budget as module

    base = module.StageConcurrencyAdmission(slots=2, per_slot_bytes=64)
    monkeypatch.setattr(module, "acquire_parallel_admission", lambda *a, **k: base)

    class RetryLease:
        def __init__(self) -> None:
            """Initialize the retry lease test double."""
            self.releases = 0

        def release(self) -> None:
            """Release the resource held by the retry lease test double."""
            self.releases += 1
            if self.releases == 1:
                raise RuntimeError("transient release failure")

    lease = RetryLease()

    def fail_second(_slots: int) -> object:
        """Raise the deliberate failure during second."""
        raise RuntimeError("second domain acquisition failed")

    with pytest.raises(RuntimeError, match="second domain acquisition failed"):
        module.acquire_stage_concurrency_admission(
            2,
            per_slot_bytes=64,
            stage="native-thread-domains-commit-through-one_retry",
            domain_acquirers={
                "remote_io": lambda _slots: lease,
                "provider_permit": fail_second,
            },
        )

    assert module._STAGE_ADMISSION_CONSTRUCTION_ESCROW.published_count() == 1
    assert lease.releases == 1
    assert module.drain_abandoned_memory_finalizers() >= 1
    assert lease.releases == 2
    assert module._STAGE_ADMISSION_CONSTRUCTION_ESCROW.published_count() == 0


def test_release_native_protocol_validation_is_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify release native protocol validation is fail closed."""
    from schema_sanitizer.core_impl import concurrency_coverage as coverage
    from schema_sanitizer.core_impl import runtime_diagnostics

    monkeypatch.setattr(runtime_diagnostics, "_native_arena_snapshot", lambda: {"available": False})
    with pytest.raises(RuntimeError, match="snapshot is unavailable"):
        coverage.validate_native_concurrency_protocol_health()

    monkeypatch.setattr(
        runtime_diagnostics,
        "_native_arena_snapshot",
        lambda: {"available": True, "snapshot_failed": True},
    )
    with pytest.raises(RuntimeError, match="snapshot failed"):
        coverage.validate_native_concurrency_protocol_health()

    good = {
        "available": True,
        "snapshot_schema_fields": 30,
        "completion_memory_protocol_violations": 0,
        "counter_underflows": 0,
        "native_physical_threads": 3,
        "external_runtime_thread_permits": 2,
        "total_physical_thread_permits": 5,
        "native_physical_thread_capacity": 8,
        "thread_permit_snapshot_stable": 1,
        "external_runtime_resident_protocol_violations": 0,
        "external_runtime_resident_threads": 2,
        "external_runtime_stack_debt_threads": 2,
    }
    monkeypatch.setattr(runtime_diagnostics, "_native_arena_snapshot", lambda: dict(good))
    coverage.validate_native_concurrency_protocol_health()

    bad = dict(good, total_physical_thread_permits=6)
    monkeypatch.setattr(runtime_diagnostics, "_native_arena_snapshot", lambda: bad)
    with pytest.raises(RuntimeError, match="do not conserve"):
        coverage.validate_native_concurrency_protocol_health()


def test_route_profiles_require_transport_specific_runtime_evidence() -> None:
    """Verify route profiles require transport specific runtime evidence."""
    from schema_sanitizer.core_impl.concurrency_route_evidence import (
        INPUT_ROUTE_PROFILE_REQUIREMENTS,
        OUTPUT_ROUTE_PROFILE_REQUIREMENTS,
    )

    assert "process_file_descriptor_admission" in INPUT_ROUTE_PROFILE_REQUIREMENTS["local_path"]
    assert "stage_concurrency_admission" in INPUT_ROUTE_PROFILE_REQUIREMENTS["remote_chunks"]
    assert "process_file_descriptor_admission" in INPUT_ROUTE_PROFILE_REQUIREMENTS["staged_remote"]
    assert (
        "process_file_descriptor_admission"
        not in INPUT_ROUTE_PROFILE_REQUIREMENTS["materialized_memory"]
    )
    assert (
        "stage_concurrency_admission" in OUTPUT_ROUTE_PROFILE_REQUIREMENTS["remote_staged_commit"]
    )
    assert "external_runtime_pool_claim" in OUTPUT_ROUTE_PROFILE_REQUIREMENTS["analytical_adapter"]
    assert "external_runtime_pool_claim" not in OUTPUT_ROUTE_PROFILE_REQUIREMENTS["stream"]


def test_parquet_stream_owner_has_real_fork_identity_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify Parquet stream owner has real fork identity guard."""
    from schema_sanitizer.adapters.parquet import record_batch_factory as module

    owner = module._ParquetStreamKeepaliveOwner()
    original_pid = owner._pid
    deferred: list[tuple[int, object]] = []
    monkeypatch.setattr(module.os, "getpid", lambda: original_pid + 1)
    monkeypatch.setattr(
        module,
        "defer_prepared_finalizer_cleanup",
        lambda ticket, capsule: deferred.append((ticket, capsule)) or True,
    )
    owner.__del__()
    assert deferred == []
    monkeypatch.undo()
    owner.close()


def test_parquet_keepalive_compacts_dead_weakref_tombstones() -> None:
    """Verify Parquet keepalive compacts dead weakref tombstones."""
    from schema_sanitizer.adapters.parquet import record_batch_factory as module

    class WeakOwner:
        pass

    dead = [weakref.ref(WeakOwner()) for _ in range(module._PARQUET_KEEPALIVE_COMPACT_THRESHOLD)]
    live_owner = WeakOwner()
    keepalive = dead + [weakref.ref(live_owner)]
    factory = SimpleNamespace(_keepalive=keepalive)

    compacted = module._factory_stream_keepalive(factory)
    assert len(compacted) == 1
    assert compacted[0]() is live_owner
