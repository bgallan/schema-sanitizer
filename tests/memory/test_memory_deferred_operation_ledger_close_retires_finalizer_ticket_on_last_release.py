"""Regression coverage for memory deferred operation ledger close retires finalizer ticket on last release."""

from __future__ import annotations

import ast
import asyncio
from pathlib import Path

import pytest
from _support.synchronization import SCHEDULER_TIMEOUT_SECONDS

ROOT = Path(__file__).resolve().parents[2]


def _source(relative: str) -> str:
    return (ROOT / "src/schema_sanitizer" / relative).read_text(encoding="utf-8")


def test_deferred_operation_ledger_close_retires_finalizer_ticket_on_last_release() -> None:
    from schema_sanitizer.core_impl import memory_budget as module

    baseline = module._MEMORY_LEDGER_FINALIZER_ESCROW.reserved_count()
    ledger = module.OperationMemoryLedger(8 << 20)
    assert module._MEMORY_LEDGER_FINALIZER_ESCROW.reserved_count() == baseline + 1
    lease = ledger.acquire(
        4096, stage="deferred-operation-ledger-close-retires-finalizer-deferred-close"
    )
    ledger.close()
    assert ledger.diagnostics().close_outstanding_bytes == 4096
    assert ledger._finalizer_ticket is not None

    lease.close()
    assert ledger.snapshot().reserved_bytes == 0
    assert ledger._finalizer_ticket is None
    assert module._MEMORY_LEDGER_FINALIZER_ESCROW.reserved_count() == baseline
    ledger.close()  # idempotent after the deferred release completes


def test_directory_metadata_retention_lease_survives_budget_scope_until_owner_release() -> None:
    from schema_sanitizer.core_impl.memory_budget import OperationMemoryLedger
    from schema_sanitizer.input_impl.directory_metadata_budget import DirectoryMetadataBudget

    ledger = OperationMemoryLedger(8 << 20)
    budget = DirectoryMetadataBudget(8 << 20, operation_memory_ledger=ledger)
    budget.charge_uris(("s3://bucket/" + "x" * 2048 + ".json",))
    retained_before = budget.used_bytes
    assert retained_before > 0

    owner = budget.retain()
    assert budget.used_bytes == 0
    assert owner.reserved_bytes == retained_before
    ledger.close()  # close is intentionally deferred while escaped metadata is live
    # The native receipt may conservatively retain short-lived resize slack,
    # but it must cover at least the escaped metadata owner until release.
    assert ledger.diagnostics().close_outstanding_bytes >= owner.reserved_bytes

    owner.close()
    assert ledger.snapshot().reserved_bytes == 0
    assert ledger._finalizer_ticket is None


def test_directory_metadata_reserves_before_retaining_python_containers() -> None:
    source = _source("input_impl/directory_metadata_budget.py")
    uris = source[source.index("    def charge_uris") : source.index("    def charge_references")]
    refs = source[source.index("    def charge_references") : source.index("    def charge_file")]
    assert uris.index("self._charge(item_charge") < uris.index("values.append(uri)")
    assert refs.index("self._charge(charge_per_item") < refs.index("retained.append(value)")
    assert "temporary = len(values) * _DIRECTORY_METADATA_REFERENCE_BYTES" in uris
    assert "temporary = len(retained) * _DIRECTORY_METADATA_REFERENCE_BYTES" in refs


def test_fork_quarantine_is_marker_only_and_production_has_no_child_safe_allocators() -> None:
    from schema_sanitizer.core_impl import fork_manager

    fork_manager.register_fork_handler(
        "deferred-operation-ledger-close-retires-finalizer-marker-only", mode="quarantine_only"
    )
    with pytest.raises(ValueError, match="marker-only"):
        fork_manager.register_fork_handler(
            "deferred-operation-ledger-close-retires-finalizer-invalid-quarantine",
            before=lambda: None,
            mode="quarantine_only",
        )

    offenders: list[str] = []
    for path in (ROOT / "src/schema_sanitizer").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            fn = node.func
            name = fn.id if isinstance(fn, ast.Name) else getattr(fn, "attr", "")
            if name not in {"register_fork_handler", "_register_fork_handler"}:
                continue
            keywords = {keyword.arg: keyword.value for keyword in node.keywords}
            opted_in = keywords.get("child_safe_without_prepare")
            if isinstance(opted_in, ast.Constant) and opted_in.value is True:
                offenders.append(f"{path.relative_to(ROOT)}:{node.lineno}")
            mode = keywords.get("mode")
            if isinstance(mode, ast.Constant) and mode.value == "quarantine_only":
                if any(key in keywords for key in ("before", "after_in_parent", "after_in_child")):
                    offenders.append(f"{path.relative_to(ROOT)}:{node.lineno}:quarantine-callback")
    assert offenders == []


def test_async_scheduler_lifecycle_closes_new_admission_and_reopens_only_quiescent() -> None:
    from schema_sanitizer.core_impl import async_scheduler as module

    module.reopen_async_scheduler_for_tests()
    module.close_async_scheduler_admission()
    assert module.async_scheduler_snapshot().admission_closed is True
    with pytest.raises(Exception, match="admission is closed"):
        module._acquire_async_scheduler_admission(1)
    assert module.wait_async_scheduler_quiescent(0.0) is True
    module.reopen_async_scheduler_for_tests()
    assert module.async_scheduler_snapshot().admission_closed is False


def test_async_scheduler_fair_share_prevents_one_operation_monopolising_normal_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import async_scheduler as module

    monkeypatch.setattr(module, "_MAX_PROCESS_ASYNC_TASK_SLOTS", 8)
    module.reopen_async_scheduler_for_tests()
    first = module._acquire_async_scheduler_admission(8)
    second = module._acquire_async_scheduler_admission(8)
    try:
        assert first.slots == 4
        assert second.slots == 4
        assert module.async_scheduler_snapshot().in_use == 8
    finally:
        second.close()
        first.close()


def test_async_scheduler_cancellation_resistance_transfers_admission_to_terminal_debt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import async_scheduler as module

    async def run() -> tuple[int, int]:
        module.reopen_async_scheduler_for_tests()
        monkeypatch.setattr(module, "_ASYNC_CANCEL_TIMEOUT_SECONDS", 0.001)
        admission = module._acquire_async_scheduler_admission(1)
        started = asyncio.Event()
        cancellation_observed = asyncio.Event()
        allow_terminal = asyncio.Event()

        async def resistant() -> None:
            started.set()
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                cancellation_observed.set()
                await allow_terminal.wait()

        task = asyncio.create_task(resistant())
        await asyncio.wait_for(started.wait(), timeout=SCHEDULER_TIMEOUT_SECONDS)
        parked = await module._stop_workers([task], admission)
        await asyncio.wait_for(cancellation_observed.wait(), timeout=SCHEDULER_TIMEOUT_SECONDS)
        during = module.async_scheduler_snapshot().terminal_debts
        assert parked is True
        allow_terminal.set()
        await asyncio.wait_for(task, timeout=SCHEDULER_TIMEOUT_SECONDS)
        after = module.async_scheduler_snapshot().terminal_debts
        assert module.wait_async_scheduler_quiescent(0.1) is True
        return during, after

    during, after = asyncio.run(run())
    assert during == 1
    assert after == 0


def test_async_result_queue_charges_retained_payload_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import async_scheduler as scheduler
    from schema_sanitizer.core_impl import memory_budget

    charged: list[int] = []

    class Lease:
        def close(self) -> None:
            return None

    monkeypatch.setattr(
        memory_budget,
        "acquire_operation_memory",
        lambda size, *, stage: charged.append(size) or Lease(),
    )

    async def run() -> list[bytes]:
        async def fetch(index: int) -> bytes:
            return b"x" * (1024 + index)

        return [
            value async for _index, value in scheduler.ordered_indexed_results(2, fetch, window=1)
        ]

    values = asyncio.run(run())
    assert [len(value) for value in values] == [1024, 1025]
    assert charged == [1088, 1089]  # payload plus conservative bytes-object overhead


def test_async_result_queue_accepts_exact_retained_bytes_estimator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import async_scheduler as scheduler
    from schema_sanitizer.core_impl import memory_budget

    charged: list[int] = []

    class OpaqueResult:
        pass

    class Lease:
        def close(self) -> None:
            return None

    monkeypatch.setattr(
        memory_budget,
        "acquire_operation_memory",
        lambda size, *, stage: charged.append(size) or Lease(),
    )

    async def run() -> list[OpaqueResult]:
        async def fetch(_index: int) -> OpaqueResult:
            return OpaqueResult()

        return [
            value
            async for _index, value in scheduler.ordered_indexed_results(
                2, fetch, window=1, retained_bytes=lambda _value: 2 << 20
            )
        ]

    values = asyncio.run(run())
    assert len(values) == 2
    assert charged == [2 << 20, 2 << 20]


def test_thread_and_fd_hard_caps_can_reach_zero_under_external_pressure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import process_resources as module

    monkeypatch.setattr(module, "_thread_requested_capacity", lambda: 128)
    monkeypatch.setattr(module, "_cgroup_pid_headroom", lambda: 0)
    monkeypatch.setattr(module, "_effective_memory_ceiling_bytes", lambda: None)
    monkeypatch.setattr(module, "resource", None)
    assert module._thread_hard_capacity(governed_in_use=0) == 0

    class Resource:
        RLIMIT_NOFILE = 7

        @staticmethod
        def getrlimit(_kind: int) -> tuple[int, int]:
            return 64, 64

    monkeypatch.setattr(module, "resource", Resource)
    monkeypatch.setattr(module, "_fd_requested_capacity", lambda: 128)
    monkeypatch.setattr(module, "_open_fd_count", lambda: 60)
    assert module._fd_hard_capacity(governed_in_use=0) == 0


def test_native_physical_thread_envelope_counts_external_and_pending_threads() -> None:
    source = (ROOT / "cpp/src/internal/runtime/operation_task_arena.cc").read_text()
    assert '"/proc/self/task"' in source
    assert "g_managed_running_threads" in source
    assert "external_threads" in source
    assert "process_managed_capacity" in source
    assert "g_process_physical_thread_permits.compare_exchange_weak" in source
    assert "if (*configured == '-')" in source
    assert "StartGovernedNativeThread" in source


def test_process_cpu_governor_refreshes_capacity_outside_admission_mutex() -> None:
    source = (ROOT / "cpp/src/internal/runtime/process_cpu_governor.hh").read_text()
    acquire = source[source.index("AcquireTask(") : source.index("void ReleaseTask")]
    lock = acquire.index("std::unique_lock lock(mutex_)")
    assert acquire.index("const auto current_capacity = RefreshCapacity();") < lock
    assert "available_cpu_capacity()" not in acquire[lock:]
    assert "active_tasks_ >= CachedCapacity()" in acquire
    assert "active_tasks_ < CachedCapacity()" in acquire
    assert "std::atomic<std::int64_t> cached_capacity_{1};" in source

    capacity = (ROOT / "cpp/src/internal/runtime/cpu_capacity.hh").read_text()
    assert "kCgroupCapacityRefreshPeriodNs = 250'000'000LL" in capacity
    assert "inline constinit CgroupCapacityCache" in capacity
    assert "inline constinit std::atomic<std::int64_t> g_hardware_count{0};" in capacity
    assert "std::atomic<std::uint64_t> owner_pid" in capacity
    assert "compare_exchange_strong" in capacity
    assert "std::mutex" not in capacity
    assert "cached_cgroup_capacity()" in capacity
    platform_start = capacity.index("std::int64_t platform_count()")
    platform_count = capacity[
        platform_start : capacity.index("#elif defined(_WIN32)", platform_start)
    ]
    assert "affinity_count()" in platform_count
    assert "cached_cgroup_capacity()" in platform_count
    assert "return 1;" in capacity[capacity.index("sample_cgroup_capacity") :]
    assert "ready_.wait_until(lock, refresh_at)" in source
    timeout_refresh = acquire[
        acquire.index("if (ready_.wait_until(lock, refresh_at)") : acquire.index(
            "refresh_at =", acquire.index("if (ready_.wait_until(lock, refresh_at)")
        )
    ]
    assert (
        timeout_refresh.index("lock.unlock();")
        < timeout_refresh.index("(void)RefreshCapacity();")
        < timeout_refresh.index("lock.lock();")
    )


def test_native_registry_has_dynamic_global_budget_and_two_choice_sharding() -> None:
    source = (ROOT / "cpp/src/internal/memory/memory_pool_registry.cc.inc").read_text()
    header = (ROOT / "cpp/src/internal/memory/memory_pool.hh").read_text()
    assert "automatic_memory_limit_bytes()" in source
    assert "process_limit / 16" in source
    assert "secondary_shard_index" in source
    assert "std::scoped_lock" in source
    assert "live_allocation_registry_secondary_probes" in source
    assert "live_allocation_registry_collision_rejections" in source
    assert "live_allocation_registry_max_shard_occupancy" in source
    assert "secondary_probes" in header
    assert "collision_rejections" in header
    assert "max_shard_occupancy" in header


def test_single_file_source_discovery_is_inside_shared_metadata_envelope() -> None:
    async_source = _source("pipeline/source_discovery.py")
    sync_source = _source("pipeline/source_discovery_sync.py")
    memory_source = _source("pipeline/source_discovery_memory.py")
    assert "budget.charge_uris(plan.source_uri for plan in plans)" in memory_source
    assert "budget.charge_associations(len(locations) * 12 + len(plans) * 2)" in memory_source
    assert "budget.charge_references(values, references_per_item=2)" in memory_source
    assert "precharge_source_locations(" in async_source
    assert "precharge_source_locations(" in sync_source


def test_stage_concurrency_admission_is_distinct_and_rolls_back_extra_domains(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import memory_budget as module

    assert issubclass(module.StageConcurrencyAdmission, module.CompositeParallelAdmission)
    assert module.StageConcurrencyAdmission is not module.CompositeParallelAdmission
    released: list[str] = []

    class Domain:
        def __init__(self, name: str) -> None:
            self.name = name

        def release(self) -> None:
            released.append(self.name)

    base = module.CompositeParallelAdmission(2, 64)
    monkeypatch.setattr(module, "acquire_parallel_admission", lambda *args, **kwargs: base)

    def fail(_slots: int) -> object:
        raise RuntimeError("domain rejected")

    with pytest.raises(RuntimeError, match="domain rejected"):
        module.acquire_stage_concurrency_admission(
            2,
            per_slot_bytes=64,
            stage="deferred-operation-ledger-close-retires-finalizer-stage",
            domain_acquirers={"first": lambda _slots: Domain("first"), "second": fail},
        )
    assert released == ["first"]


def test_release_concurrency_gate_requires_payload_evidence_not_structural_only() -> None:
    source = _source("core_impl/concurrency_coverage.py")
    gate = source[source.index("def validate_release_concurrency_pair_contracts") :]
    assert "validate_format_pair_release_contracts()" in gate

    format_gate = source[
        source.index("def validate_format_pair_release_contracts") : source.index(
            "def validate_release_concurrency_pair_contracts"
        )
    ]
    assert "validate_concurrency_pair_contracts()" in format_gate
    assert "validate_payload_observed_concurrency_pair_contracts()" in format_gate
    assert "payload_count != structural_count" in format_gate


def test_partition_result_history_supports_full_compact_and_streaming_modes() -> None:
    source = _source("pipeline/partition_execution.py")
    assert '{"full", "metadata_only", "streaming"}' in source
    assert 'if retention_mode == "full"' in source
    assert 'elif retention_mode == "metadata_only"' in source
    compact = source[
        source.index("def _metadata_only_run_result") : source.index(
            "def _compile_native_registry_state"
        )
    ]
    assert "source_manifest=None" in compact
    assert "discovered_input=" not in compact
    assert "native_registry_state=None" in compact


def test_directory_metadata_close_has_a_bounded_deadline() -> None:
    source = _source("input_impl/directory_metadata_budget.py")
    close = source[
        source.index(
            "    def close(self) -> None:", source.index("class DirectoryMetadataBudget")
        ) :
    ]
    assert "_DIRECTORY_METADATA_CLOSE_TIMEOUT_SECONDS" in close
    assert "wait(timeout=remaining)" in close
    assert "close exceeded its deadline" in close


def test_source_summary_is_streaming_and_partition_summary_is_identity_cached() -> None:
    source = _source("pipeline/source_discovery.py")
    memory_source = _source("pipeline/source_discovery_memory.py")
    summary = memory_source[
        memory_source.index("def source_summary") : memory_source.index("def cached_source_summary")
    ]
    assert "sizes =" not in summary
    assert "count += 1" in summary
    cached = memory_source[
        memory_source.index("def cached_source_summary") : memory_source.index(
            "def precharge_source_locations"
        )
    ]
    assert "id(discovered)" in cached
    partition = source[
        source.index("def _partition_plans") : source.index("async def _discover_existing")
    ]
    assert "summary_by_identity" in partition
    assert "cached_source_summary(" in partition
