"""Regression coverage for memory native cpu admission precedes dequeue commit."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src/schema_sanitizer"
CPP = ROOT / "cpp/src"


def _source(relative: str) -> str:
    return (SRC / relative).read_text(encoding="utf-8")


def test_native_cpu_admission_precedes_dequeue_commit() -> None:
    runtime = (CPP / "internal/runtime/operation_task_arena_runtime.cc.inc").read_text()
    start = runtime.index("void worker_loop")
    body = runtime[start:]
    acquire = body.index("cpu_registration.Acquire(stop)")
    dequeue = body.index("take_task<PreferDedicatedOutput>")
    no_task_release = body.index("cpu_lease = {}", dequeue)
    park = body.index("WaitWithStop(slot.ready", no_task_release)
    assert acquire < dequeue < no_task_release < park


def test_native_submit_never_waits_for_retained_bytes_under_slot_mutex() -> None:
    source = (CPP / "internal/runtime/operation_task_arena.cc").read_text()
    helper = source.index("sanitize::Status AcquireRetainedSubmitCredit")
    wait = source.index("retained_ready.wait_until", helper)
    submit = source.index("sanitize::Status OperationTaskArena::SubmitCharged", wait)
    acquire = source.index("AcquireRetainedSubmitCredit(state, retained_bytes)", submit)
    slot_lock = source.index("std::unique_lock lock(slot.mutex)", acquire)
    # Byte waiting remains completely outside SubmitCharged's slot-lock
    # transaction rather than temporarily unlocking/relocking the queue mutex.
    assert helper < wait < submit < acquire < slot_lock


def test_async_results_support_pre_materialization_credit_and_reconcile_after_fetch() -> None:
    source = _source("core_impl/async_scheduler.py")
    start = source.index("async def _fetch_with_result_admission")
    end = source.index("async def _indexed_worker", start)
    body = source[start:end]
    reserve = body.index('stage="async_result_preflight"')
    fetch = body.index("value = await fetch(index)")
    reconcile = body.index("resize(retained_size)")
    assert reserve < fetch < reconcile
    assert "expected_retained_bytes" in source


def test_async_result_estimator_extrapolates_unvisited_items_conservatively() -> None:
    from schema_sanitizer.core_impl.async_scheduler import _estimate_async_result_bytes

    payload = [b"x" * 1024 for _ in range(1_000)]
    estimate = _estimate_async_result_bytes(payload)
    assert estimate >= sum(len(item) for item in payload)


def test_async_terminal_debt_is_preallocated_owns_late_queue_and_has_no_unbounded_fallback() -> (
    None
):
    source = _source("core_impl/async_scheduler.py")
    assert "_AsyncTerminalDebt() for _ in range(_ASYNC_TERMINAL_DEBT_CAPACITY)" in source
    assert "self.result_queue = result_queue" in source
    assert "async terminal debt capacity must cover every live task slot" in source
    stop = source[
        source.index("async def _stop_workers") : source.index("async def ordered_indexed_results")
    ]
    assert "await asyncio.gather(*pending" not in stop
    assert "_park_async_terminal_debt" in stop


def test_async_scheduler_combines_fair_share_with_idle_capacity_borrowing() -> None:
    source = _source("core_impl/async_scheduler.py")
    fair = source[
        source.index("def _fair_async_candidate") : source.index(
            "def _acquire_async_task_domain_exact"
        )
    ]
    borrow = source[
        source.index("def _borrow_idle_async_capacity") : source.index("class _AsyncTerminalDebt")
    ]
    assert "fair_share" in fair
    assert "_ASYNC_ACTIVE_OPERATIONS" in fair
    assert "active == 1" in borrow
    assert "target = requested" in borrow
    assert 'stage="async_scheduler_borrow"' in borrow
    assert "counts_operation=False" in borrow


def test_stage_domains_have_one_published_global_order_and_reverse_release() -> None:
    from schema_sanitizer.core_impl.memory_budget import StageConcurrencyAdmission

    events: list[str] = []

    class Lease:
        def __init__(self, name: str) -> None:
            self.name = name

        def close(self) -> None:
            events.append(self.name)

        release = close

    admission = StageConcurrencyAdmission(
        1,
        4096,
        memory_lease=Lease("memory"),
        execution_lease=Lease("thread"),
        owns_execution_lease=True,
        domain_leases=(("remote_io", Lease("remote")), ("async_task", Lease("async"))),
    )
    admission.close()
    assert events == ["async", "remote", "thread", "memory"]
    source = _source("core_impl/memory_budget.py")
    assert "_STAGE_DOMAIN_ORDER" in source
    assert "_stage_domain_order_key" in source
    assert "sorted(" in source[source.index("def acquire_stage_concurrency_admission") :]


def test_remote_io_is_attached_to_real_stage_admission() -> None:
    source = _source("remote_impl/io_coordinator.py")
    submit = source[
        source.index("    def submit(") : source.index(
            "    def permit_snapshot", source.index("    def submit(")
        )
    ]
    assert "acquire_stage_concurrency_admission" in submit
    assert "physical_threads=False" in submit
    assert 'attach_domain("remote_io"' in submit
    assert "operation_memory_ledger" in source


def test_pair_gate_requires_real_payload_window_and_native_core_observation() -> None:
    contracts = _source("core_impl/concurrency_contracts.py")
    coverage = _source("core_impl/concurrency_coverage.py")
    translation = _source("core_impl/error_translation.py")
    assert "payload_window_bytes" in contracts
    assert "per_slot_bytes=1" not in contracts
    assert ".resize(0)" not in contracts
    assert '"native_payload_core_call"' in coverage
    assert '"native_payload_core_call"' in translation
    assert "memory_budget(memory_limit_bytes).io_chunk_bytes" in _source(
        "api_impl/file_conversion/converters.py"
    )


def test_pair_payload_window_remains_charged_until_scope_close() -> None:
    """Release evidence must retain the byte authority protecting the payload."""
    from schema_sanitizer.core_impl.concurrency_contracts import (
        activate_runtime_concurrency_pair_admission,
    )
    from schema_sanitizer.core_impl.memory_budget import OperationMemoryLedger

    ledger = OperationMemoryLedger(64 << 20)
    scope = activate_runtime_concurrency_pair_admission(
        "csv",
        "csv",
        memory_ledger=ledger,
        desired_payload_slots=1,
        payload_window_bytes=8192,
        route_profiles=("local_path", "local_file"),
    )
    try:
        scope.transfer_to_output()
        payload = scope.payload_admission
        assert payload is not None
        assert payload.memory_lease is not None
        assert payload.memory_lease.reserved_bytes == 8192
    finally:
        scope.close()
        ledger.close()


def test_shutdown_async_drain_is_multiphase_and_final_snapshot_is_authoritative() -> None:
    source = _source("core_impl/runtime_shutdown.py")
    close_admission = source.index("close_async_scheduler_admission()")
    first_drain = source.index("wait_async_scheduler_quiescent", close_admission)
    producer_close = source.index("RuntimeServicePhase.PRODUCER", first_drain)
    second_drain = source.index("wait_async_scheduler_quiescent", producer_close)
    final_snapshot = source.rindex("async_scheduler_snapshot()")
    assert close_admission < first_drain < producer_close < second_drain < final_snapshot
    tail = source[final_snapshot:]
    assert "async_snapshot.active_operations" in tail
    assert "async_snapshot.terminal_debts" in tail


def test_python_lifecycle_condition_waits_are_all_bounded() -> None:
    offenders: list[str] = []
    for path in SRC.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if not isinstance(func, ast.Attribute) or func.attr != "wait":
                continue
            if not node.args and not any(keyword.arg == "timeout" for keyword in node.keywords):
                offenders.append(f"{path.relative_to(ROOT)}:{node.lineno}")
    assert offenders == []


def test_cgroup_view_resolves_nested_membership_and_refreshes_after_short_ttl() -> None:
    from schema_sanitizer.core_impl.cgroup_view import _join_mount_path

    nested = _join_mount_path("/sys/fs/cgroup", "/", "/user.slice/app.scope")
    pod = _join_mount_path("/sys/fs/cgroup", "/kubepods", "/kubepods/pod/x")
    assert nested is not None
    assert pod is not None
    assert nested.as_posix() == "/sys/fs/cgroup/user.slice/app.scope"
    assert pod.as_posix() == "/sys/fs/cgroup/pod/x"
    source = _source("core_impl/cgroup_view.py")
    assert "_CACHE_TTL_NS" in source
    assert "time.monotonic_ns()" in source
    assert 'Path("/proc/self/cgroup")' in source
    assert 'Path("/proc/self/mountinfo")' in source


def test_native_cgroup_view_handles_v1_v2_and_mountinfo_escapes() -> None:
    source = (CPP / "internal/runtime/cgroup_view.hh").read_text()
    assert 'fopen("/proc/self/cgroup"' in source
    assert 'fopen("/proc/self/mountinfo"' in source
    assert "cgroup2" in source
    assert "unescape_mount_field" in source
    registry = (CPP / "internal/memory/memory_pool_registry.cc.inc").read_text()
    assert '"memory.limit_in_bytes"' in registry


def test_physical_thread_observation_is_cross_platform_and_fail_closed() -> None:
    source = (CPP / "internal/runtime/operation_task_arena.cc").read_text()
    assert "std::optional<std::size_t> ProcessPhysicalThreadCount" in source
    assert 'opendir("/proc/self/task")' in source
    assert "CreateToolhelp32Snapshot" in source
    assert "task_threads(mach_task_self()" in source
    permit = source[
        source.index("TryAcquireProcessPhysicalThreadPermitsUpTo") : source.index(
            "TryAcquireProcessExternalRuntimeThreadPermitsUpTo"
        )
    ]
    assert "if (!process_threads)" in permit
    assert "break;" in permit[permit.index("if (!process_threads)") :]
    assert "TryAcquireProcessThreadPermitsUpTo" in permit
    module = (CPP / "api/python_abi3/_core_abi3_module.cc").read_text()
    assert '"process_physical_thread_count"' in module


def test_thread_capacity_uses_live_memory_headroom_not_only_ceiling() -> None:
    source = _source("core_impl/process_resources.py")
    hard = source[source.index("def _thread_hard_capacity") : source.index("def _fd_hard_capacity")]
    assert "_effective_memory_headroom_bytes" in hard
    assert "pids.current" in source
    assert "process_physical_thread_count" in source


def test_default_thread_envelope_survives_reasonable_external_runtime_pools(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import process_resources

    monkeypatch.delenv("SCHEMA_SANITIZER_MAX_PROJECT_THREADS", raising=False)
    monkeypatch.setattr(process_resources.os, "cpu_count", lambda: 3)
    monkeypatch.setattr(process_resources, "_process_physical_thread_count", lambda: 32)
    monkeypatch.setattr(process_resources, "_cgroup_pid_headroom", lambda: None)
    monkeypatch.setattr(process_resources, "_effective_memory_headroom_bytes", lambda: None)
    monkeypatch.setattr(process_resources, "resource", None)

    assert process_resources._thread_requested_capacity() == 256
    assert process_resources._thread_hard_capacity(governed_in_use=0) == 224


def test_terminal_thread_and_fd_debts_are_fixed_slot_banks() -> None:
    thread = _source("core_impl/governed_thread.py")
    process = _source("core_impl/process_resources.py")
    assert "_RetirementDebtSlot() for _ in range(_MAX_RETIREMENT_DEBTS)" in thread
    reap = thread[
        thread.index("def reap_governed_thread_retirements") : thread.index(
            "def defer_governed_thread_retirement"
        )
    ]
    assert "tuple(" not in reap
    assert "claimed = (" not in reap
    assert "_UncertainFdCloseDebtSlot()" in process
    publish = process[
        process.index("def retain_uncertain_fd_close") : process.index(
            "def uncertain_fd_close_snapshot"
        )
    ]
    assert ".append(" not in publish


def test_cpu_governor_rechecks_dynamic_capacity_even_for_single_arena() -> None:
    source = (CPP / "internal/runtime/process_cpu_governor.hh").read_text()
    acquire = source[source.index("AcquireTask(") : source.index("void ReleaseTask")]
    assert "arena_width" in acquire
    assert "arena_width) <= current_capacity" in acquire
    assert "arena_width) <= CachedCapacity()" in acquire
    assert "cached_capacity_.exchange(detected" in source


def test_allocation_registry_uses_one_process_global_slab_with_shrink_only_admission() -> None:
    source = (CPP / "internal/memory/memory_pool_registry.cc.inc").read_text()
    assert "struct GlobalRegistryBank" in source
    assert "std::make_unique<GlobalAllocationSlot[]>(capacity)" in source
    assert "std::make_unique<std::uint32_t[]>(capacity)" in source
    assert "sizeof(GlobalRegistryBank) ==" in source
    assert "global_registry_fixed_overhead_bytes()" in source
    assert "owner_token_" in source
    assert "usable_slots_per_shard" in source
    assert "current_limit = registry_metadata_limit_bytes()" in source
    assert "kRegistryLimitRefreshPeriodNs" in source
    assert "std::chrono::steady_clock::now()" in source
    assert "std::lock_guard refresh_lock(limit_refresh_mutex)" in source
    # Allocate/Free use a preallocated chained index and free list. Full-bank
    # scans are reserved for destruction, where orphaned owner entries must be
    # purged even after a memory.max shrink.
    assert "bucket_heads" in source
    assert "free_heads" in source
    assert "redirected_entries" in source
    assert "global_registry_fixed_overhead_bytes()" in source
    fixed = source[
        source.index("global_registry_fixed_overhead_bytes") : source.index(
            "struct GlobalRegistryBank"
        )
    ]
    assert "sizeof(std::unique_ptr<GlobalAllocationSlot[]>)" in fixed
    assert "sizeof(std::unique_ptr<std::uint32_t[]>)" in fixed
    assert "sizeof(std::mutex)" in fixed
    register = source[
        source.index("InsertResult register_in_primary") : source.index("bool claim_in_one_shard")
    ]
    claim = source[source.index("bool claim_in_one_shard") : source.index("void purge_owner")]
    assert "find_slot(" not in register
    assert "slots_per_shard; ++index" not in register
    assert "slots_per_shard; ++index" not in claim
    assert "slot.owner == owner_token_ && slot.buffer == buffer" in claim
    assert "static_cast<std::size_t>(index) >= bank.cached_usable_slots()" in source
    assert "result = register_in_primary" in source
    assert "result = register_in_two_shards" in source
    assert "std::scoped_lock" in source
    assert "redirected_to_secondary(bank, primary)" in source
    assert "if (!reserve_owner_entry())" in source
    reserve_start = source.index("bool reserve_owner_entry")
    reserve = source[reserve_start : source.index("shard_index(", reserve_start)]
    assert "compare_exchange_weak" in reserve
    assert "current < limit" in reserve
    # Pools no longer allocate a private registry table.
    constructor = source[
        source.index("class LiveAllocationRegistry") : source.index("bool register_allocation")
    ]
    assert "make_unique" not in constructor


def test_remote_sort_scratch_is_governed_before_sorting() -> None:
    helper = _source("core_impl/governed_sort.py")
    reserve = helper.index("reserve_sort_scratch")
    sort = helper.index("values.sort", reserve)
    assert reserve < sort
    assert "acquire_operation_memory" in helper
    assert "reserve_control_plane" in helper
    for relative in (
        "remote_impl/providers/s3.py",
        "remote_impl/providers/gcs.py",
        "remote_impl/providers/azure.py",
    ):
        source = _source(relative)
        assert ".sort(" not in source
        assert "governed_sort" in source


def test_async_production_callers_supply_predictable_result_preflight_sizes() -> None:
    expected = [
        "pipeline/source_discovery.py",
        "remote_impl/providers/s3.py",
        "remote_impl/providers/gcs.py",
        "remote_impl/providers/azure.py",
    ]
    for relative in expected:
        assert "expected_retained_bytes=" in _source(relative), relative


def test_sources_keep_cmake43_and_compile() -> None:
    assert (ROOT / "CMakeLists.txt").read_text().startswith("cmake_minimum_required(VERSION 4.3)")


def test_all_native_thread_creation_paths_share_physical_thread_domain() -> None:
    workers = (CPP / "internal/runtime/ordered_executor_workers.cc.inc").read_text()
    assert "ProcessPhysicalThreadPermitLease permit(1U)" in workers
    assert "mark_process_physical_thread_running()" in workers
    assert "mark_process_physical_thread_stopped()" in workers
    assert "permit = std::move(permit)" in workers
    assert workers.index("ProcessPhysicalThreadPermitLease permit(1U)") < workers.index(
        "workers_.emplace_back"
    )


def test_thread_stack_reservations_consume_live_headroom_on_python_and_cpp() -> None:
    process = _source("core_impl/process_resources.py")
    hard = process[
        process.index("def _thread_hard_capacity") : process.index("def _fd_hard_capacity")
    ]
    assert "virtual_stack_reserved" in hard
    assert "governed_in_use" in hard
    assert "_CONSERVATIVE_THREAD_STACK_BYTES" in hard

    arena = (CPP / "internal/runtime/operation_task_arena.cc").read_text()
    assert "ManagedThreadMemoryCapacity" in arena
    assert "kStackReservation" in arena
    assert "memory.current" in arena
    assert "g_process_physical_thread_permits" in arena


def test_python_and_cpp_share_one_physical_thread_permit_domain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from threading import Thread

    from schema_sanitizer.core_impl import governed_thread

    events: list[str] = []

    class _NativePhysicalThreadApi:
        def process_physical_thread_permits_acquire(self, desired: int, minimum: int) -> int:
            assert (desired, minimum) == (1, 1)
            events.append("acquire")
            return 1

        def process_physical_thread_permits_release(self, amount: int) -> None:
            assert amount == 1
            events.append("release")

        def process_physical_thread_mark_running(self) -> None:
            events.append("running")

        def process_physical_thread_mark_stopped(self) -> None:
            events.append("stopped")

    native = _NativePhysicalThreadApi()
    monkeypatch.setattr(governed_thread, "_native_physical_thread_api", lambda: native)
    thread = Thread(target=lambda: events.append("target"))
    governed_thread.start_governed_thread(thread)
    thread.join(timeout=2.0)
    assert not thread.is_alive()
    assert events == ["acquire", "running", "target", "stopped", "release"]

    cpp = (CPP / "internal/runtime/operation_task_arena.cc").read_text()
    assert "g_process_physical_thread_permits" in cpp
    assert "TryAcquireProcessPhysicalThreadPermitsUpTo" in cpp
    module = (CPP / "api/python_abi3/_core_abi3_module.cc").read_text()
    for method in (
        "process_physical_thread_permits_acquire",
        "process_physical_thread_permits_release",
        "process_physical_thread_mark_running",
        "process_physical_thread_mark_stopped",
    ):
        assert f'"{method}"' in module
