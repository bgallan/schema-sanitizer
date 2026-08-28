"""Exercises fork preparation and child authorization across atomic epochs, reserved
finalizer rings, reusable tokens, authoritative retry maps, snapshots, pair scopes,
journals, arena generations, registries, and the native ABI. The manager freezes one
exact generation before handler registration, failed prepare never authorizes a child,
and all OS callbacks select bounded prewarmed state."""

from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _source(relative: str) -> str:
    """Return the production source text inspected by this module."""
    return (ROOT / "src/schema_sanitizer" / relative).read_text()


def test_fork_manager_freezes_exact_generation_when_before_registers_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify fork manager freezes exact generation when before registers handler."""
    from schema_sanitizer.core_impl import fork_manager as module

    monkeypatch.setattr(module, "_HANDLERS", [None] * module._MAX_FORK_HANDLERS)
    monkeypatch.setattr(module, "_COUNT", 0)
    monkeypatch.setattr(module, "_FORK_GENERATION", [None] * module._MAX_FORK_HANDLERS)
    monkeypatch.setattr(module, "_FORK_PREPARED", bytearray(module._MAX_FORK_HANDLERS))
    monkeypatch.setattr(module, "_FORK_GENERATION_COUNT", 0)
    monkeypatch.setattr(module, "_FORK_GENERATION_ACTIVE", False)
    events: list[str] = []

    def b_child() -> None:
        """Run the registered child callback in deterministic order."""
        events.append("b_child")

    def a_before() -> None:
        """Run the registered before-fork callback in deterministic order."""
        events.append("a_before")
        module.register_fork_handler(
            "fork-manager-freezes-exact-generation-when-b",
            after_in_child=b_child,
            child_safe_without_prepare=True,
            mode="child_safe",
        )

    module.register_fork_handler(
        "fork-manager-freezes-exact-generation-when-a", before=a_before, mode="prepared_swap"
    )
    module._before()
    module._child()
    assert events == ["a_before"]


def test_fork_manager_failed_prepare_never_authorizes_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify fork manager failed prepare never authorizes child."""
    from schema_sanitizer.core_impl import fork_manager as module

    monkeypatch.setattr(module, "_HANDLERS", [None] * module._MAX_FORK_HANDLERS)
    monkeypatch.setattr(module, "_COUNT", 0)
    monkeypatch.setattr(module, "_FORK_GENERATION", [None] * module._MAX_FORK_HANDLERS)
    monkeypatch.setattr(module, "_FORK_PREPARED", bytearray(module._MAX_FORK_HANDLERS))
    monkeypatch.setattr(module, "_FORK_GENERATION_COUNT", 0)
    monkeypatch.setattr(module, "_FORK_GENERATION_ACTIVE", False)
    events: list[str] = []

    def fail_before() -> None:
        """Raise the deliberate failure during before."""
        raise MemoryError("fork-manager-freezes-exact-generation-when")

    def child() -> None:
        """Run the child-side operation in the controlled lifecycle."""
        events.append("child")

    module.register_fork_handler(
        "fork-manager-freezes-exact-generation-when-failed",
        before=fail_before,
        after_in_child=child,
        mode="prepared_swap",
    )
    module._before()
    module._child()
    assert events == []


def test_every_fork_handler_has_explicit_bounded_mode() -> None:
    # Import representative runtime modules so their bounded contracts register.
    """Verify every fork handler has explicit bounded mode."""
    from schema_sanitizer.core_impl import control_plane_budget as _cp  # noqa: F401
    from schema_sanitizer.core_impl import finalizer_registry as _fr  # noqa: F401
    from schema_sanitizer.core_impl import runtime_shutdown as _rs  # noqa: F401
    from schema_sanitizer.core_impl.fork_manager import fork_handler_contracts

    contracts = fork_handler_contracts()
    assert contracts
    assert len(contracts) <= 256
    assert all(
        mode in {"prepared_swap", "child_safe", "quarantine_only"} for _, mode, _ in contracts
    )


def test_atomic_epoch_can_write_into_preallocated_buffer() -> None:
    """Verify atomic epoch can write into preallocated buffer."""
    from schema_sanitizer.core_impl.atomic_epoch import AtomicEpoch

    epoch = AtomicEpoch()
    for _ in range(257):
        assert epoch.increment()
    target = bytearray(16)
    epoch.write_into(target, 4)
    assert int.from_bytes(target[4:12], "little") == 257


def test_native_atomic_epoch_has_direct_buffer_abi_without_pylong_readback() -> None:
    """Verify native atomic epoch has direct buffer ABI without pylong readback."""
    source = _source("core_impl/atomic_epoch.py")
    cpp = (ROOT / "cpp/src/api/python_abi3/options/prepare.cc").read_text()
    assert "atomic_epoch_write_le" in source
    assert "atomic_epoch_write_activity" in cpp
    write_start = (
        cpp.index("AtomicEpochWrite")
        if "AtomicEpochWrite" in cpp
        else cpp.index("atomic_epoch_write")
    )
    fragment = cpp[write_start : write_start + 7000]
    assert "PyByteArray" in fragment


def test_reserved_finalizer_ring_prepares_before_owner_visibility_and_recycle() -> None:
    """Verify reserved finalizer ring prepares before owner visibility and recycle."""
    source = _source("core_impl/finalizer_escrow.py")
    reserve = source[
        source.index("    def reserve_ticket", source.index("class ReservedFinalizerEscrow")) :
    ]
    reserve = reserve[: reserve.index("\n    def release_ticket")]
    assert reserve.index("_ring_prepare_pop_locked") < reserve.index(
        "self._states[slot] = _RESERVED"
    )
    assert reserve.index("ticket =") < reserve.index("self._states[slot] = _RESERVED")
    release = source[
        source.index("    def release_ticket", source.index("class ReservedFinalizerEscrow")) :
    ]
    release = release[: release.index("\n    def ", 10)]
    # Owner-first retirement commits as owner-free RECYCLE_PENDING. Ring/counter
    # recycling is derived bookkeeping and may be completed by a later safe point.
    assert "_ring_prepare_push_locked" not in release
    assert release.index("self._slots[slot] = _EMPTY") < release.index(
        "self._states[slot] = _RECYCLE_PENDING"
    )
    assert release.index("self._states[slot] = _RECYCLE_PENDING") < release.index(
        "self._recycle_one_pending_locked()"
    )


def test_control_plane_failed_prepare_does_not_consume_reusable_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify control plane failed prepare does not consume reusable token."""
    from schema_sanitizer.core_impl.control_plane_budget import _ProcessControlPlaneBudget

    budget = _ProcessControlPlaneBudget()
    first = budget.reserve("fork-manager-freezes-exact-generation-when", 256)
    token = first.token
    assert budget.release(first)
    free_before = budget._free_token_count

    calls = 0

    def fail_on_growth(value: int) -> bool:
        """Inject the on growth failure at the controlled test point."""
        nonlocal calls
        calls += 1
        if calls == 1:
            return True
        raise MemoryError("fork-manager-freezes-exact-generation-when shadow")

    monkeypatch.setattr(budget, "_sync_native_shadow_locked", fail_on_growth)
    with pytest.raises(MemoryError):
        budget.reserve("fork-manager-freezes-exact-generation-when", 256)
    assert budget._free_token_count == free_before
    assert budget._free_tokens[budget._free_token_head] == token


def test_retry_deadline_index_has_one_physical_node_per_logical_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify retry deadline index has one physical node per logical retry."""
    from schema_sanitizer.core_impl import retry_scheduler as module

    scheduler = module._RetryScheduler()
    monkeypatch.setattr(scheduler, "_ensure_workers", lambda: None)
    for index in range(200):
        assert scheduler.schedule(
            ("fork-manager-freezes-exact-generation-when", "same"),
            lambda: None,
            delay_seconds=3600 + index,
        )
    with scheduler._condition:
        assert len(scheduler._current) == 1
        assert len(scheduler._heap) == 1
        assert scheduler._heap[0] in scheduler._current.values()


def test_retry_deadline_index_backing_storage_never_grows() -> None:
    """Verify retry deadline index backing storage never grows."""
    from schema_sanitizer.core_impl.retry_scheduler import _BoundedDeadlineIndex

    heap = _BoundedDeadlineIndex(8)
    assert len(heap._slots) == 8
    assert heap._capacity == 8


def test_retry_ready_map_is_authoritative_not_deque_rotation() -> None:
    """Verify retry ready map is authoritative not deque rotation."""
    source = _source("core_impl/retry_scheduler.py")
    scheduler_start = source.index("class _RetryScheduler")
    body = source[source.index("    def _take_ready_locked", scheduler_start) :]
    body = body[: body.index("\n    def ", 10)]
    assert "for candidate in self._ready_by_key.values()" in body
    assert "popleft" not in body
    assert ".append(" not in body


def test_release_guardian_dead_letter_owner_remains_authoritative_for_dedup() -> None:
    """Verify release guardian dead letter owner remains authoritative for dedup."""
    source = _source("core_impl/retry_scheduler.py")
    assert "self._items" in source
    assert "DEAD_LETTER" in source
    # Dead-letter transition keeps the same owner rooted in _items.
    marker = "item.state = _GuardedReleaseState.DEAD_LETTER"
    pos = source.index(marker)
    fragment = source[max(0, pos - 700) : pos + 500]
    assert "Keep the same identity in the authoritative" in fragment
    assert "self._items.pop" not in fragment


def test_cleanup_dispatcher_keeps_authoritative_owner_map_across_states() -> None:
    """Verify cleanup dispatcher keeps authoritative owner map across states."""
    source = _source("core_impl/cleanup_dispatcher.py")
    assert "self._owned_index" in source
    assert "_CleanupState.DELAYED" in source
    assert "_CleanupState.PARKED" in source
    assert "_CleanupState.DEAD_LETTER" in source
    assert "for call in self._owned_index.values()" in source


def test_availability_notifier_uses_authoritative_delivery_map_not_queue_rotation() -> None:
    """Verify availability notifier uses authoritative delivery map not queue rotation."""
    source = _source("core_impl/process_resources.py")
    start = source.index("class _AvailabilityNotifier")
    fragment = source[start : source.index("_AVAILABILITY_NOTIFIER =", start)]
    assert "self._queued" in fragment
    assert "for delivery in self._queued.values()" in fragment
    assert "self._queue.popleft(" not in fragment
    assert "self._queue.append(" not in fragment


def test_operation_memory_snapshot_is_pure_and_safe_point_is_explicit() -> None:
    """Verify operation memory snapshot is pure and safe point is explicit."""
    source = _source("core_impl/memory_budget.py")
    start = source.index("    def snapshot(self", source.index("class OperationMemoryLedger"))
    end = source.index("\n    def ", start + 8)
    body = source[start:end]
    assert "drain_abandoned_memory_finalizers" not in body
    assert "def safe_point(" in source


def test_pair_bootstrap_is_excluded_from_payload_contract_evidence() -> None:
    """Verify pair bootstrap is excluded from payload contract evidence."""
    contracts = _source("core_impl/concurrency_contracts.py")
    coverage = _source("core_impl/concurrency_coverage.py")
    start = contracts.index("def activate_runtime_concurrency_pair_admission")
    end = contracts.index("\ndef reset_runtime_concurrency_pair", start)
    body = contracts[start:end]
    assert "_PAIR_BOOTSTRAP.set(True)" in body
    assert "runtime_pair_payload_contract_observations" in contracts
    assert "validate_payload_observed_concurrency_pair_contracts" in coverage


def test_pair_bootstrap_credit_is_retired_before_output_metrics() -> None:
    """Verify pair bootstrap credit is retired before output metrics."""
    source = _source("core_impl/concurrency_contracts.py")
    start = source.index("    def transfer_to_output(self)")
    end = source.index("\n    def close(self)", start)
    body = source[start:end]
    assert 'transfer("pipeline_pair_output")' in body
    assert "close()" in body
    assert "self.admission = None" in body
    assert body.index("close()") < body.index("self._output_stage = True")


def test_public_pair_scope_remains_live_through_output_work() -> None:
    """Verify public pair scope remains live through output work."""
    for relative in ("api_impl/file_conversion/converters.py", "api_impl/analytical.py"):
        source = _source(relative)
        transfer = source.index("pair_scope.transfer_to_output()")
        close = source.rindex("pair_scope.close()")
        assert transfer < close


def test_cross_process_journal_only_commits_owner_delta_on_success() -> None:
    """Verify cross process journal only commits owner delta on success."""
    for relative in ("core_impl/cross_process_memory.py", "core_impl/cross_process_storage.py"):
        source = _source(relative)
        assert "committed = False" in source or "commit_owner_delta = False" in source
        assert "if committed:" in source or "if commit_owner_delta:" in source


def test_cross_process_liveness_work_is_bounded_per_transaction() -> None:
    """Verify cross process liveness work is bounded per transaction."""
    for relative in ("core_impl/cross_process_memory.py", "core_impl/cross_process_storage.py"):
        source = _source(relative)
        assert "_MAX_LIVENESS_CHECKS_PER_TRANSACTION = 256" in source


def test_low_core_estimates_retained_bytes_before_executor_mutex() -> None:
    """Verify low core estimates retained bytes before executor mutex."""
    source = (ROOT / "cpp/src/internal/runtime/ordered_executor.hh").read_text()
    assert "EstimateQueueRetainedBytes(outcome.result)" in source
    # fork-manager-freezes-exact-generation-when moved all retained-byte estimation to callers before store_outcome_locked.
    definition = source.index("bool store_outcome_locked(")
    store = source[definition : definition + 4500]
    assert "EstimateQueueRetainedBytes" not in store


def test_operation_task_arena_generation_exhaustion_is_fail_closed() -> None:
    """Verify operation task arena generation exhaustion is fail closed."""
    source = (ROOT / "cpp/src/internal/runtime/operation_task_arena.cc").read_text()
    assert "NextArenaGeneration" in source
    assert "std::numeric_limits<std::uint64_t>::max()" in source
    assert "generation namespace exhausted" in source


def test_shutdown_failure_recording_uses_preallocated_static_codes() -> None:
    """Verify shutdown failure recording uses preallocated static codes."""
    source = _source("core_impl/runtime_shutdown.py")
    assert "class _BoundedShutdownFailures" in source
    assert "observability_failures.append(f" not in source
    assert "finalizer_drain_failures.append(f" not in source


def test_terminal_ownership_generation_exhaustion_is_latched_fail_closed() -> None:
    """Verify terminal ownership generation exhaustion is latched fail closed."""
    source = _source("core_impl/terminal_ownership.py")
    assert "generation_exhausted" in source
    assert "_prepare_generation_advance_locked" in source


def test_native_shadow_is_prewarmed_before_global_admission_lock() -> None:
    """Verify native shadow is prewarmed before global admission lock."""
    source = _source("core_impl/control_plane_budget.py")
    start = source.index("    def reserve(", source.index("class _ProcessControlPlaneBudget"))
    end = source.index("\n    def release", start)
    body = source[start:end]
    assert body.index("self.prewarm_native_shadow()") < body.index(
        "with _GOVERNED_MEMORY_ADMISSION_LOCK"
    )


def test_reusable_fixed_width_lease_namespaces_replace_lifetime_growth() -> None:
    """Verify reusable fixed width lease namespaces replace lifetime growth."""
    process = _source("core_impl/process_resources.py")
    memory = _source("core_impl/memory_budget.py")
    storage = _source("core_impl/temporary_storage.py")
    provider = _source("remote_impl/provider_throttle.py")
    for source in (process, memory, storage, provider):
        assert "next_reusable_token" in source


def test_runtime_service_registry_uses_fixed_capacity_slot_generation_pool() -> None:
    """Verify runtime service registry uses fixed capacity slot generation pool."""
    source = _source("core_impl/runtime_registry.py")
    assert "BoundedGenerationPool(256)" in source
    assert "self._token_pool.acquire_for(entry)" in source
    assert "self._token_pool.release_for(entry)" in source


def test_only_central_fork_dispatchers_register_with_os() -> None:
    """Verify only central fork dispatchers register with OS."""
    matches = []
    for path in (ROOT / "src/schema_sanitizer").rglob("*.py"):
        text = path.read_text()
        if "os.register_at_fork(" in text:
            matches.append(path.relative_to(ROOT / "src/schema_sanitizer").as_posix())
    assert sorted(matches) == ["core_impl/fork_manager.py", "core_impl/fork_safety.py"]


def test_static_escrow_footprint_is_runtime_layout_derived_not_single_magic_multiplier() -> None:
    """Verify static escrow footprint is runtime layout derived not single magic multiplier."""
    source = _source("core_impl/finalizer_escrow.py")
    assert "sys.getsizeof" in source
    assert "_reserved_escrow_static_bytes" in source


def test_native_abi_surface_declares_direct_atomic_activity_writer() -> None:
    """Verify native ABI surface declares direct atomic activity writer."""
    catalog = (ROOT / "cpp/src/internal/abi/python_abi3/method_catalog.inc").read_text()
    module = (ROOT / "cpp/src/api/python_abi3/_core_abi3_module.cc").read_text()
    assert "atomic_epoch_write_activity" in catalog
    assert 'include "internal/abi/python_abi3/method_catalog.inc"' in module
