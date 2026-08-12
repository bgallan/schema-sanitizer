"""Regression coverage for memory async corruption closes admission but exact cleanup capability survives."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src" / "schema_sanitizer"
CPP = ROOT / "cpp" / "src"


def test_async_corruption_closes_admission_but_exact_cleanup_capability_survives(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import async_scheduler as scheduler
    from schema_sanitizer.errors import SchemaSanitizerResourceError

    monkeypatch.setattr(scheduler, "_ASYNC_TASK_SLOTS_IN_USE", 1)
    monkeypatch.setattr(scheduler, "_ASYNC_ACTIVE_OPERATIONS", 1)
    monkeypatch.setattr(scheduler, "_ASYNC_PROTOCOL_VIOLATIONS", 0)
    monkeypatch.setattr(scheduler, "_ASYNC_ADMISSION_CLOSED", False)
    monkeypatch.setattr(scheduler, "_ASYNC_CORRUPTED", False)

    lease = scheduler._AsyncTaskDomainLease(2)
    with pytest.raises(RuntimeError, match="cleanup did not commit"):
        lease.release()

    assert scheduler._ASYNC_CORRUPTED is True
    assert scheduler._ASYNC_ADMISSION_CLOSED is True
    assert scheduler._ASYNC_PROTOCOL_VIOLATIONS >= 1
    assert lease._released is False
    assert lease._slots_released is False
    assert lease._operation_released is True
    with pytest.raises(SchemaSanitizerResourceError, match="admission is closed"):
        scheduler._acquire_async_task_domain_exact(1)

    # Reconcile the fault-injected counter and retry the same exact capability.
    scheduler._ASYNC_TASK_SLOTS_IN_USE = 2
    lease.release()
    assert lease._released is True
    assert scheduler._ASYNC_TASK_SLOTS_IN_USE == 0
    # Cleanup never reopens admission after a corruption latch.
    assert scheduler._ASYNC_CORRUPTED is True
    assert scheduler._ASYNC_ADMISSION_CLOSED is True


def test_stage_broker_keeps_partial_async_domain_rooted_for_cleanup_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import async_scheduler as scheduler
    from schema_sanitizer.core_impl.memory_budget import StageConcurrencyAdmission

    monkeypatch.setattr(scheduler, "_ASYNC_TASK_SLOTS_IN_USE", 1)
    monkeypatch.setattr(scheduler, "_ASYNC_ACTIVE_OPERATIONS", 1)
    monkeypatch.setattr(scheduler, "_ASYNC_PROTOCOL_VIOLATIONS", 0)
    monkeypatch.setattr(scheduler, "_ASYNC_ADMISSION_CLOSED", False)
    monkeypatch.setattr(scheduler, "_ASYNC_CORRUPTED", False)

    lease = scheduler._AsyncTaskDomainLease(2)
    admission = StageConcurrencyAdmission(
        slots=2, per_slot_bytes=0, domain_leases=(("async_task", lease),)
    )
    with pytest.raises(RuntimeError, match="cleanup did not commit"):
        admission.close()
    assert admission.domain_leases == (("async_task", lease),)
    assert lease._released is False

    scheduler._ASYNC_TASK_SLOTS_IN_USE = 2
    admission.close()
    assert admission.domain_leases == ()
    assert lease._released is True


def test_terminal_slots_are_authority_and_corruption_quarantines_publication() -> None:
    from schema_sanitizer.core_impl.terminal_ownership import TerminalOwnershipLedger

    ledger = TerminalOwnershipLedger(capacity=1)
    assert ledger.publish("async-corruption-closes-admission-but-exact", 1, retained_bytes=64)
    ledger._owners = 0

    # A stale-low cache must not manufacture a second slot/capacity.
    assert (
        ledger.publish("async-corruption-closes-admission-but-exact", 2, retained_bytes=64) is False
    )
    snapshot = ledger.snapshot()
    assert snapshot.owners == 1
    assert snapshot.corrupted is True

    # Exact slot cleanup remains authoritative after quarantine.
    ledger.retire("async-corruption-closes-admission-but-exact", 1)
    snapshot = ledger.snapshot()
    assert snapshot.owners == 0
    assert snapshot.corrupted is True
    assert ledger.publish("async-corruption-closes-admission-but-exact", 3) is False


def test_static_control_plane_recomputes_authoritative_total_and_stays_quarantined(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import static_control_plane as static

    monkeypatch.setattr(static, "_ENTRIES", {"a": 10, "b": 10})
    monkeypatch.setattr(static, "_TOTAL", 10)
    monkeypatch.setattr(static, "_FROZEN", False)
    monkeypatch.setattr(static, "_CORRUPTED", False)

    with pytest.raises(RuntimeError, match="corrupted; admission is closed"):
        static.reserve_static_control_plane("c", 1)
    assert static._CORRUPTED is True
    # Read-side accounting is conservative/exact even after cache corruption.
    assert static.static_control_plane_bytes() == 20

    # Exact cleanup is still legal and repairs the aggregate cache only.
    assert static.rollback_static_control_plane("a", 10) is True
    assert static._TOTAL == 10
    assert static._ENTRIES == {"b": 10}
    assert static._CORRUPTED is True
    with pytest.raises(RuntimeError, match="corrupted; admission is closed"):
        static.reserve_static_control_plane("c", 1)


def test_native_operation_ledger_overrelease_has_irreversible_admission_latch() -> None:
    source = (CPP / "internal" / "memory" / "memory_pool.cc").read_text(encoding="utf-8")
    header = (CPP / "internal" / "memory" / "memory_pool.hh").read_text(encoding="utf-8")
    reserve = source[
        source.index("sanitize::Status OperationMemoryLedger::ReserveLocal") : source.index(
            "int64_t OperationMemoryLedger::ReleaseLocal"
        )
    ]
    release = source[
        source.index("int64_t OperationMemoryLedger::ReleaseLocal") : source.index(
            "sanitize::Status OperationMemoryLedger::Reserve("
        )
    ]
    assert "state & kCorruptedBit" in reserve
    # C++ permits adjacent literals to be split by formatters without changing
    # the emitted diagnostic.  Compare their concatenated source spelling.
    reserve_text = re.sub(r'"\s*"', "", reserve)
    assert "admission closed after accounting corruption" in reserve_text
    assert "state_.compare_exchange_weak" in reserve
    assert "over_release ? kCorruptedBit" in release
    assert "state_.compare_exchange_weak" in release
    assert "std::atomic<std::uint64_t> state_{0}" in header
    assert "static constexpr std::uint64_t kCorruptedBit" in header


def test_native_operation_ledger_runtime_rejects_reserve_after_overrelease_if_built() -> None:
    try:
        from schema_sanitizer import _core_abi3 as native
    except ImportError:
        pytest.skip("native ABI3 core is not built in this test environment")

    ledger = native.operation_memory_ledger_create(100)
    native.operation_memory_ledger_reserve(
        ledger, 100, "async-corruption-closes-admission-but-exact"
    )
    native.operation_memory_ledger_release(ledger, 150)
    assert native.operation_memory_ledger_diagnostics(ledger) == (1, 50)
    with pytest.raises(MemoryError, match="admission closed after accounting corruption"):
        native.operation_memory_ledger_reserve(
            ledger, 1, "async-corruption-closes-admission-but-exact_readmit"
        )


def test_fork_safety_bootstrap_does_not_recycle_bank_on_third_nested_fork(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import fork_safety

    monkeypatch.setattr(fork_safety, "_FORK_GENERATION", 2)
    monkeypatch.setattr(fork_safety, "_CHILD_LOCK_BANK_INDEX", 0)
    monkeypatch.setattr(fork_safety, "_PREPARED_CHILD_LOCK", None)
    fork_safety._prepare_fork_child_state()
    assert fork_safety._PREPARED_CHILD_LOCK is None


def test_fork_manager_blocks_third_nested_prepared_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from schema_sanitizer.core_impl import fork_manager, fork_safety

    called: list[str] = []
    handler = fork_manager._ForkHandler(
        "async-corruption-closes-admission-but-exact-dummy",
        lambda: called.append("before"),
        None,
        None,
        1,
        False,
        "prepared_swap",
    )
    handlers = [None] * fork_manager._MAX_FORK_HANDLERS
    handlers[0] = handler
    monkeypatch.setattr(fork_manager, "_HANDLERS", handlers)
    monkeypatch.setattr(fork_manager, "_COUNT", 1)
    monkeypatch.setattr(fork_safety, "_FORK_GENERATION", 2)
    monkeypatch.setattr(fork_manager, "_FORK_GENERATION_COUNT", 0)
    monkeypatch.setattr(fork_manager, "_FORK_GENERATION_ACTIVE", False)

    fork_manager._before()
    assert called == []
    assert fork_manager._FORK_GENERATION_COUNT == 0
    assert fork_manager._FORK_GENERATION_ACTIVE is True
    fork_manager._parent()


def test_before_fork_callbacks_only_select_preallocated_state() -> None:
    partition = (SRC / "api_impl" / "partition_resources.py").read_text(encoding="utf-8")
    sessions = (SRC / "remote_impl" / "provider_session_pool.py").read_text(encoding="utf-8")
    contracts = (SRC / "core_impl" / "concurrency_contracts.py").read_text(encoding="utf-8")

    p_start = partition.index("def _prepare_partition_resources_for_fork")
    p_end = partition.index("\ndef _clear_partition_resources_fork_preparation", p_start)
    assert "ContextVar(" not in partition[p_start:p_end]
    assert "_FORK_CURRENT_PARTITION_RESOURCES_BANKS" in partition[p_start:p_end]

    s_start = sessions.index("def _prepare_provider_session_pool_for_fork")
    s_end = sessions.index("\ndef _clear_provider_session_pool_fork_preparation", s_start)
    assert "ContextVar(" not in sessions[s_start:s_end]
    assert "_FORK_CURRENT_POOL_BANKS" in sessions[s_start:s_end]

    c_start = contracts.index("def _prepare_contracts_for_fork")
    c_end = contracts.index("\ndef _clear_contracts_fork_preparation", c_start)
    callback = contracts[c_start:c_end]
    assert ".clear()" not in callback
    assert "observed[name] = 0" not in callback


def test_fork_manager_contains_global_one_shot_bank_guard() -> None:
    source = (SRC / "core_impl" / "fork_manager.py").read_text(encoding="utf-8")
    block = source[source.index("def _before()") : source.index("\ndef _parent()")]
    assert "fork_quarantine_generation() > 1" in block
    assert block.index("fork_quarantine_generation() > 1") < block.index("callback()")
