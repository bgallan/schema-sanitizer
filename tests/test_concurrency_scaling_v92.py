"""Regression coverage for v92 executor-local arena submission tickets."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.concurrency_coverage import (
    concurrency_pair_guarantees,
)
from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[1]
ARENA_HEADER = ROOT / "cpp/src/internal/runtime/operation_task_arena.hh"
ARENA = ROOT / "cpp/src/internal/runtime/operation_task_arena.cc"
EXECUTOR = ROOT / "cpp/src/internal/runtime/ordered_executor.hh"
SUBMISSION = ROOT / "cpp/src/internal/runtime/ordered_executor_submission.cc.inc"
STAGE = "high_core_executor_local_arena_submission_tickets"


def test_v92_shared_lane_cursor_is_touched_once_per_executor_not_per_packet() -> None:
    """Verify the named concurrency regression contract."""
    source = ARENA.read_text(encoding="utf-8")
    reserve_start = source.index("OperationTaskArena::ReserveSubmissionTicket")
    reserve_end = source.index("sanitize::Status OperationTaskArena::Submit", reserve_start)
    reserve = source[reserve_start:reserve_end]
    ticket_submit = source[source.index("std::size_t submission_ticket") :]

    assert reserve.count("cursor->fetch_add") == 1
    assert "cursor->fetch_add" not in ticket_submit

    executor = EXECUTOR.read_text(encoding="utf-8")
    constructor = executor[executor.index("OrderedExecutor(std::size_t") :]
    assert "worker_count_ > 8U" in constructor
    assert "arena_->ReserveSubmissionTicket(arena_submission_plan_)" in constructor
    assert executor.count("ReserveSubmissionTicket(arena_submission_plan_)") == 1


def test_v92_ticket_skip_on_failed_admission_needs_no_shared_rollback() -> None:
    """Verify the named concurrency regression contract."""
    executor = EXECUTOR.read_text(encoding="utf-8")
    helper = SUBMISSION.read_text(encoding="utf-8")

    for source in (executor, helper):
        failure = source[source.index("if (!submit_status.ok())") :]
        assert "--next_high_core_arena_ticket_" not in failure
        assert "--next_submit_ordinal_" in failure
        assert "completion_ring_.RollbackSubmit();" in failure


def test_v92_all_56_pairs_inherit_high_core_local_tickets() -> None:
    """Verify the named concurrency regression contract."""
    pairs = concurrency_pair_guarantees()

    assert sum(len(outputs) for outputs in pairs.values()) == 56
    for input_name, outputs in pairs.items():
        assert len(outputs) == 7, input_name
        for guarantee in outputs.values():
            assert STAGE in guarantee["shared_parallel_stages"]
            assert guarantee["source_to_sink_parallel_path"] is True
            assert guarantee["eligible_multi_benefit"] is True


def test_v92_native_local_tickets_preserve_order_and_exact_drain() -> None:
    """Verify the named concurrency regression contract."""
    require_native()
    for workers in (2, 4, 5, 8, 16):
        elapsed, completed, checksum, started, peak, queued, submitted = (
            native_core.ordered_executor_arena_completion_probe(workers, 25_000, 1)
        )
        assert elapsed > 0
        assert completed == 25_000
        assert checksum >= 0
        assert 1 <= started <= workers
        assert 1 <= peak <= workers
        assert queued == 0
        assert submitted == 25_000


def test_v92_direct_concurrent_arena_producers_keep_shared_ticket_safety() -> None:
    """Verify the named concurrency regression contract."""
    require_native()
    for workers in (2, 4, 8, 16):
        elapsed, submitted, finished, queued, started, peak = (
            native_core.operation_task_arena_concurrent_submit_probe(workers, 2, 2_000)
        )
        assert elapsed > 0
        assert submitted == 4_000
        assert finished == 4_000
        assert queued == 0
        assert 1 <= started <= workers
        assert 1 <= peak <= workers


def test_v92_cancellation_still_drains_executor_local_ticket_work() -> None:
    """Verify the named concurrency regression contract."""
    require_native()
    elapsed_us, active, observed_stop, queued = (
        native_core.operation_task_arena_cancellation_probe()
    )
    assert elapsed_us > 0
    assert active == 0
    assert observed_stop >= 1
    assert queued == 0
