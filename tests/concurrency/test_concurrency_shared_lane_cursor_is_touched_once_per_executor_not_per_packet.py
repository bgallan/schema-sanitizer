"""Regression coverage for concurrency shared lane cursor is touched once per executor not per packet."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ARENA_HEADER = ROOT / "cpp/src/internal/runtime/operation_task_arena.hh"
ARENA = ROOT / "cpp/src/internal/runtime/operation_task_arena.cc"
EXECUTOR = ROOT / "cpp/src/internal/runtime/ordered_executor.hh"
SUBMISSION = ROOT / "cpp/src/internal/runtime/ordered_executor_submission.cc.inc"


def test_shared_lane_cursor_is_touched_once_per_executor_not_per_packet() -> None:
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


def test_ticket_skip_on_failed_admission_needs_no_shared_rollback() -> None:
    """Verify the named concurrency regression contract."""
    executor = EXECUTOR.read_text(encoding="utf-8")
    helper = SUBMISSION.read_text(encoding="utf-8")

    for source in (executor, helper):
        failure = source[source.index("if (!submit_status.ok())") :]
        assert "--next_high_core_arena_ticket_" not in failure
        assert "--next_submit_ordinal_" in failure
        assert "completion_ring_.RollbackSubmit();" in failure
