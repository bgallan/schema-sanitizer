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
DOC = ROOT / "CONCURRENCY_SCALING_V92.md"
STAGE = "high_core_executor_local_arena_submission_tickets"


def test_v92_arena_exposes_pre_reserved_ticket_submission() -> None:
    header = ARENA_HEADER.read_text(encoding="utf-8")
    source = ARENA.read_text(encoding="utf-8")

    assert "ReserveSubmissionTicket(" in header
    assert "std::size_t submission_ticket" in header
    assert "const auto ticket = ReserveSubmissionTicket(plan);" in source
    assert "return Submit(std::move(task), plan, ticket, telemetry_kind);" in source
    assert "const auto ticket = submission_ticket;" in source


def test_v92_shared_lane_cursor_is_touched_once_per_executor_not_per_packet() -> None:
    source = ARENA.read_text(encoding="utf-8")
    reserve_start = source.index("OperationTaskArena::ReserveSubmissionTicket")
    reserve_end = source.index("sanitize::Status OperationTaskArena::Submit", reserve_start)
    reserve = source[reserve_start:reserve_end]
    ticket_submit = source[source.index("std::size_t submission_ticket") :]

    assert reserve.count("plan.cursor->fetch_add") == 1
    assert "plan.cursor->fetch_add" not in ticket_submit

    executor = EXECUTOR.read_text(encoding="utf-8")
    constructor = executor[executor.index("OrderedExecutor(std::size_t") :]
    assert "worker_count_ > 8U" in constructor
    assert "arena_->ReserveSubmissionTicket(arena_submission_plan_)" in constructor
    assert executor.count("ReserveSubmissionTicket(arena_submission_plan_)") == 1


def test_v92_only_high_core_helper_advances_ticket_under_existing_mutex() -> None:
    executor = EXECUTOR.read_text(encoding="utf-8")
    helper = SUBMISSION.read_text(encoding="utf-8")

    submit_body = executor[
        executor.index("sanitize::Status Submit") : executor.index(
            "sanitize::Status FinishSubmission"
        )
    ]
    assert "worker_count_ > 8 && uses_arena_completion_slots()" in submit_body
    assert "next_high_core_arena_ticket_" not in submit_body
    assert "arena_submission_plan_, telemetry_kind_" in submit_body

    helper_lock = helper.index("std::lock_guard lock(mutex_);")
    helper_ticket = helper.index("arena_submission_ticket = next_high_core_arena_ticket_++;")
    assert helper_lock < helper_ticket
    assert "arena_submission_plan_, arena_submission_ticket, telemetry_kind_" in helper


def test_v92_ticket_skip_on_failed_admission_needs_no_shared_rollback() -> None:
    executor = EXECUTOR.read_text(encoding="utf-8")
    helper = SUBMISSION.read_text(encoding="utf-8")

    for source in (executor, helper):
        failure = source[source.index("if (!submit_status.ok())") :]
        assert "--next_high_core_arena_ticket_" not in failure
        assert "--next_submit_ordinal_" in failure
        assert "completion_ring_.RollbackSubmit();" in failure


def test_v92_all_56_pairs_inherit_high_core_local_tickets() -> None:
    pairs = concurrency_pair_guarantees()

    assert sum(len(outputs) for outputs in pairs.values()) == 56
    for input_name, outputs in pairs.items():
        assert len(outputs) == 7, input_name
        for guarantee in outputs.values():
            assert STAGE in guarantee["shared_parallel_stages"]
            assert guarantee["source_to_sink_parallel_path"] is True
            assert guarantee["eligible_multi_benefit"] is True


def test_v92_documentation_records_complete_coverage_and_guardrails() -> None:
    text = DOC.read_text(encoding="utf-8")

    assert "8 x 7" in text
    assert "pure-Python" in text
    assert "one shared atomic RMW per packet" in text
    assert ">8 workers" in text
    assert "seed" in text
    assert "memory remains bounded" in text


def test_v92_native_local_tickets_preserve_order_and_exact_drain() -> None:
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
    require_native()
    elapsed_us, active, observed_stop, queued = (
        native_core.operation_task_arena_cancellation_probe()
    )
    assert elapsed_us > 0
    assert active == 0
    assert observed_stop >= 1
    assert queued == 0
