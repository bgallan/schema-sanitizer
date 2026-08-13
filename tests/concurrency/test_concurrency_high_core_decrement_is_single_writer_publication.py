"""Regression coverage for concurrency high core decrement is single writer publication."""

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[2]
EXECUTOR = ROOT / "cpp/src/internal/runtime/ordered_executor.hh"
SUBMISSION = ROOT / "cpp/src/internal/runtime/ordered_executor_submission.cc.inc"
COMPLETION = ROOT / "cpp/src/internal/runtime/ordered_executor_arena_completion.cc.inc"
EVIDENCE = ROOT / "benchmarks/evidence/concurrency/scheduler/high-core-inflight-consumption.json"


def test_high_core_decrement_is_single_writer_publication():
    """The high-core helper replaces one locked RMW with load/store publication."""
    header = EXECUTOR.read_text()
    helper = header.split("void decrement_high_core_in_flight_locked() noexcept", 1)[1]
    helper = helper.split("}\n", 1)[0]
    assert "in_flight_.load(std::memory_order_relaxed)" in helper
    assert "in_flight_.store(current - 1U, std::memory_order_release)" in helper
    assert "fetch_sub" not in helper


def test_only_high_core_arena_consumption_uses_helper():
    """Successful consumption keeps the low-core path through eight workers."""
    header = EXECUTOR.read_text()
    completion = COMPLETION.read_text()
    take_dispatch = header.split("sanitize::Result<Outcome> TakeNext()", 1)[1]
    take_dispatch = take_dispatch.split("std::unique_lock lock(mutex_);", 1)[0]
    assert "if (worker_count_ > 8U)" in take_dispatch
    assert "take_next_arena<true>()" in take_dispatch
    assert "take_next_arena<false>()" in take_dispatch
    assert "template <bool HighCore>" in completion
    assert "if constexpr (HighCore)" in completion
    assert "decrement_high_core_in_flight_locked();" in completion
    assert "in_flight_.fetch_sub(1, std::memory_order_release);" in completion


def test_high_core_submission_rollback_matches_publication_strategy():
    """A rejected arena task rolls back the high-core count under the same mutex."""
    submission = SUBMISSION.read_text()
    rollback = submission.split("if (!submit_status.ok())", 1)[1]
    assert "std::lock_guard lock(mutex_);" in rollback
    assert "completion_ring_.RollbackSubmit();" in rollback
    assert "decrement_high_core_in_flight_locked();" in rollback


def test_native_cancellation_still_drains():
    """Terminalization does not leave active or queued arena tasks behind."""
    require_native()
    _, active, observed, queued = native_core.operation_task_arena_cancellation_probe()
    assert active == 0
    assert observed >= 1
    assert queued == 0


def test_evidence_and_scope_are_recorded():
    """The retained benchmark and design note document evidence and limits."""
    import json

    evidence = json.loads(EVIDENCE.read_text())
    assert evidence["pair_count"] == 15
    assert evidence["candidate_wins"] == 15
    assert evidence["paired_median_reduction_percent"] > 80.0
