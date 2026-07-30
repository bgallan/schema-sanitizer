"""Regression coverage for v86 precompiled arena submission plans."""

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
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
EXECUTOR = ROOT / "cpp/src/internal/runtime/ordered_executor.hh"
SUBMISSION = ROOT / "cpp/src/internal/runtime/ordered_executor_submission.cc.inc"


def test_v86_submission_plan_retains_all_invariant_lane_geometry() -> None:
    """One immutable plan owns every value previously rebuilt per packet."""
    header = ARENA_HEADER.read_text(encoding="utf-8")
    arena = ARENA.read_text(encoding="utf-8")

    assert "struct TaskArenaSubmissionPlan final" in header
    for field in (
        "lane_begin",
        "lane_end",
        "width",
        "alternative_offset",
        "allowed_mask",
        "cursor",
    ):
        assert field in header
    prepare = arena[
        arena.index("OperationTaskArena::PrepareSubmissionPlan") : arena.index(
            "sanitize::Status OperationTaskArena::Submit",
            arena.index("OperationTaskArena::PrepareSubmissionPlan"),
        )
    ]
    assert "lane_mask(plan.lane_begin, plan.lane_end)" in prepare
    assert "plan.cursor = &state_->upstream_cursor" in prepare
    assert "plan.cursor = &state_->output_cursor" in prepare
    assert "plan.cursor = &state_->all_cursor" in prepare


def test_v86_planned_submit_reuses_mask_cursor_and_alternative_offset() -> None:
    """The hot admission path consumes the plan instead of rebuilding it."""
    arena = ARENA.read_text(encoding="utf-8")
    runtime = RUNTIME.read_text(encoding="utf-8")
    planned = arena[
        arena.index("const auto lane_begin = plan.lane_begin;") : arena.index(
            "OperationTaskArena::worker_count"
        )
    ]

    assert "plan.cursor->fetch_add" in arena
    assert "const auto ticket = submission_ticket;" in planned
    assert "plan.allowed_mask" in planned
    assert "plan.alternative_offset" in planned
    assert "std::min(lane_width" not in planned
    assert "lane_mask(lane_begin, lane_end)" not in planned
    assert "std::uint64_t allowed" in runtime


def test_v86_ordered_executor_prepares_once_and_reuses_for_both_submit_paths() -> None:
    """Normal and high-core executor submission share the cached plan."""
    executor = EXECUTOR.read_text(encoding="utf-8")
    high_core = SUBMISSION.read_text(encoding="utf-8")

    assert executor.count("PrepareSubmissionPlan(worker_count_, lane_)") == 1
    assert "TaskArenaSubmissionPlan arena_submission_plan_;" in executor
    assert "arena_submission_plan_, telemetry_kind_" in executor
    assert "arena_submission_plan_, arena_submission_ticket, telemetry_kind_" in high_core
    assert "worker_count_, lane_, telemetry_kind_" not in high_core


def test_v86_all_56_pairs_inherit_precompiled_stage_submission() -> None:
    """Every supported source and sink crosses a planned ordered stage."""
    pairs = concurrency_pair_guarantees()

    assert sum(len(outputs) for outputs in pairs.values()) == 56
    for input_name, outputs in pairs.items():
        assert len(outputs) == 7, input_name
        for output_name, guarantee in outputs.items():
            assert "precompiled_stage_submission_plan" in guarantee["shared_parallel_stages"]
            assert guarantee["source_to_sink_parallel_path"] is True
            assert guarantee["eligible_multi_benefit"] is True


def test_v86_native_ordered_completion_preserves_exact_contract() -> None:
    """Cached geometry leaves bounded completion and ordinal order unchanged."""
    require_native()
    elapsed, completed, checksum, started, peak, queued, submitted = (
        native_core.ordered_executor_arena_completion_probe(4, 20_000, 0)
    )

    assert elapsed > 0
    assert completed == 20_000
    assert checksum >= 0
    assert started == 4
    assert 1 <= peak <= 4
    assert queued == 0
    assert submitted == 20_000
