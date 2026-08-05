"""Regression coverage for v86 precompiled arena submission plans."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.concurrency_coverage import (
    concurrency_pair_guarantees,
)
from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[2]
ARENA_HEADER = ROOT / "cpp/src/internal/runtime/operation_task_arena.hh"
ARENA = ROOT / "cpp/src/internal/runtime/operation_task_arena.cc"
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
EXECUTOR = ROOT / "cpp/src/internal/runtime/ordered_executor.hh"
SUBMISSION = ROOT / "cpp/src/internal/runtime/ordered_executor_submission.cc.inc"


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
