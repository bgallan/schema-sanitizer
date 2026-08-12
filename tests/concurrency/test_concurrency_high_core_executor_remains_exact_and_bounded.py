"""Regression coverage for concurrency high core executor remains exact and bounded."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.native_runtime import native_core


def test_high_core_executor_remains_exact_and_bounded() -> None:
    """Telemetry batching cannot alter ordered completion or worker ceilings."""
    require_native()
    result = native_core.ordered_executor_arena_completion_probe(16, 30_000, 16)

    assert result[1] == 30_000
    assert result[2] != 0
    assert 1 <= result[3] <= 16
    assert 1 <= result[4] <= 16
    assert result[5] == 0
    assert result[6] == 30_000


def test_high_core_batching_remains_bounded() -> None:
    """The high-core-executor-remains-exact-and-bounded 32-task batching bound survives later shard publication."""
    root = Path(__file__).resolve().parents[2]
    runtime = (root / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc").read_text(
        encoding="utf-8"
    )
    task_telemetry = (root / "cpp/src/internal/runtime/operation_task_telemetry.cc.inc").read_text(
        encoding="utf-8"
    )
    telemetry = (root / "cpp/src/internal/runtime/performance_telemetry.cc").read_text(
        encoding="utf-8"
    )

    assert '#include "internal/runtime/operation_task_telemetry.cc.inc"' in runtime
    assert "class TaskTelemetryBatch final" in task_telemetry
    assert "worker_count > 8U ? 32U : 8U" in task_telemetry
    assert "telemetry_->RecordWorkerTaskBatch(" in task_telemetry
    assert "RecordTaskBatch(" in telemetry
    assert "task_started_[index].fetch_add(task_count" in telemetry
    assert "task_finished_[index].fetch_add(task_count" in telemetry


def test_low_and_high_core_paths_are_value_deterministic() -> None:
    """Crossing the batching gate cannot change the ordered value oracle."""
    require_native()
    low = native_core.ordered_executor_arena_completion_probe(8, 12_000, 32)
    high = native_core.ordered_executor_arena_completion_probe(16, 12_000, 32)

    assert low[1] == high[1] == 12_000
    assert low[2] == high[2]
    assert low[5] == high[5] == 0
