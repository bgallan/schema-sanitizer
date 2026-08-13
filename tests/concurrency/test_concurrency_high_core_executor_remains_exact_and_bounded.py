"""Regression coverage for concurrency high core executor remains exact and bounded."""

from __future__ import annotations

from pathlib import Path


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
