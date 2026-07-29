"""Regression coverage for v81 worker-active streak accounting."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from conftest import require_native

import schema_sanitizer as ss
from schema_sanitizer.api_impl.execution_context import default_pool
from schema_sanitizer.core_impl.concurrency_coverage import (
    concurrency_pair_guarantees,
)

ROOT = Path(__file__).resolve().parents[1]
ARENA_RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
TELEMETRY_HEADER = ROOT / "cpp/src/internal/runtime/performance_telemetry.hh"
TELEMETRY_SOURCE = ROOT / "cpp/src/internal/runtime/performance_telemetry.cc"


def test_v81_activity_is_accounted_once_per_worker_streak() -> None:
    """Adjacent packets retain one active transition until the worker parks."""
    runtime = ARENA_RUNTIME.read_text(encoding="utf-8")

    assert "class WorkerActivityStreak final" in runtime
    assert "if (active_)" in runtime
    assert "activity.Start();" in runtime
    assert "activity.Stop();" in runtime
    assert runtime.count("state_->active.fetch_add") == 1
    assert runtime.count("state_->active.fetch_sub") == 1
    assert "local_peak_active_" in runtime
    assert "PerformanceCounter::kWorkerActiveStreaks" in runtime


def test_v81_park_transition_rechecks_local_work_without_stranding() -> None:
    """Targeted wake coalescing cannot miss work appended before parking."""
    runtime = ARENA_RUNTIME.read_text(encoding="utf-8")
    recheck = runtime.index("if (!slot.tasks.empty())")
    stop = runtime.index("activity.Stop();", recheck)
    wait = runtime.index("WaitWithStop(slot.ready", stop)

    assert recheck < stop < wait
    assert "while this worker was still advertised as running" in runtime
    assert "observed_epoch = slot.wake_epoch.load" in runtime[recheck:stop]


def test_v81_telemetry_exposes_exact_streak_count() -> None:
    """The new counter is bounded and does not add a public resource control."""
    header = TELEMETRY_HEADER.read_text(encoding="utf-8")
    source = TELEMETRY_SOURCE.read_text(encoding="utf-8")

    assert "kWorkerActiveStreaks" in header
    assert '"worker_active_streaks"' in source
    assert "getenv" not in header + source
    assert "memory_limit_bytes" in header


def test_v81_all_56_pairs_inherit_active_streak_reduction() -> None:
    """Every input/output route executes through the common streak-aware arena."""
    pairs = concurrency_pair_guarantees()

    assert sum(len(outputs) for outputs in pairs.values()) == 56
    for input_name, outputs in pairs.items():
        assert len(outputs) == 7, input_name
        for output_name, guarantee in outputs.items():
            assert "worker_active_streak_accounting" in guarantee["shared_parallel_stages"], (
                input_name,
                output_name,
            )
            assert guarantee["source_to_sink_parallel_path"] is True
            assert guarantee["eligible_multi_benefit"] is True


def test_v81_public_pipeline_reports_fewer_streaks_than_tasks(
    tmp_path: Path,
) -> None:
    """A sustained public conversion amortizes active transitions across packets."""
    require_native()
    if not hasattr(os, "sched_getaffinity") or not hasattr(os, "sched_setaffinity"):
        pytest.skip("CPU affinity is required for the four-worker contract")
    original_affinity = os.sched_getaffinity(0)
    if len(original_affinity) < 4:
        pytest.skip("at least four available CPUs are required")
    selected = set(sorted(original_affinity)[:4])

    source = tmp_path / "rows.jsonl"
    output = tmp_path / "rows.csv"
    with source.open("w", encoding="utf-8") as handle:
        for row in range(16_000):
            handle.write(
                json.dumps(
                    {f"field_{column:02d}": row * 40 + column for column in range(40)},
                    separators=(",", ":"),
                )
                + "\n"
            )

    try:
        os.sched_setaffinity(0, selected)
        ss.to_csv(
            source,
            output,
            input_format="jsonl",
            multi_threading=True,
            memory_limit_bytes=96 << 20,
            parse_integers=True,
            field_name_policy="preserve",
        )
        stats = default_pool().get().performance_stats()
    finally:
        os.sched_setaffinity(0, original_affinity)

    tasks = stats["tasks"]
    started = sum(int(values["started"]) for values in tasks.values())
    finished = sum(int(values["finished"]) for values in tasks.values())
    streaks = int(stats["counters"]["worker_active_streaks"])

    assert started == finished > 4
    assert 0 < streaks < finished
    assert int(stats["counters"]["peak_active_tasks"]) >= 2
    assert output.exists() and output.stat().st_size > 0
