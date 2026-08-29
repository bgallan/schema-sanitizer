"""Test the worker transition from active execution into a parked state.

A final local-work recheck must prevent stranding, and exact streak telemetry must remain visible
while the public pipeline reports bounded scheduling streaks after all tasks drain.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

import schema_sanitizer as ss
from schema_sanitizer.api_impl.execution_context import default_pool

ROOT = Path(__file__).resolve().parents[2]
ARENA_RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
TELEMETRY_HEADER = ROOT / "cpp/src/internal/runtime/performance_telemetry.hh"
TELEMETRY_SOURCE = ROOT / "cpp/src/internal/runtime/performance_telemetry.cc"


def test_park_transition_rechecks_local_work_without_stranding() -> None:
    """Targeted wake coalescing cannot miss work appended before parking."""
    runtime = ARENA_RUNTIME.read_text(encoding="utf-8")
    recheck = runtime.index("if (!slot.tasks.empty())")
    stop = runtime.index("activity.Stop();", recheck)
    wait = runtime.index("WaitWithStop(slot.ready", stop)

    assert recheck < stop < wait
    assert "while this worker was still advertised as running" in runtime
    assert "observed_epoch = slot.wake_epoch.load" in runtime[recheck:stop]


def test_telemetry_exposes_exact_streak_count() -> None:
    """The new counter is bounded and does not add a public resource control."""
    header = TELEMETRY_HEADER.read_text(encoding="utf-8")
    source = TELEMETRY_SOURCE.read_text(encoding="utf-8")

    assert "kWorkerActiveStreaks" in header
    assert '"worker_active_streaks"' in source
    assert "getenv" not in header + source
    assert "memory_limit_bytes" in header


def test_public_pipeline_reports_bounded_streak_count(
    tmp_path: Path,
    require_native: None,
) -> None:
    """A public conversion publishes a valid streak count after draining tasks."""
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
            memory_limit_bytes=128 << 20,
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
    assert 0 < streaks <= finished
    assert output.exists() and output.stat().st_size > 0
