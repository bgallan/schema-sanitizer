"""Regression coverage for concurrency worker shard publishes exact cumulative snapshot."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
TELEMETRY_HEADER = ROOT / "cpp/src/internal/runtime/performance_telemetry.hh"
TELEMETRY_SOURCE = ROOT / "cpp/src/internal/runtime/performance_telemetry.cc"
TELEMETRY_JSON = ROOT / "cpp/src/internal/runtime/performance_telemetry_json.cc.inc"
EVIDENCE = ROOT / "benchmarks/evidence/concurrency/telemetry/active-streak-shards.json"


def test_worker_shard_publishes_exact_cumulative_snapshot() -> None:
    """Each physical worker owns a plain total and publishes one store."""
    header = TELEMETRY_HEADER.read_text(encoding="utf-8")
    source = TELEMETRY_SOURCE.read_text(encoding="utf-8")
    method = source.split("RecordWorkerActiveStreak", 1)[1].split("RecordWorkerStarted", 1)[0]

    assert "std::uint64_t active_streaks_local = 0;" in header
    assert "std::atomic<std::int64_t> active_streaks{0};" in header
    assert "++shard.active_streaks_local;" in method
    assert "shard.active_streaks.store" in method
    assert "std::memory_order_relaxed" in method
    assert "fetch_add" not in method


def test_json_aggregates_global_and_worker_streak_totals() -> None:
    """The public telemetry key remains exact and backward compatible."""
    source = TELEMETRY_JSON.read_text(encoding="utf-8")
    aggregate = source.split("worker_active_streak_count", 1)[1].split("const auto stream_ns", 1)[0]

    assert "PerformanceCounter::kWorkerActiveStreaks" in aggregate
    assert "shard.active_streaks.load" in aggregate
    assert "worker_active_streak_count()" in source


def test_tsan_probe_exercises_exact_real_arena_streaks() -> None:
    """The real arena probe checks concurrent shards and exact JSON totals."""
    source = (
        ROOT / "benchmarks/probes/concurrency/telemetry/active-streak-shards-tsan.cc"
    ).read_text(encoding="utf-8")

    assert "OperationTaskArena::Make(workers, telemetry)" in source
    assert "for (const auto workers : {2U, 4U, 8U, 16U})" in source
    assert "constexpr std::size_t kWaves = 64U" in source
    assert "worker_active_streaks" in source
    assert "arena->active_tasks() == workers" in source
    assert "arena->queued_tasks() == 0U" in source


def test_evidence_is_scoped_and_consistently_positive() -> None:
    """The benchmark reports only isolated telemetry publication evidence."""
    evidence = json.loads(EVIDENCE.read_text(encoding="utf-8"))

    assert evidence["pair_count"] == 15
    assert "active-streak telemetry publication" in evidence["scope"]
    scenarios = {int(item["workers"]): item for item in evidence["scenarios"]}
    assert set(scenarios) == {2, 4, 8, 16}
    for item in scenarios.values():
        assert item["candidate_wins"] == 15
        assert item["paired_median_reduction_percent"] > 80.0
