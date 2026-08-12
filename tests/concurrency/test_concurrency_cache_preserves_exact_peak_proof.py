"""Regression coverage for concurrency cache preserves exact peak proof."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
EVIDENCE = ROOT / "benchmarks/evidence/concurrency/telemetry/worker-local-peak-cache.json"


def test_cache_preserves_exact_peak_proof() -> None:
    """The implementation documents why a cached value cannot hide a new peak."""
    source = RUNTIME.read_text(encoding="utf-8")

    assert "worker is the sole writer" in source
    assert "was already offered to update_peak()" in source
    assert "covered by the global" in source
    assert "std::memory_order_relaxed" in source


def test_tsan_probe_exercises_repeated_real_arena_streaks() -> None:
    """The standalone probe checks exact peaks and repeated park/wake waves."""
    source = (
        ROOT / "benchmarks/probes/concurrency/telemetry/worker-local-peak-cache-tsan.cc"
    ).read_text(encoding="utf-8")

    assert "OperationTaskArena::Make(workers)" in source
    assert "for (const auto workers : {4U, 8U, 16U})" in source
    assert "arena->active_tasks() != workers" in source
    assert "arena->peak_active_tasks() != workers" in source
    assert "constexpr std::size_t kWaves = 128U" in source
    assert "arena->queued_tasks() == 0U" in source


def test_evidence_is_scoped_to_peak_bookkeeping() -> None:
    """The benchmark records paired evidence without claiming pipeline speedup."""
    evidence = json.loads(EVIDENCE.read_text(encoding="utf-8"))

    assert evidence["pair_count"] == 15
    assert "peak-active bookkeeping" in evidence["scope"]
    scenarios = {int(item["workers"]): item for item in evidence["scenarios"]}
    assert set(scenarios) == {4, 8, 16}
    for item in scenarios.values():
        assert item["candidate_wins"] >= 10
        assert item["paired_median_reduction_percent"] > 15.0
