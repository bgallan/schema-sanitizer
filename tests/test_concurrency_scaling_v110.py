"""Regression coverage for v110 worker-local peak-active caching."""

from __future__ import annotations

import json
from pathlib import Path

from schema_sanitizer.core_impl.concurrency_coverage import (
    concurrency_pair_guarantees,
)

ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
DOC = ROOT / "CONCURRENCY_SCALING_V110.md"
EVIDENCE = ROOT / "benchmarks/v110_worker_local_peak_cache_ab.json"
STAGE = "worker_local_monotonic_peak_active_cache"


def test_v110_worker_cache_elides_redundant_global_peak_loads() -> None:
    """Only a worker-local active high-water mark reaches the shared maximum."""
    source = RUNTIME.read_text(encoding="utf-8")
    activity = source.split("class WorkerActivityStreak final", 1)[1].split(
        "template <bool PreferDedicatedOutput", 1
    )[0]

    assert "std::size_t local_peak_active_ = 0;" in activity
    assert "if (active > local_peak_active_)" in activity
    assert "local_peak_active_ = active;" in activity
    assert "update_peak(&state_->peak_active, active)" in activity
    assert activity.index("if (active > local_peak_active_)") < activity.index(
        "update_peak(&state_->peak_active, active)"
    )
    assert activity.count("state_->active.fetch_add") == 1
    assert activity.count("state_->active.fetch_sub") == 1


def test_v110_cache_preserves_exact_peak_proof() -> None:
    """The implementation documents why a cached value cannot hide a new peak."""
    source = RUNTIME.read_text(encoding="utf-8")

    assert "worker is the sole writer" in source
    assert "was already offered to update_peak()" in source
    assert "covered by the global" in source
    assert "std::memory_order_relaxed" in source


def test_v110_tsan_probe_exercises_repeated_real_arena_streaks() -> None:
    """The standalone probe checks exact peaks and repeated park/wake waves."""
    source = (ROOT / "benchmarks/v110_worker_local_peak_cache_tsan.cc").read_text(encoding="utf-8")

    assert "OperationTaskArena::Make(workers)" in source
    assert "for (const auto workers : {4U, 8U, 16U})" in source
    assert "arena->active_tasks() != workers" in source
    assert "arena->peak_active_tasks() != workers" in source
    assert "constexpr std::size_t kWaves = 128U" in source
    assert "arena->queued_tasks() == 0U" in source


def test_v110_all_56_pairs_inherit_worker_local_peak_cache() -> None:
    """Every supported source/sink pair crosses the common arena activity path."""
    pairs = concurrency_pair_guarantees()

    assert sum(len(outputs) for outputs in pairs.values()) == 56
    for input_name, outputs in pairs.items():
        assert len(outputs) == 7, input_name
        for output_name, guarantee in outputs.items():
            assert STAGE in guarantee["shared_parallel_stages"], (
                input_name,
                output_name,
            )
            assert guarantee["source_to_sink_parallel_path"] is True
            assert guarantee["eligible_multi_benefit"] is True


def test_v110_evidence_is_scoped_to_peak_bookkeeping() -> None:
    """The benchmark records paired evidence without claiming pipeline speedup."""
    text = DOC.read_text(encoding="utf-8")
    evidence = json.loads(EVIDENCE.read_text(encoding="utf-8"))

    assert "8 x 7 = 56" in text
    assert "pure-Python rows" in text
    assert "not end-to-end" in text
    assert evidence["pair_count"] == 15
    assert "peak-active bookkeeping" in evidence["scope"]
    scenarios = {int(item["workers"]): item for item in evidence["scenarios"]}
    assert set(scenarios) == {4, 8, 16}
    for item in scenarios.values():
        assert item["candidate_wins"] >= 10
        assert item["paired_median_reduction_percent"] > 15.0
