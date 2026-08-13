"""Regression coverage for concurrency selection visits only ordered set bits."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
SELECTION = ROOT / "cpp/src/internal/runtime/operation_task_arena_selection.hh"
EVIDENCE = ROOT / "benchmarks/evidence/concurrency/scheduler/sparse-round-robin-selection.json"


def test_selection_visits_only_ordered_set_bits() -> None:
    """Compact admission stays ordered and wide admission uses word shards."""
    helper = SELECTION.read_text(encoding="utf-8")
    runtime = RUNTIME.read_text(encoding="utf-8")

    assert "struct OrderedLaneCandidates final" in helper
    assert ".first = relative & ~before_start" in helper
    assert ".wrapped = relative & before_start" in helper
    assert "std::countr_zero(ordered.first)" in helper
    assert "std::countr_zero(ordered.wrapped)" in helper
    assert "relative == width_mask" in helper
    assert runtime.count("task_arena_detail::ordered_lane_candidates(") == 2
    assert "first_ordered_lane_index(" in runtime
    reservation = runtime.split("reserve_unstarted_worker(", 1)[1].split(
        "queue_visibility_snapshot(", 1
    )[0]
    assert "if (state->scalable_scan)" in reservation
    assert "admitted_dynamic.TrySetFirstClear(begin, end, lane_origin)" in reservation
    assert "for (std::size_t offset = 0; offset < width; ++offset)" not in reservation
    assert reservation.index("if (state->scalable_scan)") < reservation.index(
        "task_arena_detail::ordered_lane_candidates("
    )


def test_preserves_startup_cas_and_running_acquire() -> None:
    """Only candidate enumeration changes; synchronization stays authoritative."""
    runtime = RUNTIME.read_text(encoding="utf-8")

    assert "admitted_mask.compare_exchange_weak(" in runtime
    assert "std::memory_order_acq_rel" in runtime
    assert "ordered.full_lane" in runtime
    assert "running.load(\n            std::memory_order_acquire)" in runtime
    assert "ordered.first &= ordered.first - 1U" in runtime
    assert "ordered.wrapped &= ordered.wrapped - 1U" in runtime


def test_probe_checks_equivalence_and_real_arena() -> None:
    """The native probe covers exact ordering and live scheduler execution."""
    source = (
        ROOT / "benchmarks/probes/concurrency/scheduler/sparse-round-robin-selection-tsan.cc"
    ).read_text(encoding="utf-8")
    compact = source.replace(" ", "").replace("\n", "")

    assert "verify_exhaustive_round_robin_equivalence" in source
    assert "verify_wide_random_round_robin_equivalence" in source
    assert "{16U,24U,32U}" in compact
    assert "{2U,4U,8U,16U,32U}" in compact
    assert "TaskArenaLane::kUpstream" in source
    assert "TaskArenaLane::kOutput" in source
    assert "TaskArenaLane::kAll" in source
    assert "arena->submitted_tasks()==kTasks" in compact
    assert "arena->active_tasks()==0U" in compact
    assert "arena->queued_tasks()==0U" in compact


def test_evidence_is_positive_and_narrowly_scoped() -> None:
    """The paired evidence covers three widths without throughput claims."""
    evidence = json.loads(EVIDENCE.read_text(encoding="utf-8"))

    assert evidence["pair_count"] == 15
    assert "round-robin worker selection" in evidence["scope"]
    assert "does not measure parsing" in evidence["scope"]
    scenarios = {int(item["lane_width"]): item for item in evidence["scenarios"]}
    assert set(scenarios) == {8, 16, 32}
    for item in scenarios.values():
        assert item["candidate_wins"] == 15
        assert item["paired_median_reduction_percent"] > 70.0
