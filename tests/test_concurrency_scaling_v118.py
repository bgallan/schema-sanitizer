"""Regression coverage for v118 single-modulo lane-origin reuse."""

from __future__ import annotations

import json
from pathlib import Path

from schema_sanitizer.core_impl.concurrency_coverage import (
    concurrency_pair_guarantees,
)

ROOT = Path(__file__).resolve().parents[1]
ARENA = ROOT / "cpp/src/internal/runtime/operation_task_arena.cc"
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
SELECTION = ROOT / "cpp/src/internal/runtime/operation_task_arena_selection.hh"
EVIDENCE = ROOT / "benchmarks/v118_single_modulo_lane_origin_ab.json"
PROBE = ROOT / "benchmarks/v118_single_modulo_lane_origin_tsan.cc"
STAGE = "single_modulo_lane_origin_reuse"


def test_v118_runtime_selection_consumes_normalized_origins() -> None:
    """Startup and idle selection avoid recomputing ticket division."""
    runtime = RUNTIME.read_text(encoding="utf-8")
    selection = SELECTION.read_text(encoding="utf-8")

    assert runtime.count("task_arena_detail::kNormalizedLaneOrigin") == 2
    assert "std::size_t lane_origin" in runtime
    assert (
        "std::size_t ticket"
        not in runtime.split("reserve_unstarted_worker", 1)[1].split(
            "queue_visibility_snapshot", 1
        )[0]
    )
    assert "const auto start = ticket % width;" in selection
    assert "advance_normalized_lane_origin" in selection


def test_v118_overflow_fallback_preserves_historical_sequence() -> None:
    """Only size_t wraparound falls back to the exact historical modulo."""
    selection = SELECTION.read_text(encoding="utf-8")
    probe = PROBE.read_text(encoding="utf-8")
    compact = probe.replace(" ", "").replace("\n", "")

    assert "std::numeric_limits<std::size_t>::max() - delta" in selection
    assert "return (ticket + delta) % width;" in selection
    assert "width=1U;width<=32U;++width" in compact
    assert "std::numeric_limits<std::size_t>::max()" in probe
    assert "(ticket+alternative)%width" in compact
    assert "(ticket+1U)%width" in compact


def test_v118_probe_covers_real_mixed_lane_arena() -> None:
    """The native/TSan probe crosses odd/even widths and all stage lanes."""
    source = PROBE.read_text(encoding="utf-8")
    compact = source.replace(" ", "").replace("\n", "")

    assert "{2U,3U,4U,5U,8U,16U,32U}" in compact
    assert "TaskArenaLane::kUpstream" in source
    assert "TaskArenaLane::kOutput" in source
    assert "TaskArenaLane::kAll" in source
    assert "kProducerCount=3U" in compact
    assert "arena->submitted_tasks()==kTasks" in compact
    assert "arena->active_tasks()==0U" in compact
    assert "arena->queued_tasks()==0U" in compact


def test_v118_all_56_pairs_inherit_single_modulo_origin() -> None:
    """Every supported source/sink pair crosses the shared admission path."""
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


def test_v118_evidence_covers_odd_and_power_of_two_widths() -> None:
    """Paired evidence remains positive across representative lane widths."""
    evidence = json.loads(EVIDENCE.read_text(encoding="utf-8"))

    assert evidence["pair_count"] == 15
    assert evidence["iterations_per_process"] == 20_000_000
    assert "does not measure parsing" in evidence["scope"]
    scenarios = {int(item["lane_width"]): item for item in evidence["scenarios"]}
    assert set(scenarios) == {3, 5, 8, 16, 32}
    for item in scenarios.values():
        assert item["candidate_wins"] == 15
        assert item["paired_median_reduction_percent"] > 60.0


def test_v118_version_is_at_least_0371() -> None:
    """Later source packages retain the v118 minimum project version."""
    version = (ROOT / "meta/VERSION").read_text(encoding="utf-8").strip()
    assert tuple(map(int, version.split("."))) >= (0, 3, 71)
