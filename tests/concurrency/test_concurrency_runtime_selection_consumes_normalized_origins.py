"""Regression coverage for concurrency runtime selection consumes normalized origins."""

from __future__ import annotations

from pathlib import Path

from benchmarks.concurrency.assets import load_evidence, load_probe

ROOT = Path(__file__).resolve().parents[2]
ARENA = ROOT / "cpp/src/internal/runtime/operation_task_arena.cc"
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
SELECTION = ROOT / "cpp/src/internal/runtime/operation_task_arena_selection.hh"


def test_runtime_selection_consumes_normalized_origins() -> None:
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


def test_overflow_fallback_preserves_exact_modulo_sequence() -> None:
    """Only size_t wraparound falls back to the exact modulo sequence."""
    selection = SELECTION.read_text(encoding="utf-8")
    probe = load_probe("scheduler/lane-origin-modulo-tsan.cc")
    compact = probe.replace(" ", "").replace("\n", "")

    assert "std::numeric_limits<std::size_t>::max() - delta" in selection
    assert "return (ticket + delta) % width;" in selection
    assert "width=1U;width<=32U;++width" in compact
    assert "std::numeric_limits<std::size_t>::max()" in probe
    assert "(ticket+alternative)%width" in compact
    assert "(ticket+1U)%width" in compact


def test_probe_covers_real_mixed_lane_arena() -> None:
    """The native/TSan probe crosses odd/even widths and all stage lanes."""
    source = load_probe("scheduler/lane-origin-modulo-tsan.cc")
    compact = source.replace(" ", "").replace("\n", "")

    assert "{2U,3U,4U,5U,8U,16U,32U}" in compact
    assert "TaskArenaLane::kUpstream" in source
    assert "TaskArenaLane::kOutput" in source
    assert "TaskArenaLane::kAll" in source
    assert "kProducerCount=3U" in compact
    assert "arena->submitted_tasks()==kTasks" in compact
    assert "arena->active_tasks()==0U" in compact
    assert "arena->queued_tasks()==0U" in compact


def test_evidence_covers_odd_and_power_of_two_widths() -> None:
    """Paired evidence remains positive across representative lane widths."""
    evidence = load_evidence("lane-origin-modulo")

    assert evidence["pair_count"] == 15
    assert evidence["iterations_per_process"] == 20_000_000
    assert "does not measure parsing" in evidence["scope"]
    scenarios = {int(item["lane_width"]): item for item in evidence["scenarios"]}
    assert set(scenarios) == {3, 5, 8, 16, 32}
    for item in scenarios.values():
        assert item["candidate_wins"] == 15
        assert item["paired_median_reduction_percent"] > 60.0
