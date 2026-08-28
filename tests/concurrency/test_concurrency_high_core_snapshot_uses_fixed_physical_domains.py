"""Regression coverage for concurrency high core snapshot uses fixed physical domains."""

from __future__ import annotations

from pathlib import Path

from benchmarks.concurrency.assets import load_evidence, load_probe

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"


def test_high_core_snapshot_uses_fixed_physical_domains() -> None:
    """High-core stealing reuses fixed shard geometry."""
    source = RUNTIME.read_text(encoding="utf-8")
    block = source.split("template <bool Sharded>", 1)[1].split("idle_started_worker(", 1)[0]

    assert "if constexpr (!Sharded)" in block
    assert "state->worker_count > 8U" in block
    assert "state->worker_count > 16U" in block
    assert "state->worker_count > 24U" in block
    assert "queue_visibility[0].nonempty_mask.load" in block
    assert "queue_visibility[1].nonempty_mask.load" in block
    assert "queue_visibility[2].nonempty_mask.load" in block
    assert "std::countr_zero" not in block
    assert "while (remaining" not in block
    assert "return snapshot & allowed;" in block


def test_low_core_path_remains_one_visibility_load() -> None:
    """Arenas with at most eight workers retain the existing specialization."""
    source = RUNTIME.read_text(encoding="utf-8")
    block = source.split("template <bool Sharded>", 1)[1].split("idle_started_worker(", 1)[0]
    low_core = block.split("if constexpr (!Sharded)", 1)[1].split("// High-core workers", 1)[0]

    assert low_core.count("primary_queue_visibility.nonempty_mask.load") == 1
    assert "queue_visibility[" not in low_core
    assert "allowed" in low_core


def test_forced_steal_probe_covers_all_shard_boundaries() -> None:
    """The real arena probe forces stealing at every physical shard boundary."""
    source = load_probe("scheduler/fixed-visibility-snapshot-tsan.cc")
    compact = source.replace(" ", "").replace("\n", "")

    assert "{9U,16U,17U,24U,25U,32U}" in compact
    assert "TaskArenaLane::kAll" in source
    assert "kQuickTasks=4096U" in compact
    assert "plan,0U" in compact
    assert "arena->stolen_tasks()>0U" in compact
    assert "arena->active_tasks()==0U" in compact
    assert "arena->queued_tasks()==0U" in compact


def test_evidence_is_positive_and_correctly_scoped() -> None:
    """Paired evidence covers all 2/3/4-shard boundaries without broad claims."""
    evidence = load_evidence("fixed-visibility-snapshot")

    assert evidence["pair_count"] == 15
    assert evidence["iterations_per_process"] == 20_000_000
    assert "does not measure parsing" in evidence["scope"]
    scenarios = {int(item["workers"]): item for item in evidence["scenarios"]}
    assert set(scenarios) == {9, 16, 17, 24, 25, 32}
    for item in scenarios.values():
        assert item["candidate_wins"] == 15
        assert item["paired_median_reduction_percent"] > 40.0
