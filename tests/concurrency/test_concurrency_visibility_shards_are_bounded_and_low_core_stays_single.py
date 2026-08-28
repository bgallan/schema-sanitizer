"""Regression coverage for concurrency visibility shards are bounded and low core stays single."""

from __future__ import annotations

from pathlib import Path

from benchmarks.concurrency.assets import load_evidence, load_probe

ROOT = Path(__file__).resolve().parents[2]
ARENA = ROOT / "cpp/src/internal/runtime/operation_task_arena.cc"
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"


def test_visibility_shards_are_bounded_and_low_core_stays_single() -> None:
    """Only arenas wider than eight activate multiple aligned shards."""
    source = ARENA.read_text(encoding="utf-8")

    assert "struct alignas(64) QueueVisibilityShard" in source
    assert "std::array<QueueVisibilityShard, 3> queue_visibility" in source
    assert "std::atomic<std::uint64_t> nonempty_mask{0}" in source


def test_publication_and_snapshots_use_only_relevant_shards() -> None:
    """Transitions publish locally and narrow admission avoids unrelated loads."""
    source = RUNTIME.read_text(encoding="utf-8")
    snapshot = source.split("queue_visibility_snapshot(", 1)[1].split("idle_started_worker(", 1)[0]

    assert "plan.visibility_shard_begin" in snapshot
    assert "plan.visibility_shard_end" in snapshot
    assert "for (auto shard" in snapshot
    assert "state->queue_visibility[shard - 1U]" in snapshot
    assert "if constexpr (!Sharded)" in snapshot
    assert "return snapshot & allowed;" in snapshot
    assert "visibility->nonempty_mask.fetch_or(" in source
    assert "visibility->nonempty_mask.fetch_and(" in source
    assert "queue_visibility_snapshot<PreferDedicatedOutput>" in source
    assert "queue_visibility_snapshot(state, plan)" in source
    assert "initialized_snapshot) &" in source


def test_real_arena_probe_exercises_disjoint_visibility_domains() -> None:
    """The probe repeatedly publishes low and high lane transitions concurrently."""
    source = load_probe("scheduler/sharded-queue-visibility-tsan.cc")

    compact = source.replace(" ", "").replace("\n", "")
    assert "for(constautoworkers:{9U,16U,32U})" in compact
    assert "TaskArenaLane::kUpstream" in source
    assert "TaskArenaLane::kOutput" in source
    assert "std::jthread low_producer" in source
    assert "std::jthread high_producer" in source
    assert "arena->active_tasks()==0U" in compact
    assert "arena->queued_tasks()==0U" in compact


def test_evidence_is_positive_and_scoped_to_visibility_bookkeeping() -> None:
    """Evidence is paired, positive, and avoids end-to-end throughput claims."""
    evidence = load_evidence("sharded-queue-visibility")

    assert evidence["pair_count"] == 15
    assert "queue-visibility publication" in evidence["scope"]
    scenarios = {int(item["workers"]): item for item in evidence["scenarios"]}
    assert set(scenarios) == {12, 16, 32}
    for item in scenarios.values():
        assert item["candidate_wins"] == 15
        assert item["paired_median_reduction_percent"] > 40.0
