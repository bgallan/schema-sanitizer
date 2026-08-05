"""Regression coverage for v113 high-core sharded queue visibility."""

from __future__ import annotations

import json
from pathlib import Path

from schema_sanitizer.core_impl.concurrency_coverage import (
    concurrency_pair_guarantees,
)

ROOT = Path(__file__).resolve().parents[2]
ARENA = ROOT / "cpp/src/internal/runtime/operation_task_arena.cc"
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
EVIDENCE = ROOT / "benchmarks/v113_sharded_queue_visibility_ab.json"
STAGE = "high_core_sharded_queue_visibility"


def test_v113_visibility_shards_are_bounded_and_low_core_stays_single() -> None:
    """Only arenas wider than eight activate multiple aligned shards."""
    source = ARENA.read_text(encoding="utf-8")

    assert "struct alignas(64) QueueVisibilityShard" in source
    assert "std::array<QueueVisibilityShard, 3> queue_visibility" in source
    assert "std::atomic<std::uint64_t> nonempty_mask{0}" in source
    assert "first high-core shard" in source
    assert "sole 1-8-worker line" in source.lower()


def test_v113_publication_and_snapshots_use_only_relevant_shards() -> None:
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


def test_v113_real_arena_probe_exercises_disjoint_visibility_domains() -> None:
    """The probe repeatedly publishes low and high lane transitions concurrently."""
    source = (ROOT / "benchmarks/v113_sharded_queue_visibility_tsan.cc").read_text(encoding="utf-8")

    compact = source.replace(" ", "").replace("\n", "")
    assert "for(constautoworkers:{9U,16U,32U})" in compact
    assert "TaskArenaLane::kUpstream" in source
    assert "TaskArenaLane::kOutput" in source
    assert "std::jthread low_producer" in source
    assert "std::jthread high_producer" in source
    assert "arena->active_tasks()==0U" in compact
    assert "arena->queued_tasks()==0U" in compact


def test_v113_all_56_pairs_inherit_sharded_visibility() -> None:
    """Every supported source/sink pair crosses the shared arena stage."""
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


def test_v113_evidence_is_positive_and_scoped_to_visibility_bookkeeping() -> None:
    """Evidence is paired, positive, and avoids end-to-end throughput claims."""
    evidence = json.loads(EVIDENCE.read_text(encoding="utf-8"))

    assert evidence["pair_count"] == 15
    assert "queue-visibility publication" in evidence["scope"]
    scenarios = {int(item["workers"]): item for item in evidence["scenarios"]}
    assert set(scenarios) == {12, 16, 32}
    for item in scenarios.values():
        assert item["candidate_wins"] == 15
        assert item["paired_median_reduction_percent"] > 40.0
