"""Regression coverage for v119 fixed physical visibility snapshots."""

from __future__ import annotations

import json
from pathlib import Path

from schema_sanitizer.core_impl.concurrency_coverage import (
    concurrency_pair_guarantees,
)

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
EVIDENCE = ROOT / "benchmarks/v119_fixed_visibility_snapshot_ab.json"
PROBE = ROOT / "benchmarks/v119_fixed_visibility_snapshot_tsan.cc"
STAGE = "fixed_physical_queue_visibility_snapshot"


def test_v119_high_core_snapshot_uses_fixed_physical_domains() -> None:
    """High-core stealing no longer rediscovers shard geometry per attempt."""
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


def test_v119_low_core_path_remains_one_visibility_load() -> None:
    """Arenas with at most eight workers retain the existing specialization."""
    source = RUNTIME.read_text(encoding="utf-8")
    block = source.split("template <bool Sharded>", 1)[1].split("idle_started_worker(", 1)[0]
    low_core = block.split("if constexpr (!Sharded)", 1)[1].split("// High-core workers", 1)[0]

    assert low_core.count("primary_queue_visibility.nonempty_mask.load") == 1
    assert "queue_visibility[" not in low_core
    assert "allowed" in low_core


def test_v119_forced_steal_probe_covers_all_shard_boundaries() -> None:
    """The real arena probe forces stealing at every physical shard boundary."""
    source = PROBE.read_text(encoding="utf-8")
    compact = source.replace(" ", "").replace("\n", "")

    assert "{9U,16U,17U,24U,25U,32U}" in compact
    assert "TaskArenaLane::kAll" in source
    assert "kQuickTasks=4096U" in compact
    assert "plan,0U" in compact
    assert "arena->stolen_tasks()>0U" in compact
    assert "arena->active_tasks()==0U" in compact
    assert "arena->queued_tasks()==0U" in compact


def test_v119_all_56_pairs_inherit_fixed_visibility_snapshots() -> None:
    """Every supported source/sink pair crosses the common stealing path."""
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


def test_v119_evidence_is_positive_and_correctly_scoped() -> None:
    """Paired evidence covers all 2/3/4-shard boundaries without broad claims."""
    evidence = json.loads(EVIDENCE.read_text(encoding="utf-8"))

    assert evidence["pair_count"] == 15
    assert evidence["iterations_per_process"] == 20_000_000
    assert "does not measure parsing" in evidence["scope"]
    scenarios = {int(item["workers"]): item for item in evidence["scenarios"]}
    assert set(scenarios) == {9, 16, 17, 24, 25, 32}
    for item in scenarios.values():
        assert item["candidate_wins"] == 15
        assert item["paired_median_reduction_percent"] > 40.0


def test_v119_version_is_at_least_0372() -> None:
    """Later source packages retain the v119 version floor."""
    version = (ROOT / "meta/VERSION").read_text(encoding="utf-8").strip()
    assert tuple(map(int, version.split("."))) >= (0, 3, 72)
