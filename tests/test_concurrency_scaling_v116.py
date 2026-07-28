"""Regression coverage for v116 arena writer-domain cache-line isolation."""

from __future__ import annotations

import json
from pathlib import Path

from schema_sanitizer.core_impl.concurrency_coverage import (
    concurrency_pair_guarantees,
)

ROOT = Path(__file__).resolve().parents[1]
ARENA = ROOT / "cpp/src/internal/runtime/operation_task_arena.cc"
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
EVIDENCE = ROOT / "benchmarks/v116_arena_writer_domain_cacheline_ab.json"
PROBE = ROOT / "benchmarks/v116_arena_writer_domain_cacheline_tsan.cc"
STAGE = "cacheline_isolated_arena_writer_domains"


def test_v116_isolates_independent_arena_writer_domains() -> None:
    """Each independent producer cursor and activity domain starts aligned."""
    source = ARENA.read_text(encoding="utf-8")
    state = source[source.index("struct OperationTaskArena::State") : source.index("namespace {")]

    assert "alignas(64) std::atomic<std::size_t> upstream_cursor" in state
    assert "alignas(64) std::atomic<std::size_t> output_cursor" in state
    assert "alignas(64) std::atomic<std::size_t> all_cursor" in state
    assert "alignas(64) std::atomic<bool> stopping" in state
    assert "alignas(64) std::atomic<std::size_t> active" in state
    assert state.index("upstream_cursor") < state.index("output_cursor")
    assert state.index("output_cursor") < state.index("all_cursor")
    assert state.index("all_cursor") < state.index("stopping")
    assert state.index("initialized_mask") < state.index("active")


def test_v116_preserves_cursor_and_activity_atomic_semantics() -> None:
    """The optimization changes layout only, not scheduling synchronization."""
    arena = ARENA.read_text(encoding="utf-8")
    runtime = RUNTIME.read_text(encoding="utf-8")

    assert "plan.cursor = &state_->all_cursor;" in arena
    assert "plan.cursor = &state_->upstream_cursor;" in arena
    assert "plan.cursor = &state_->output_cursor;" in arena
    assert "plan.cursor->fetch_add(1, std::memory_order_relaxed)" in arena
    assert runtime.count("state_->active.fetch_add") == 1
    assert runtime.count("state_->active.fetch_sub") == 1
    assert "update_peak(&state_->peak_active, active)" in runtime


def test_v116_probe_stresses_all_cursor_domains_and_exact_drain() -> None:
    """The native/TSan probe submits concurrently through all lane cursors."""
    source = PROBE.read_text(encoding="utf-8")
    compact = source.replace(" ", "").replace("\n", "")

    assert "std::barrier" in source
    assert "TaskArenaLane::kUpstream" in source
    assert "TaskArenaLane::kOutput" in source
    assert "TaskArenaLane::kAll" in source
    assert "{2U,4U,8U,16U,32U}" in compact
    assert "kProducerCount=3U" in compact
    assert "arena->submitted_tasks()==kTasks" in compact
    assert "arena->active_tasks()==0U" in compact
    assert "arena->queued_tasks()==0U" in compact


def test_v116_all_56_pairs_inherit_arena_writer_isolation() -> None:
    """Every supported source/sink pair crosses the shared arena layout."""
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


def test_v116_evidence_is_positive_and_narrowly_scoped() -> None:
    """The evidence covers realistic independent writers without overclaiming."""
    evidence = json.loads(EVIDENCE.read_text(encoding="utf-8"))

    assert evidence["pair_count"] == 15
    assert "does not measure parsing" in evidence["scope"]
    scenarios = {item["scenario"]: item for item in evidence["scenarios"]}
    assert set(scenarios) == {
        "cursor_activity",
        "two_cursors_activity",
        "three_cursors_two_activity",
    }
    assert {item["writer_threads"] for item in scenarios.values()} == {2, 3, 5}
    for item in scenarios.values():
        assert item["candidate_wins"] == 15
        assert item["paired_median_reduction_percent"] > 45.0


def test_v116_version_is_at_least_0369() -> None:
    """Later source packages retain the v116 minimum project version."""
    version = (ROOT / "meta/VERSION").read_text(encoding="utf-8").strip()
    assert tuple(map(int, version.split("."))) >= (0, 3, 69)
