"""Regression coverage for v114 running-publication cache-line isolation."""

from __future__ import annotations

import json
from pathlib import Path

from schema_sanitizer.core_impl.concurrency_coverage import (
    concurrency_pair_guarantees,
)

ROOT = Path(__file__).resolve().parents[1]
ARENA = ROOT / "cpp/src/internal/runtime/operation_task_arena.cc"
EVIDENCE = ROOT / "benchmarks/v114_running_publication_cacheline_ab.json"
STAGE = "cacheline_isolated_worker_running_publication"


def test_v114_running_publication_has_a_dedicated_cacheline() -> None:
    """Worker activity publication is separated from queue accounting."""
    source = ARENA.read_text(encoding="utf-8")
    fragment = source.split("std::size_t stolen_local", 1)[1].split(
        "std::atomic<bool> first_task_pending", 1
    )[0]

    assert "std::atomic<std::size_t> stolen" in fragment
    assert "alignas(64) std::atomic<bool> running" in fragment
    assert "independently contended publication off the queue snapshot" in " ".join(
        fragment.split()
    )


def test_v114_does_not_change_running_memory_orders_or_wake_logic() -> None:
    """The layout optimization does not weaken synchronization semantics."""
    runtime = (ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc").read_text(
        encoding="utf-8"
    )
    arena = ARENA.read_text(encoding="utf-8")

    assert "running.store(true, std::memory_order_release)" in runtime
    assert "running.store(false, std::memory_order_release)" in runtime
    assert "slot.running.load(std::memory_order_acquire)" in arena
    assert "const auto wake_target = !target_running;" in arena


def test_v114_all_56_pairs_inherit_running_publication_isolation() -> None:
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


def test_v114_evidence_is_positive_and_scoped() -> None:
    """The paired benchmark is positive and avoids throughput claims."""
    evidence = json.loads(EVIDENCE.read_text(encoding="utf-8"))

    assert evidence["pair_count"] == 15
    assert "cache-line ownership" in evidence["scope"]
    assert "does not measure parsing" in evidence["scope"]
    scenarios = {int(item["worker_pairs"]): item for item in evidence["scenarios"]}
    assert set(scenarios) == {2, 4, 8}
    for item in scenarios.values():
        assert item["candidate_wins"] >= 14
        assert item["paired_median_reduction_percent"] > 20.0


def test_v114_real_arena_probe_covers_multiple_worker_counts() -> None:
    """The native probe validates exact completion and drain."""
    source = (ROOT / "benchmarks/v114_running_publication_cacheline_tsan.cc").read_text(
        encoding="utf-8"
    )
    compact = source.replace(" ", "").replace("\n", "")

    assert "{2U,4U,8U,16U}" in compact
    assert "arena->submitted_tasks()==kTasks" in compact
    assert "arena->active_tasks()!=0U||arena->queued_tasks()!=0U" in compact
    assert "arena->peak_active_tasks()>0U" in compact
