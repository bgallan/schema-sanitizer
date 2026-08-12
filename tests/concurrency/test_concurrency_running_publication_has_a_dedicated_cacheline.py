"""Regression coverage for concurrency running publication has a dedicated cacheline."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ARENA = ROOT / "cpp/src/internal/runtime/operation_task_arena.cc"
EVIDENCE = ROOT / "benchmarks/evidence/concurrency/layout/running-publication-cacheline.json"


def test_running_publication_has_a_dedicated_cacheline() -> None:
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


def test_does_not_change_running_memory_orders_or_wake_logic() -> None:
    """The layout optimization does not weaken synchronization semantics."""
    runtime = (ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc").read_text(
        encoding="utf-8"
    )
    arena = ARENA.read_text(encoding="utf-8")

    assert "running.store(true, std::memory_order_release)" in runtime
    assert "running.store(false, std::memory_order_release)" in runtime
    assert "slot.running.load(std::memory_order_acquire)" in arena
    assert "const auto wake_target = !target_running;" in arena


def test_evidence_is_positive_and_scoped() -> None:
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


def test_real_arena_probe_covers_multiple_worker_counts() -> None:
    """The native probe validates exact completion and drain."""
    source = (
        ROOT / "benchmarks/probes/concurrency/layout/running-publication-cacheline-tsan.cc"
    ).read_text(encoding="utf-8")
    compact = source.replace(" ", "").replace("\n", "")

    assert "{2U,4U,8U,16U}" in compact
    assert "arena->submitted_tasks()==kTasks" in compact
    assert "arena->active_tasks()!=0U||arena->queued_tasks()!=0U" in compact
    assert "arena->peak_active_tasks()>0U" in compact
