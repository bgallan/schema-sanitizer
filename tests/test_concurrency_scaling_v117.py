"""Regression coverage for v117 wake-epoch cache-line isolation."""

from __future__ import annotations

import json
from pathlib import Path

from schema_sanitizer.core_impl.concurrency_coverage import (
    concurrency_pair_guarantees,
)

ROOT = Path(__file__).resolve().parents[1]
ARENA = ROOT / "cpp/src/internal/runtime/operation_task_arena.cc"
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
DOC = ROOT / "CONCURRENCY_SCALING_V117.md"
EVIDENCE = ROOT / "benchmarks/v117_wake_epoch_cacheline_ab.json"
PROBE = ROOT / "benchmarks/v117_wake_epoch_cacheline_tsan.cc"
STAGE = "cacheline_isolated_worker_wake_epoch_publication"


def test_v117_isolates_wake_epoch_on_both_sides() -> None:
    """The queue control block starts after the aligned wake publication line."""
    source = ARENA.read_text(encoding="utf-8")
    slot = source[source.index("struct WorkerSlot final") : source.index("explicit State")]

    wake = "alignas(64) std::atomic<std::uint64_t> wake_epoch{0};"
    queue = "alignas(64) std::deque<QueuedTask> tasks;"
    assert wake in slot
    assert queue in slot
    assert slot.index(wake) < slot.index(queue)
    assert "unused tail of the epoch line" in slot


def test_v117_preserves_wake_protocol_operations() -> None:
    """The layout change does not alter wake RMWs, loads, or notifications."""
    arena = ARENA.read_text(encoding="utf-8")
    runtime = RUNTIME.read_text(encoding="utf-8")

    assert arena.count("wake_epoch.fetch_add(1, std::memory_order_release)") >= 4
    assert "helper_slot.wake_epoch.fetch_add(1, std::memory_order_release)" in arena
    assert "slot.wake_epoch.load(std::memory_order_acquire)" in runtime
    assert "slot.ready.wait(lock, stop" in runtime
    assert "slot.ready.notify_one()" in arena
    assert "slot->ready.notify_all()" in arena


def test_v117_probe_repeats_real_park_wake_and_exact_drain() -> None:
    """The native/TSan probe stresses wake and queue ownership repeatedly."""
    source = PROBE.read_text(encoding="utf-8")
    compact = source.replace(" ", "").replace("\n", "")

    assert "kWaves=96U" in compact
    assert "{2U,4U,8U,16U,32U}" in compact
    assert "arena->wake_epoch_publishes()>=workers" in compact
    assert "arena->submitted_tasks()==workers*kWaves" in compact
    assert "arena->active_tasks()==0U" in compact
    assert "arena->queued_tasks()==0U" in compact


def test_v117_all_56_pairs_inherit_wake_epoch_isolation() -> None:
    """Every supported source/sink pair crosses the shared worker wake layout."""
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


def test_v117_evidence_is_positive_and_narrowly_scoped() -> None:
    """The benchmark shows cache ownership gains without throughput claims."""
    evidence = json.loads(EVIDENCE.read_text(encoding="utf-8"))
    text = DOC.read_text(encoding="utf-8")

    assert evidence["pair_count"] == 15
    assert evidence["iterations_per_thread"] == 5_000_000
    assert "does not measure parsing" in evidence["scope"]
    scenarios = {item["scenario"]: item for item in evidence["scenarios"]}
    assert set(scenarios) == {"wake_queue", "wake_queue_observer"}
    for item in scenarios.values():
        assert item["candidate_wins"] == 15
        assert item["paired_median_reduction_percent"] > 60.0
    assert "8 x 7 = 56" in text
    assert "pure-Python" in text


def test_v117_version_is_at_least_0370() -> None:
    """Later source packages retain the v117 minimum project version."""
    version = (ROOT / "meta/VERSION").read_text(encoding="utf-8").strip()
    assert tuple(map(int, version.split("."))) >= (0, 3, 70)
