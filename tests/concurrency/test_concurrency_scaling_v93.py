"""Regression coverage for v93 mutex-owned queue accounting."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.concurrency_coverage import (
    concurrency_pair_guarantees,
)
from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[2]
ARENA = ROOT / "cpp/src/internal/runtime/operation_task_arena.cc"
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
STAGE = "mutex_owned_queue_counters_single_store_publication"


def test_v93_worker_slots_keep_mutex_owned_exact_counters() -> None:
    """Verify the named concurrency regression contract."""
    source = ARENA.read_text(encoding="utf-8")
    slot = source[source.index("struct WorkerSlot") : source.index("explicit State")]

    assert "std::size_t queued_local = 0;" in slot
    assert "std::size_t submitted_local = 0;" in slot
    assert "std::atomic<std::size_t> queued{0};" in slot
    assert "std::atomic<std::size_t> submitted{0};" in slot
    assert "Exact mutex-owned counters" in slot


def test_v93_local_and_stolen_dequeue_avoid_queue_depth_rmw() -> None:
    """Verify the named concurrency regression contract."""
    runtime = RUNTIME.read_text(encoding="utf-8")

    assert "--slot.queued_local;" in runtime
    assert "slot.queued.store(slot.queued_local" in runtime
    assert "--candidate.queued_local;" in runtime
    assert "candidate.queued.store(candidate.queued_local" in runtime
    assert ".queued.fetch_sub" not in runtime


def test_v93_all_56_pairs_inherit_single_store_queue_accounting() -> None:
    """Verify the named concurrency regression contract."""
    pairs = concurrency_pair_guarantees()

    assert sum(len(outputs) for outputs in pairs.values()) == 56
    for input_name, outputs in pairs.items():
        assert len(outputs) == 7, input_name
        for guarantee in outputs.values():
            assert STAGE in guarantee["shared_parallel_stages"]
            assert guarantee["source_to_sink_parallel_path"] is True
            assert guarantee["eligible_multi_benefit"] is True


def test_v93_native_ordered_executor_preserves_exact_drain() -> None:
    """Verify the named concurrency regression contract."""
    require_native()
    for workers in (2, 4, 5, 8, 16):
        elapsed, completed, checksum, started, peak, queued, submitted = (
            native_core.ordered_executor_arena_completion_probe(workers, 25_000, 1)
        )
        assert elapsed > 0
        assert completed == 25_000
        assert checksum >= 0
        assert 1 <= started <= workers
        assert 1 <= peak <= workers
        assert queued == 0
        assert submitted == 25_000


def test_v93_concurrent_direct_producers_preserve_exact_snapshots() -> None:
    """Verify the named concurrency regression contract."""
    require_native()
    for workers in (2, 4, 8, 16):
        elapsed, submitted, finished, queued, started, peak = (
            native_core.operation_task_arena_concurrent_submit_probe(workers, 2, 2_000)
        )
        assert elapsed > 0
        assert submitted == 4_000
        assert finished == 4_000
        assert queued == 0
        assert 1 <= started <= workers
        assert 1 <= peak <= workers
