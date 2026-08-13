"""Regression coverage for concurrency worker slots keep mutex owned exact counters."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[2]
ARENA = ROOT / "cpp/src/internal/runtime/operation_task_arena.cc"
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"


def test_worker_slots_keep_mutex_owned_exact_counters() -> None:
    """Verify the named concurrency regression contract."""
    source = ARENA.read_text(encoding="utf-8")
    slot = source[source.index("struct WorkerSlot") : source.index("explicit State")]

    assert "std::size_t queued_local = 0;" in slot
    assert "std::size_t submitted_local = 0;" in slot
    assert "std::atomic<std::size_t> queued{0};" in slot
    assert "std::atomic<std::size_t> submitted{0};" in slot
    assert "Exact mutex-owned counters" in slot


def test_local_and_stolen_dequeue_avoid_queue_depth_rmw() -> None:
    """Verify the named concurrency regression contract."""
    runtime = RUNTIME.read_text(encoding="utf-8")

    assert "--slot.queued_local;" in runtime
    assert "slot.queued.store(slot.queued_local" in runtime
    assert "--candidate.queued_local;" in runtime
    assert "candidate.queued.store(candidate.queued_local" in runtime
    assert ".queued.fetch_sub" not in runtime


def test_concurrent_direct_producers_preserve_exact_snapshots() -> None:
    """Verify the named concurrency regression contract."""
    require_native()
    for workers in (2, 4, 8, 16):
        _elapsed, submitted, finished, queued, started, peak = (
            native_core.operation_task_arena_concurrent_submit_probe(workers, 2, 2_000)
        )
        assert submitted == 4_000
        assert finished == 4_000
        assert queued == 0
        assert 1 <= started <= workers
        assert 1 <= peak <= workers
