"""Regression coverage for v90 worker-sharded submission accounting."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.concurrency_coverage import (
    concurrency_pair_guarantees,
)
from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[1]
ARENA = ROOT / "cpp/src/internal/runtime/operation_task_arena.cc"


def test_v90_submitted_counter_is_worker_sharded() -> None:
    """Multi-worker admission no longer updates one operation-global RMW."""
    source = ARENA.read_text(encoding="utf-8")
    worker_slot = source[source.index("struct WorkerSlot") : source.index("explicit State")]
    state_fields = source[
        source.index("const std::size_t worker_count") : source.index("namespace {")
    ]

    assert "std::atomic<std::size_t> submitted{0};" in worker_slot
    assert "std::atomic<std::size_t> submitted{0};" not in state_fields
    assert "std::atomic<std::size_t> inline_submitted{0};" in state_fields
    assert "state_->submitted.fetch_add" not in source


def test_v90_shard_update_reuses_the_worker_queue_mutex() -> None:
    """The exact shard uses load/store only inside existing queue admission."""
    source = ARENA.read_text(encoding="utf-8")
    admission = source[
        source.index("auto &slot = *state_->slots[physical]") : source.index(
            "if (state_->telemetry)", source.index("auto &slot = *state_->slots[physical]")
        )
    ]

    assert "std::lock_guard lock(slot.mutex);" in admission
    assert "slot.tasks.push_back" in admission
    assert "++slot.submitted_local;" in admission
    assert "slot.submitted.store(slot.submitted_local" in admission
    assert "slot.submitted.load" not in admission
    assert "slot.submitted.fetch_add" not in admission


def test_v90_exact_total_is_aggregated_only_on_diagnostic_read() -> None:
    """Diagnostics sum a bounded number of worker publications on demand."""
    source = ARENA.read_text(encoding="utf-8")
    function = source[
        source.index("OperationTaskArena::submitted_tasks") : source.index(
            "OperationTaskArena::stolen_tasks"
        )
    ]

    assert "state_->inline_submitted.load(std::memory_order_relaxed)" in function
    assert "for (const auto &slot : state_->slots)" in function
    assert "slot->submitted.load(std::memory_order_relaxed)" in function
    assert "fetch_add" not in function


def test_v90_all_56_pairs_inherit_worker_sharded_submission_accounting() -> None:
    """Every supported input and output crosses the optimized shared arena."""
    pairs = concurrency_pair_guarantees()

    assert sum(len(outputs) for outputs in pairs.values()) == 56
    for input_name, outputs in pairs.items():
        assert len(outputs) == 7, input_name
        for guarantee in outputs.values():
            assert "worker_sharded_submission_accounting" in guarantee["shared_parallel_stages"]
            assert guarantee["source_to_sink_parallel_path"] is True
            assert guarantee["eligible_multi_benefit"] is True


def test_v90_native_stage_shards_preserve_exact_submitted_total() -> None:
    """Two concurrent ordered stages still report every admitted arena task."""
    require_native()
    for workers in (2, 4):
        result = native_core.operation_task_arena_probe(workers, workers, workers, 64)
        reported_workers, peak, total_threads, _overlap, _up, _out, submitted = result
        assert reported_workers == workers
        assert 1 <= peak <= workers
        assert 1 <= total_threads <= workers
        assert submitted == 128


def test_v90_native_concurrent_producers_preserve_exact_shards() -> None:
    """Concurrent coordinators cannot lose or duplicate shard increments."""
    require_native()
    for workers in (2, 4):
        elapsed, submitted, finished, queued, started, peak = (
            native_core.operation_task_arena_concurrent_submit_probe(workers, 2, 2_000)
        )
        assert elapsed > 0
        assert submitted == 4_000
        assert finished == 4_000
        assert queued == 0
        assert 1 <= started <= workers
        assert 1 <= peak <= workers
