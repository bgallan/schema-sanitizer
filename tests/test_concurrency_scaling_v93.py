"""Regression coverage for v93 mutex-owned queue accounting."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.concurrency_coverage import (
    concurrency_pair_guarantees,
)
from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[1]
ARENA = ROOT / "cpp/src/internal/runtime/operation_task_arena.cc"
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
DOC = ROOT / "CONCURRENCY_SCALING_V93.md"
STAGE = "mutex_owned_queue_counters_single_store_publication"


def test_v93_worker_slots_keep_mutex_owned_exact_counters() -> None:
    source = ARENA.read_text(encoding="utf-8")
    slot = source[source.index("struct WorkerSlot") : source.index("explicit State")]

    assert "std::size_t queued_local = 0;" in slot
    assert "std::size_t submitted_local = 0;" in slot
    assert "std::atomic<std::size_t> queued{0};" in slot
    assert "std::atomic<std::size_t> submitted{0};" in slot
    assert "Exact mutex-owned counters" in slot


def test_v93_admission_publishes_each_counter_with_one_atomic_store() -> None:
    source = ARENA.read_text(encoding="utf-8")
    begin = source.index("auto &slot = *state_->slots[physical]")
    end = source.index("if (state_->telemetry)", begin)
    admission = source[begin:end]

    assert "std::lock_guard lock(slot.mutex);" in admission
    assert "queued_before = slot.queued_local;" in admission
    assert "++slot.queued_local;" in admission
    assert "slot.queued.store(slot.queued_local" in admission
    assert "++slot.submitted_local;" in admission
    assert "slot.submitted.store(slot.submitted_local" in admission
    assert "slot.queued.fetch_add" not in admission
    assert "slot.submitted.load" not in admission
    assert "slot.submitted.fetch_add" not in admission


def test_v93_local_and_stolen_dequeue_avoid_queue_depth_rmw() -> None:
    runtime = RUNTIME.read_text(encoding="utf-8")

    assert "--slot.queued_local;" in runtime
    assert "slot.queued.store(slot.queued_local" in runtime
    assert "--candidate.queued_local;" in runtime
    assert "candidate.queued.store(candidate.queued_local" in runtime
    assert ".queued.fetch_sub" not in runtime


def test_v93_shutdown_resets_private_and_published_depth() -> None:
    source = ARENA.read_text(encoding="utf-8")
    shutdown = source[source.index("void OperationTaskArena::Shutdown") :]

    clear = shutdown.index("slot->tasks.clear();")
    local = shutdown.index("slot->queued_local = 0U;", clear)
    published = shutdown.index("slot->queued.store(0, std::memory_order_relaxed);", local)
    assert clear < local < published


def test_v93_diagnostics_remain_lock_free_and_exact() -> None:
    source = ARENA.read_text(encoding="utf-8")
    submitted = source[
        source.index("OperationTaskArena::submitted_tasks") : source.index(
            "OperationTaskArena::stolen_tasks"
        )
    ]
    queued = source[
        source.index("OperationTaskArena::queued_tasks") : source.index(
            "OperationTaskArena::started_workers"
        )
    ]

    assert "slot->submitted.load(std::memory_order_relaxed)" in submitted
    assert "slot->queued.load(std::memory_order_relaxed)" in queued
    assert "lock_guard" not in submitted
    assert "lock_guard" not in queued


def test_v93_all_56_pairs_inherit_single_store_queue_accounting() -> None:
    pairs = concurrency_pair_guarantees()

    assert sum(len(outputs) for outputs in pairs.values()) == 56
    for input_name, outputs in pairs.items():
        assert len(outputs) == 7, input_name
        for guarantee in outputs.values():
            assert STAGE in guarantee["shared_parallel_stages"]
            assert guarantee["source_to_sink_parallel_path"] is True
            assert guarantee["eligible_multi_benefit"] is True


def test_v93_native_ordered_executor_preserves_exact_drain() -> None:
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


def test_v93_documentation_records_full_matrix_and_guardrails() -> None:
    text = DOC.read_text(encoding="utf-8")

    assert "8 x 7" in text
    assert "pure-Python" in text
    assert "one atomic store" in text
    assert "lock-free diagnostics" in text
    assert "memory remains bounded" in text
