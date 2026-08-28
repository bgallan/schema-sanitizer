"""Certify exact sharded telemetry publication by the shared operation arena.

Completion, active-streak, submission, task, and native-stage counters must remain cache-separated
and single-writer where promised, use stores instead of contested updates, and aggregate exact
JSON totals under real concurrent producers.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from _support.source_contracts import source_text

import schema_sanitizer as ss
from benchmarks.concurrency.assets import load_evidence, load_probe
from schema_sanitizer.api_impl.execution_context import default_pool
from schema_sanitizer.core_impl.native_runtime import native_core

RUNTIME = "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
ARENA_TELEMETRY = "cpp/src/internal/runtime/operation_task_telemetry.cc.inc"
HEADER = "cpp/src/internal/runtime/performance_telemetry.hh"
SOURCE = "cpp/src/internal/runtime/performance_telemetry.cc"
JSON_SOURCE = "cpp/src/internal/runtime/performance_telemetry_json.cc.inc"


def test_every_multiworker_range_uses_worker_completion_shards() -> None:
    """Every valid worker count routes completion batches to one shard."""
    source = source_text(ARENA_TELEMETRY)
    assert "telemetry_->RecordWorkerTaskBatch(" in source
    assert "worker_count > 8U ? 32U : 8U" in source
    assert "direct_global_" not in source
    record = source.split("void Record(", 1)[1].split("void Flush()", 1)[0]
    assert "RecordTaskStarted" not in record
    assert "RecordTaskFinished" not in record
    assert "fetch_add" not in record
    assert "compare_exchange" not in record
    flush = source.split("void Flush()", 1)[1]
    assert "telemetry_->RecordTaskBatch(" not in flush


def test_completion_shard_probe_covers_mid_and_high_core_arenas() -> None:
    """The TSan probe validates 5-, 8-, and 16-worker drains."""
    source = load_probe("telemetry/completion-shards-tsan.cc")
    assert "OperationTaskArena::Make(workers, telemetry)" in source
    assert "for (const auto workers : {5U, 8U, 16U})" in source
    assert "arena->active_tasks() != 0U" in source
    assert "arena->queued_tasks() != 0U" in source
    assert r",\"started\":" in source and r",\"finished\":" in source


def test_completion_shard_evidence_covers_worker_boundaries() -> None:
    """Completion-shard evidence covers both widened gates."""
    evidence = load_evidence("completion-shards")
    scenarios = {int(item["workers"]): item for item in evidence["scenarios"]}
    assert set(scenarios) == {5, 8, 16}
    for item in scenarios.values():
        assert item["candidate_wins"] == evidence["pair_count"] == 15
        assert item["paired_median_reduction_percent"] > 85.0


def test_worker_streak_shard_publishes_exact_cumulative_snapshot() -> None:
    """Each worker owns a plain streak total and publishes one store."""
    header = source_text(HEADER)
    method = source_text(SOURCE).split("RecordWorkerActiveStreak", 1)[1]
    method = method.split("RecordWorkerStarted", 1)[0]
    assert "std::uint64_t active_streaks_local = 0;" in header
    assert "std::atomic<std::int64_t> active_streaks{0};" in header
    assert "++shard.active_streaks_local;" in method
    assert "shard.active_streaks.store" in method
    assert "std::memory_order_relaxed" in method
    assert "fetch_add" not in method


def test_json_aggregates_global_and_worker_streak_totals() -> None:
    """The public streak counter remains exact."""
    source = source_text(JSON_SOURCE)
    aggregate = source.split("worker_active_streak_count", 1)[1]
    aggregate = aggregate.split("const auto stream_ns", 1)[0]
    assert "PerformanceCounter::kWorkerActiveStreaks" in aggregate
    assert "shard.active_streaks.load" in aggregate
    assert "worker_active_streak_count()" in source


def test_active_streak_probe_exercises_exact_real_arena_streaks() -> None:
    """The real arena probe checks concurrent streak shards and JSON totals."""
    source = load_probe("telemetry/active-streak-shards-tsan.cc")
    assert "OperationTaskArena::Make(workers, telemetry)" in source
    assert "for (const auto workers : {2U, 4U, 8U, 16U})" in source
    assert "constexpr std::size_t kWaves = 64U" in source
    assert "worker_active_streaks" in source
    assert "arena->active_tasks() == workers" in source
    assert "arena->queued_tasks() == 0U" in source


def test_active_streak_evidence_is_scoped_and_positive() -> None:
    """Streak evidence reports isolated publication only."""
    evidence = load_evidence("active-streak-shards")
    assert evidence["pair_count"] == 15
    assert "active-streak telemetry publication" in evidence["scope"]
    scenarios = {int(item["workers"]): item for item in evidence["scenarios"]}
    assert set(scenarios) == {2, 4, 8, 16}
    for item in scenarios.values():
        assert item["candidate_wins"] == 15
        assert item["paired_median_reduction_percent"] > 80.0


def test_submission_shards_are_separate_and_cache_aligned() -> None:
    """Producer snapshots do not share worker completion shards."""
    header = source_text(HEADER)
    submission = header.split("struct alignas(64) WorkerSubmissionTelemetryShard", 1)[1]
    submission = submission.split("struct alignas(64) WorkerTaskTelemetryShard", 1)[0]
    assert "submitted_local" in submission
    assert "peak_queue_depth_local" in submission
    assert "worker_submission_shards_" in submission
    assert "WorkerTaskTelemetryShard" not in submission
    assert "kWorkerSubmissionShardCount = 32" in submission


def test_sharded_submission_publication_uses_stores_not_rmw() -> None:
    """Queue-mutex publication performs no RMW or CAS loop."""
    function = source_text(SOURCE).split("void PerformanceTelemetry::RecordWorkerTaskSubmitted", 1)[
        1
    ]
    function = function.split("void PerformanceTelemetry::RecordTaskStarted", 1)[0]
    assert "submitted_local" in function
    assert "peak_queue_depth_local" in function
    assert function.count(".store(") >= 2
    for forbidden in (
        "fetch_add",
        "compare_exchange",
        "update_maximum",
        "task_submitted_",
        "&peak_queue_depth_",
    ):
        assert forbidden not in function


def test_serialization_aggregates_submission_shards() -> None:
    """Task totals, peak depth, and diagnosis include every shard."""
    source = source_text(JSON_SOURCE)
    for fragment in (
        "task_submitted_sum",
        "for (const auto &shard : worker_submission_shards_)",
        "shard.submitted[kind_index].load",
        "shard.peak_queue_depth.load",
        "const auto submitted = task_submitted_sum(index);",
        "materialization_tasks = task_submitted_sum",
        "validation_tasks = task_submitted_sum",
        '"peak_queue_depth",\n                   peak_queue_depth()',
    ):
        assert fragment in source


def test_worker_task_shards_remain_the_common_publication_owner() -> None:
    """Worker task shards remain authoritative for every worker range."""
    runtime = source_text(RUNTIME) + source_text(ARENA_TELEMETRY)
    header = source_text(HEADER)
    source = source_text(SOURCE) + source_text(JSON_SOURCE)
    assert "alignas(64) WorkerTaskTelemetryShard" in header
    assert "kWorkerTaskShardCount = 32" in header
    assert "RecordWorkerTaskBatch" in header
    assert "telemetry_->RecordWorkerTaskBatch(" in runtime
    assert "direct_global_" not in runtime
    assert "worker_count > 8U ? 32U : 8U" in runtime
    assert "telemetry_->RecordTaskBatch(" not in runtime
    assert "low_core_task_telemetry_batches" in source
    assert "getenv" not in runtime
    assert "std::thread" not in runtime


def test_telemetry_json_aggregates_global_and_worker_task_shards() -> None:
    """Stats retain exact task totals and maxima."""
    source = source_text(SOURCE) + source_text(JSON_SOURCE)
    for fragment in (
        "const auto task_sum",
        "const auto task_maximum",
        "for (const auto &shard : worker_task_shards_)",
        "WorkerTaskTelemetryShard::started",
        "WorkerTaskTelemetryShard::finished",
        "WorkerTaskTelemetryShard::queue_wait_ns",
        "WorkerTaskTelemetryShard::run_ns",
        "WorkerTaskTelemetryShard::max_queue_wait_ns",
        "WorkerTaskTelemetryShard::max_run_ns",
    ):
        assert fragment in source


def test_worker_shards_keep_private_single_writer_totals() -> None:
    """Low-core shards own plain cumulative values beside snapshots."""
    shard = source_text(HEADER).split("struct alignas(64) WorkerTaskTelemetryShard", 1)[1]
    shard = shard.split("static constexpr std::size_t kWorkerTaskShardCount", 1)[0]
    for fragment in (
        "completed_local",
        "queue_wait_ns_local",
        "run_ns_local",
        "max_queue_wait_ns_local",
        "max_run_ns_local",
        "batches_local",
        "std::array<std::uint64_t",
    ):
        assert fragment in shard


def test_worker_batch_publication_uses_stores_not_rmw() -> None:
    """The sole writer publishes task snapshots without RMWs."""
    function = source_text(SOURCE).split("void PerformanceTelemetry::RecordWorkerTaskBatch", 1)[1]
    function = function.split("void PerformanceTelemetry::RecordTaskStolen", 1)[0]
    assert "signed_snapshot" in function
    assert function.count(".store(") >= 5
    for forbidden in (
        "fetch_add",
        "compare_exchange",
        "update_maximum",
        "task_started_",
        "task_finished_",
    ):
        assert forbidden not in function


def test_signed_snapshot_preserves_atomic_add_bit_pattern() -> None:
    """Unsigned locals preserve the published signed bit pattern."""
    source = source_text(SOURCE)
    assert "#include <bit>" in source
    assert "std::int64_t signed_snapshot(std::uint64_t value)" in source
    assert "return std::bit_cast<std::int64_t>(value);" in source


def test_public_four_worker_stats_cover_every_shard(tmp_path: Path, require_native: None) -> None:
    """One real conversion validates submission, task, and worker snapshots."""
    if not hasattr(os, "sched_getaffinity") or not hasattr(os, "sched_setaffinity"):
        pytest.skip("CPU affinity is required for the four-worker contract")
    original_affinity = os.sched_getaffinity(0)
    if len(original_affinity) < 4:
        pytest.skip("at least four available CPUs are required")
    selected = set(sorted(original_affinity)[:4])
    source = tmp_path / "rows.jsonl"
    output = tmp_path / "rows.csv"
    with source.open("w", encoding="utf-8") as handle:
        for row in range(12_000):
            handle.write(
                json.dumps(
                    {f"field_{column:02d}": row * 32 + column for column in range(32)},
                    separators=(",", ":"),
                )
                + "\n"
            )
    try:
        os.sched_setaffinity(0, selected)
        ss.to_csv(
            source,
            output,
            input_format="jsonl",
            multi_threading=True,
            memory_limit_bytes=64 << 20,
            parse_integers=True,
            field_name_policy="preserve",
        )
        stats = default_pool().get().performance_stats()
    finally:
        os.sched_setaffinity(0, original_affinity)
    tasks = stats["tasks"]
    submitted = sum(int(values["submitted"]) for values in tasks.values())
    started = sum(int(values["started"]) for values in tasks.values())
    finished = sum(int(values["finished"]) for values in tasks.values())
    batches = int(stats["counters"]["low_core_task_telemetry_batches"])
    assert submitted == started == finished > 0
    assert 0 < batches <= finished
    assert int(stats["counters"]["peak_active_tasks"]) >= 2
    assert int(stats["counters"]["peak_queue_depth"]) > 0
    assert output.exists() and output.stat().st_size > 0


def test_submission_and_worker_publication_evidence_is_scoped() -> None:
    """Both producer and worker evidence records remain conservative."""
    submission = load_evidence("submission-shards")
    for key in ("single_producer", "four_producers"):
        result = submission[key]
        assert result["candidate_wins"] == result["pair_count"] == 15
        assert result["paired_median_reduction_percent"] > 30.0
    assert "not an end-to-end throughput claim" in submission["scope"]

    worker = load_evidence("worker-single-store-publication")
    assert worker["candidate_wins"] == worker["pair_count"] == 15
    assert worker["paired_median_reduction_percent"] > 70.0
    assert "not an end-to-end throughput claim" in worker["scope"]


@pytest.mark.parametrize("workers", (2, 4))
def test_native_stage_shards_preserve_exact_submitted_total(
    workers: int, require_native: None
) -> None:
    """Concurrent ordered stages report every admitted task."""
    result = native_core.operation_task_arena_probe(workers, workers, workers, 64)
    reported_workers, peak, total_threads, _overlap, _up, _out, submitted = result
    assert reported_workers == workers
    assert 1 <= peak <= workers
    assert 1 <= total_threads <= workers
    assert submitted == 128


@pytest.mark.parametrize("workers", (2, 4))
def test_native_concurrent_producers_preserve_exact_shards(
    workers: int, require_native: None
) -> None:
    """Concurrent coordinators cannot lose shard increments."""
    _elapsed, submitted, finished, queued, started, peak = (
        native_core.operation_task_arena_concurrent_submit_probe(workers, 2, 2_000)
    )
    assert submitted == 4_000
    assert finished == 4_000
    assert queued == 0
    assert 1 <= started <= workers
    assert 1 <= peak <= workers
