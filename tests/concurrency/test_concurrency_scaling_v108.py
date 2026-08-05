"""Regression coverage for v108 worker-sharded submission telemetry."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from conftest import require_native

import schema_sanitizer as ss
from schema_sanitizer.api_impl.execution_context import default_pool
from schema_sanitizer.core_impl.concurrency_coverage import (
    concurrency_pair_guarantees,
)

ROOT = Path(__file__).resolve().parents[2]
HEADER = ROOT / "cpp/src/internal/runtime/performance_telemetry.hh"
TELEMETRY = ROOT / "cpp/src/internal/runtime/performance_telemetry.cc"
TELEMETRY_JSON = ROOT / "cpp/src/internal/runtime/performance_telemetry_json.cc.inc"
ARENA = ROOT / "cpp/src/internal/runtime/operation_task_arena.cc"
EVIDENCE = ROOT / "benchmarks/v108_worker_submission_telemetry_ab.json"
STAGE = "single_store_worker_sharded_submission_telemetry"


def test_v108_submission_shards_are_separate_and_cache_aligned() -> None:
    """Producer admission snapshots do not share the worker completion shard."""
    header = HEADER.read_text(encoding="utf-8")
    submission = header.split("struct alignas(64) WorkerSubmissionTelemetryShard", 1)[1].split(
        "struct alignas(64) WorkerTaskTelemetryShard", 1
    )[0]

    assert "submitted_local" in submission
    assert "peak_queue_depth_local" in submission
    assert "worker_submission_shards_" in submission
    assert "WorkerTaskTelemetryShard" not in submission
    assert "kWorkerSubmissionShardCount = 32" in submission


def test_v108_sharded_submission_publication_uses_stores_not_rmw() -> None:
    """Queue-mutex-authorized publication performs no fetch-add or CAS loop."""
    source = TELEMETRY.read_text(encoding="utf-8")
    function = source.split("void PerformanceTelemetry::RecordWorkerTaskSubmitted", 1)[1].split(
        "void PerformanceTelemetry::RecordTaskStarted", 1
    )[0]

    assert "submitted_local" in function
    assert "peak_queue_depth_local" in function
    assert function.count(".store(") >= 2
    assert "fetch_add" not in function
    assert "compare_exchange" not in function
    assert "update_maximum" not in function
    assert "task_submitted_" not in function
    assert "&peak_queue_depth_" not in function


def test_v108_serialization_aggregates_submission_shards() -> None:
    """Final task totals, peak depth, and diagnosis include every shard."""
    source = TELEMETRY_JSON.read_text(encoding="utf-8")

    assert "task_submitted_sum" in source
    assert "for (const auto &shard : worker_submission_shards_)" in source
    assert "shard.submitted[kind_index].load" in source
    assert "shard.peak_queue_depth.load" in source
    assert "const auto submitted = task_submitted_sum(index);" in source
    assert "materialization_tasks = task_submitted_sum" in source
    assert "validation_tasks = task_submitted_sum" in source
    assert '"peak_queue_depth",\n                   peak_queue_depth()' in source


def test_v108_all_56_pairs_inherit_sharded_submission_telemetry() -> None:
    """Every supported source and sink crosses the common admission runtime."""
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


def test_v108_public_multiworker_stats_remain_exact(tmp_path: Path) -> None:
    """A real conversion drains exact sharded submissions and writes output."""
    require_native()
    if not hasattr(os, "sched_getaffinity") or not hasattr(os, "sched_setaffinity"):
        pytest.skip("CPU affinity is required for the four-worker contract")
    original_affinity = os.sched_getaffinity(0)
    if len(original_affinity) < 4:
        pytest.skip("at least four available CPUs are required")
    selected = set(sorted(original_affinity)[:4])

    source = tmp_path / "rows.jsonl"
    output = tmp_path / "rows.csv"
    with source.open("w", encoding="utf-8") as handle:
        for row in range(8_000):
            handle.write(
                json.dumps(
                    {f"field_{column:02d}": row * 24 + column for column in range(24)},
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

    assert submitted == started == finished > 0
    assert int(stats["counters"]["peak_queue_depth"]) > 0
    assert output.exists() and output.stat().st_size > 0


def test_v108_documentation_and_evidence_record_scope() -> None:
    """Release evidence records coverage, cache isolation, and both producer cases."""
    evidence = json.loads(EVIDENCE.read_text(encoding="utf-8"))

    for key in ("single_producer", "four_producers"):
        result = evidence[key]
        assert result["candidate_wins"] == result["pair_count"] == 15
        assert result["paired_median_reduction_percent"] > 30.0
    assert "not an end-to-end throughput claim" in evidence["scope"]
