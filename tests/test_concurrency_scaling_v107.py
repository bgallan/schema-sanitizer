"""Regression coverage for v107 single-store worker telemetry publication."""

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

ROOT = Path(__file__).resolve().parents[1]
HEADER = ROOT / "cpp/src/internal/runtime/performance_telemetry.hh"
SOURCE = ROOT / "cpp/src/internal/runtime/performance_telemetry.cc"
EVIDENCE = ROOT / "benchmarks/v107_worker_telemetry_single_store_ab.json"
STAGE = "single_store_worker_local_task_telemetry_publication"


def test_v107_worker_shards_keep_private_single_writer_totals() -> None:
    """Low-core telemetry shards own plain cumulative values beside snapshots."""
    header = HEADER.read_text(encoding="utf-8")
    shard = header.split("struct alignas(64) WorkerTaskTelemetryShard", 1)[1]
    shard = shard.split("static constexpr std::size_t kWorkerTaskShardCount", 1)[0]

    assert "completed_local" in shard
    assert "queue_wait_ns_local" in shard
    assert "run_ns_local" in shard
    assert "max_queue_wait_ns_local" in shard
    assert "max_run_ns_local" in shard
    assert "batches_local" in shard
    assert "std::array<std::uint64_t" in shard


def test_v107_worker_batch_publication_uses_stores_not_rmw() -> None:
    """The sole writer publishes snapshots without fetch-add or CAS loops."""
    source = SOURCE.read_text(encoding="utf-8")
    function = source.split("void PerformanceTelemetry::RecordWorkerTaskBatch", 1)[1]
    function = function.split("void PerformanceTelemetry::RecordTaskStolen", 1)[0]

    assert "signed_snapshot" in function
    assert function.count(".store(") >= 5
    assert "fetch_add" not in function
    assert "compare_exchange" not in function
    assert "update_maximum" not in function
    assert "task_started_" not in function
    assert "task_finished_" not in function


def test_v107_signed_snapshot_preserves_atomic_add_bit_pattern() -> None:
    """Unsigned locals avoid signed overflow while preserving published bits."""
    source = SOURCE.read_text(encoding="utf-8")

    assert "#include <bit>" in source
    assert "std::int64_t signed_snapshot(std::uint64_t value)" in source
    assert "return std::bit_cast<std::int64_t>(value);" in source


def test_v107_all_56_pairs_inherit_single_store_telemetry() -> None:
    """Every supported source and sink crosses the common telemetry owner."""
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


def test_v107_public_four_worker_stats_remain_exact(tmp_path: Path) -> None:
    """A real low-core conversion drains exact task snapshots and output."""
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
    batches = int(stats["counters"]["low_core_task_telemetry_batches"])

    assert submitted == started == finished > 0
    assert 0 < batches <= finished
    assert int(stats["counters"]["peak_active_tasks"]) >= 2
    assert output.exists() and output.stat().st_size > 0


def test_v107_documentation_and_evidence_record_scope() -> None:
    """Release evidence states coverage, measurement, and conservative gates."""
    evidence = json.loads(EVIDENCE.read_text(encoding="utf-8"))

    assert evidence["candidate_wins"] == evidence["pair_count"] == 15
    assert evidence["paired_median_reduction_percent"] > 70.0
    assert "not an end-to-end throughput claim" in evidence["scope"]
