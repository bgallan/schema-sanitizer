"""Regression coverage for v80 low-core telemetry sharding."""

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
ARENA_RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
ARENA_TELEMETRY = ROOT / "cpp/src/internal/runtime/operation_task_telemetry.cc.inc"
TELEMETRY_HEADER = ROOT / "cpp/src/internal/runtime/performance_telemetry.hh"
TELEMETRY_SOURCE = ROOT / "cpp/src/internal/runtime/performance_telemetry.cc"
TELEMETRY_JSON = ROOT / "cpp/src/internal/runtime/performance_telemetry_json.cc.inc"


def test_v80_task_telemetry_shards_remain_the_common_publication_owner() -> None:
    """The v80 shards remain authoritative after later worker-range widening."""
    runtime = ARENA_RUNTIME.read_text(encoding="utf-8") + ARENA_TELEMETRY.read_text(
        encoding="utf-8"
    )
    header = TELEMETRY_HEADER.read_text(encoding="utf-8")
    source = TELEMETRY_SOURCE.read_text(encoding="utf-8") + TELEMETRY_JSON.read_text(
        encoding="utf-8"
    )

    assert "alignas(64) WorkerTaskTelemetryShard" in header
    assert "kWorkerTaskShardCount = 32" in header
    assert "RecordWorkerTaskBatch" in header
    assert "telemetry_->RecordWorkerTaskBatch(" in runtime
    assert "direct_global_" not in runtime
    assert "worker_count > 8U ? 32U : 8U" in runtime
    assert "RecordWorkerTaskBatch" in runtime
    assert "telemetry_->RecordTaskBatch(" not in runtime
    assert "low_core_task_telemetry_batches" in source
    assert "getenv" not in runtime
    assert "std::thread" not in runtime


def test_v80_telemetry_json_aggregates_global_and_worker_shards() -> None:
    """Stats retain exact totals and maxima without a sanitizer result change."""
    source = TELEMETRY_SOURCE.read_text(encoding="utf-8") + TELEMETRY_JSON.read_text(
        encoding="utf-8"
    )

    assert "const auto task_sum" in source
    assert "const auto task_maximum" in source
    assert "for (const auto &shard : worker_task_shards_)" in source
    assert "WorkerTaskTelemetryShard::started" in source
    assert "WorkerTaskTelemetryShard::finished" in source
    assert "WorkerTaskTelemetryShard::queue_wait_ns" in source
    assert "WorkerTaskTelemetryShard::run_ns" in source
    assert "WorkerTaskTelemetryShard::max_queue_wait_ns" in source
    assert "WorkerTaskTelemetryShard::max_run_ns" in source


def test_v80_all_56_pairs_inherit_shared_scheduler_reduction() -> None:
    """Every supported source-to-sink path uses the optimized common arena."""
    pairs = concurrency_pair_guarantees()

    assert sum(len(outputs) for outputs in pairs.values()) == 56
    for input_name, outputs in pairs.items():
        assert len(outputs) == 7, input_name
        for output_name, guarantee in outputs.items():
            assert (
                "low_core_worker_sharded_task_telemetry" in guarantee["shared_parallel_stages"]
            ), (input_name, output_name)
            assert guarantee["source_to_sink_parallel_path"] is True
            assert guarantee["eligible_multi_benefit"] is True


def test_v80_public_four_worker_path_publishes_exact_sharded_stats(
    tmp_path: Path,
) -> None:
    """A real conversion reports complete task totals through the new shards."""
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
            threading_mode="multi",
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

    assert submitted > 0
    assert submitted == started == finished
    assert 0 < batches <= finished
    assert int(stats["counters"]["peak_active_tasks"]) >= 2
    assert output.exists() and output.stat().st_size > 0
