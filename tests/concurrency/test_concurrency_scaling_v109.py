"""Regression coverage for v109 all-worker completion telemetry sharding."""

from __future__ import annotations

import json
from pathlib import Path

from schema_sanitizer.core_impl.concurrency_coverage import (
    concurrency_pair_guarantees,
)

ROOT = Path(__file__).resolve().parents[2]
ARENA_TELEMETRY = ROOT / "cpp/src/internal/runtime/operation_task_telemetry.cc.inc"
EVIDENCE = ROOT / "benchmarks/v109_all_worker_completion_telemetry_ab.json"
STAGE = "all_worker_sharded_task_completion_telemetry"


def test_v109_every_multiworker_range_uses_worker_completion_shards() -> None:
    """All valid arena worker counts route completion batches to one shard."""
    source = ARENA_TELEMETRY.read_text(encoding="utf-8")

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


def test_v109_tsan_probe_covers_real_mid_and_high_core_arenas() -> None:
    """The focused real-arena probe validates 5, 8, and 16 worker drains."""
    source = (ROOT / "benchmarks/v109_all_worker_completion_telemetry_tsan.cc").read_text(
        encoding="utf-8"
    )

    assert "OperationTaskArena::Make(workers, telemetry)" in source
    assert "for (const auto workers : {5U, 8U, 16U})" in source
    assert "arena->active_tasks() != 0U" in source
    assert "arena->queued_tasks() != 0U" in source
    assert r",\"started\":" in source and r",\"finished\":" in source


def test_v109_all_56_pairs_inherit_all_worker_completion_shards() -> None:
    """Every supported source/sink pair crosses the common arena telemetry."""
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


def test_v109_evidence_covers_mid_and_high_worker_policies() -> None:
    """Evidence records both widened gates and avoids throughput overclaims."""
    evidence = json.loads(EVIDENCE.read_text(encoding="utf-8"))

    scenarios = {int(item["workers"]): item for item in evidence["scenarios"]}
    assert set(scenarios) == {5, 8, 16}
    for item in scenarios.values():
        assert item["candidate_wins"] == evidence["pair_count"] == 15
        assert item["paired_median_reduction_percent"] > 85.0
