"""Regression coverage for concurrency every multiworker range uses worker completion shards."""

from __future__ import annotations

import json
from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[2]
ARENA_TELEMETRY = ROOT / "cpp/src/internal/runtime/operation_task_telemetry.cc.inc"
EVIDENCE = ROOT / "benchmarks/evidence/concurrency/telemetry/completion-shards.json"


def test_every_multiworker_range_uses_worker_completion_shards() -> None:
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


def test_tsan_probe_covers_real_mid_and_high_core_arenas() -> None:
    """The focused real-arena probe validates 5, 8, and 16 worker drains."""
    source = (ROOT / "benchmarks/probes/concurrency/telemetry/completion-shards-tsan.cc").read_text(
        encoding="utf-8"
    )

    assert "OperationTaskArena::Make(workers, telemetry)" in source
    assert "for (const auto workers : {5U, 8U, 16U})" in source
    assert "arena->active_tasks() != 0U" in source
    assert "arena->queued_tasks() != 0U" in source
    assert r",\"started\":" in source and r",\"finished\":" in source


def test_evidence_covers_mid_and_high_worker_policies() -> None:
    """Evidence records both widened gates and avoids throughput overclaims."""
    evidence = json.loads(EVIDENCE.read_text(encoding="utf-8"))

    scenarios = {int(item["workers"]): item for item in evidence["scenarios"]}
    assert set(scenarios) == {5, 8, 16}
    for item in scenarios.values():
        assert item["candidate_wins"] == evidence["pair_count"] == 15
        assert item["paired_median_reduction_percent"] > 85.0


def test_native_completion_drains_exactly_across_shard_boundaries() -> None:
    """Verify the named concurrency regression contract."""
    require_native()
    for workers in (2, 4, 5, 8, 16):
        elapsed, completed, checksum, started, peak, queued, submitted = (
            native_core.ordered_executor_arena_completion_probe(workers, 20_000, 0)
        )
        assert elapsed > 0
        assert completed == 20_000
        assert checksum >= 0
        assert 1 <= started <= workers
        assert 1 <= peak <= workers
        assert queued == 0
        assert submitted == 20_000
