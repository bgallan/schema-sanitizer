"""Regression coverage for v91 worker-count-sharded completion accounting."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.concurrency_coverage import (
    concurrency_pair_guarantees,
)
from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[2]
EXECUTOR = ROOT / "cpp/src/internal/runtime/ordered_executor.hh"
SUBMISSION = ROOT / "cpp/src/internal/runtime/ordered_executor_submission.cc.inc"
LEASE = ROOT / "cpp/src/internal/runtime/external_task_lease.hh"


def test_v91_all_56_pairs_inherit_sharded_completion_accounting() -> None:
    """Verify the named concurrency regression contract."""
    pairs = concurrency_pair_guarantees()

    assert sum(len(outputs) for outputs in pairs.values()) == 56
    for input_name, outputs in pairs.items():
        assert len(outputs) == 7, input_name
        for guarantee in outputs.values():
            assert (
                "worker_count_sharded_external_completion_accounting"
                in guarantee["shared_parallel_stages"]
            )
            assert guarantee["source_to_sink_parallel_path"] is True
            assert guarantee["eligible_multi_benefit"] is True


def test_v91_native_completion_drains_exactly_across_shard_boundaries() -> None:
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


def test_v91_stage_cancellation_preserves_lifetime_wait() -> None:
    """Verify the named concurrency regression contract."""
    require_native()
    elapsed_us, active, observed_stop, queued = (
        native_core.operation_task_arena_cancellation_probe()
    )
    assert elapsed_us > 0
    assert active == 0
    assert observed_stop >= 1
    assert queued == 0
