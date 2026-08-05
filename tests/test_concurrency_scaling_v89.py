"""Regression coverage for v89 external-task lifetime accounting."""

from __future__ import annotations

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.concurrency_coverage import concurrency_pair_guarantees
from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[1]
EXECUTOR = ROOT / "cpp/src/internal/runtime/ordered_executor.hh"
LEASE = ROOT / "cpp/src/internal/runtime/external_task_lease.hh"
SUBMISSION = ROOT / "cpp/src/internal/runtime/ordered_executor_submission.cc.inc"


def test_v89_all_56_pairs_inherit_single_rmw_lifetime_accounting() -> None:
    """Verify the named concurrency regression contract."""
    pairs = concurrency_pair_guarantees()
    assert sum(len(outputs) for outputs in pairs.values()) == 56
    for input_name, outputs in pairs.items():
        assert len(outputs) == 7, input_name
        for guarantee in outputs.values():
            assert (
                "single_rmw_external_task_lifetime_accounting"
                in guarantee["shared_parallel_stages"]
            )
            assert guarantee["source_to_sink_parallel_path"] is True
            assert guarantee["eligible_multi_benefit"] is True


def test_v89_native_ordered_completion_still_drains_exactly() -> None:
    """Verify the named concurrency regression contract."""
    require_native()
    for workers in (2, 4):
        elapsed, completed, checksum, started, peak, queued, submitted = (
            native_core.ordered_executor_arena_completion_probe(workers, 20_000, 0)
        )
        assert elapsed > 0
        assert completed == 20_000
        assert checksum >= 0
        assert started == workers
        assert 1 <= peak <= workers
        assert queued == 0
        assert submitted == 20_000
