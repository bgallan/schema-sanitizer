"""Regression coverage for v103 compact arena terminal flags."""

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.concurrency_coverage import concurrency_pair_guarantees
from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[2]
EXECUTOR = ROOT / "cpp/src/internal/runtime/ordered_executor.hh"
COMPLETION = ROOT / "cpp/src/internal/runtime/ordered_executor_arena_completion.cc.inc"
STAGE = "single_snapshot_arena_terminal_flags"


def test_v103_all_56_pairs_inherit_stage():
    """Verify the named concurrency regression contract."""
    pairs = concurrency_pair_guarantees()
    assert len(pairs) == 8
    assert sum(map(len, pairs.values())) == 56
    assert "python" in pairs
    for outputs in pairs.values():
        assert len(outputs) == 7
        for guarantee in outputs.values():
            assert STAGE in guarantee["shared_parallel_stages"]


def test_v103_native_order_cancel_and_drain():
    """Verify the named concurrency regression contract."""
    require_native()
    for workers in (2, 4, 5, 8, 16):
        errors, completed, _, started, peak, queued, submitted = (
            native_core.ordered_executor_arena_completion_probe(workers, 4000, 0)
        )
        assert errors > 0
        assert completed == 4000
        assert 1 <= started <= workers
        assert 1 <= peak <= workers
        assert queued == 0
        assert submitted == 4000
    _, active, observed, queued = native_core.operation_task_arena_cancellation_probe()
    assert active == 0
    assert observed >= 1
    assert queued == 0
