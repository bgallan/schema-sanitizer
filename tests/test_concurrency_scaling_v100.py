"""Regression coverage for v100 single-sentinel external task leases."""

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.concurrency_coverage import concurrency_pair_guarantees
from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[1]
LEASE = ROOT / "cpp/src/internal/runtime/external_task_lease.hh"
BENCH = ROOT / "benchmarks/v100_external_task_lease_sentinel_ab.json"
STAGE = "single_sentinel_external_task_lease_completion"


def test_v100_owner_is_the_only_mutable_completion_sentinel() -> None:
    """Verify the named concurrency regression contract."""
    source = LEASE.read_text(encoding="utf-8")
    move_start = source.index("ExternalTaskLease(ExternalTaskLease &&other)")
    move = source[move_start : source.index("ExternalTaskLease &operator=", move_start)]
    complete = source[source.index("void Complete()") : source.index("private:")]
    assert "other.owner_ = nullptr" in move
    assert "other.Complete()" not in move
    assert "owner_ = nullptr" in complete
    assert "abandon_ = nullptr" not in complete
    assert "if (owner_)" in source or "if (owner_ && abandon_)" in source


def test_v100_all_56_pairs_inherit_single_sentinel_lease() -> None:
    """Verify the named concurrency regression contract."""
    pairs = concurrency_pair_guarantees()
    assert len(pairs) == 8
    assert sum(len(outputs) for outputs in pairs.values()) == 56
    assert "python" in pairs
    for input_name, outputs in pairs.items():
        assert len(outputs) == 7, input_name
        for guarantee in outputs.values():
            assert STAGE in guarantee["shared_parallel_stages"]
            assert guarantee["source_to_sink_parallel_path"] is True
            assert guarantee["eligible_multi_benefit"] is True


def test_v100_native_order_cancel_and_external_drain_remain_exact() -> None:
    """Verify the named concurrency regression contract."""
    require_native()
    for workers in (2, 4, 5, 8, 16):
        elapsed, completed, checksum, started, peak, queued, submitted = (
            native_core.ordered_executor_arena_completion_probe(workers, 10_000, 0)
        )
        assert elapsed > 0
        assert completed == 10_000
        assert checksum >= 0
        assert 1 <= started <= workers
        assert 1 <= peak <= workers
        assert queued == 0
        assert submitted == 10_000

    elapsed, active, observed_stop, queued = native_core.operation_task_arena_cancellation_probe()
    assert elapsed >= 0
    assert active == 0
    assert observed_stop >= 1
    assert queued == 0


def test_v100_documentation_and_benchmark_cover_matrix_and_limits() -> None:
    """Verify the named concurrency regression contract."""
    benchmark = BENCH.read_text(encoding="utf-8")
    assert '"pairs": 21' in benchmark
    assert '"iterations": 30000000' in benchmark
