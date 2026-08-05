"""Regression coverage for v101 compile-time external task leases."""

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.concurrency_coverage import concurrency_pair_guarantees
from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[1]
LEASE = ROOT / "cpp/src/internal/runtime/external_task_lease.hh"
EXECUTOR = ROOT / "cpp/src/internal/runtime/ordered_executor.hh"
SUBMISSION = ROOT / "cpp/src/internal/runtime/ordered_executor_submission.cc.inc"
BENCH = ROOT / "benchmarks/v101_static_external_task_lease_ab.json"
STAGE = "compile_time_abandonment_single_shard_lease"


def test_v101_abandonment_policy_is_compile_time_and_lease_is_two_words() -> None:
    """Verify the named concurrency regression contract."""
    source = LEASE.read_text(encoding="utf-8")
    assert (
        "template <void (*Abandon)(void *, std::size_t) noexcept>" in source
        or "template <class Owner, void (Owner::*Abandon)(std::size_t) noexcept>" in source
    )
    assert "static_assert(Abandon != nullptr)" in source
    assert "Abandon(owner_, shard_);" in source or "(owner_->*Abandon)(shard_);" in source
    assert "Abandon abandon_" not in source
    assert "void *owner_" in source or "Owner *owner_" in source
    assert "std::size_t shard_" in source
    assert "shard() const noexcept" in source
    assert "other.owner_ = nullptr" in source
    assert "void Complete() noexcept { owner_ = nullptr; }" in source


def test_v101_all_56_pairs_inherit_static_single_copy_lease() -> None:
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


def test_v101_native_order_cancellation_and_drain_remain_exact() -> None:
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


def test_v101_documentation_and_benchmark_record_scope_and_limits() -> None:
    """Verify the named concurrency regression contract."""
    benchmark = BENCH.read_text(encoding="utf-8")
    assert '"pairs": 21' in benchmark
    assert '"iterations": 30000000' in benchmark
    assert '"v100_lease_bytes": 24' in benchmark
    assert '"v101_lease_bytes": 16' in benchmark
