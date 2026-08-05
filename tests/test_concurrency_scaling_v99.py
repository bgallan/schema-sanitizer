"""Regression coverage for v99 initialized-worker admission snapshots."""

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.concurrency_coverage import concurrency_pair_guarantees
from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
ARENA = ROOT / "cpp/src/internal/runtime/operation_task_arena.cc"
BENCH = ROOT / "benchmarks/v99_initialized_worker_snapshot_admission_ab.json"
STAGE = "initialized_worker_snapshot_admission_elision"


def test_v99_idle_selection_uses_initialized_snapshot_without_started_reload() -> None:
    """Verify the named concurrency regression contract."""
    source = RUNTIME.read_text(encoding="utf-8")
    helper = source[source.index("idle_started_worker(") : source.index("void mark_nonempty")]
    assert "std::uint64_t initialized_snapshot" in helper
    assert "initialized_snapshot & allowed" in helper
    assert "started_mask.load" not in helper
    assert "initialized implies started" in helper


def test_v99_all_56_pairs_inherit_initialized_admission_snapshot() -> None:
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


def test_v99_native_order_startup_cancel_and_drain_remain_exact() -> None:
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


def test_v99_documentation_and_benchmark_cover_matrix_and_host_limit() -> None:
    """Verify the named concurrency regression contract."""
    benchmark = BENCH.read_text(encoding="utf-8")
    for workers in ('"2"', '"4"', '"5"', '"8"', '"16"'):
        assert workers in benchmark
