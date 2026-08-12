"""Regression coverage for concurrency worker loop compiles distinct low and parallel stop paths."""

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT / "cpp/src/internal/runtime/operation_task_arena_runtime.cc.inc"
BENCH = ROOT / "benchmarks/evidence/concurrency/lifecycle/stop-token-worker-loop.json"


def test_worker_loop_compiles_distinct_low_and_parallel_stop_paths() -> None:
    """Verify the named concurrency regression contract."""
    source = RUNTIME.read_text(encoding="utf-8")
    loop = source[
        source.index(
            "template <bool PreferDedicatedOutput, bool CheckGlobalStopping>"
        ) : source.index("[[nodiscard]] bool worker_already_started_fast_path")
    ]
    startup = source[source.index("ensure_worker_started(") :]

    assert "if constexpr (CheckGlobalStopping)" in loop
    assert "state->stopping.load(std::memory_order_acquire)" in loop
    assert "worker_loop<false, true>" in startup
    assert "worker_loop<false, false>" in startup
    assert "worker_loop<true, false>" in startup
    assert startup.index("state->worker_count > 8U") < startup.index("state->worker_count >= 4U")


def test_four_plus_hot_path_has_no_dynamic_global_stop_reload() -> None:
    """Verify the named concurrency regression contract."""
    source = RUNTIME.read_text(encoding="utf-8")
    loop = source[
        source.index("while (!stop.stop_requested())") : source.index(
            "OperationTaskArena::State::QueuedTask queued"
        )
    ]
    assert "if constexpr (CheckGlobalStopping)" in loop
    assert loop.count("state->stopping.load") == 1
    assert "StopToken" in source
    assert "admission and park wakeup" in source


def test_native_order_drain_and_cancellation_remain_exact() -> None:
    """Verify the named concurrency regression contract."""
    require_native()
    for workers in (2, 4, 5, 8, 16):
        elapsed, completed, checksum, started, peak, queued, submitted = (
            native_core.ordered_executor_arena_completion_probe(workers, 8_000, 0)
        )
        assert elapsed > 0
        assert completed == 8_000
        assert checksum >= 0
        assert 1 <= started <= workers
        assert 1 <= peak <= workers
        assert queued == 0
        assert submitted == 8_000

    elapsed, active, observed_stop, queued = native_core.operation_task_arena_cancellation_probe()
    assert elapsed >= 0
    assert active == 0
    assert observed_stop >= 1
    assert queued == 0


def test_documentation_and_benchmark_record_threshold_and_matrix() -> None:
    """Verify the named concurrency regression contract."""
    benchmark = BENCH.read_text(encoding="utf-8")
    for workers in ('"4"', '"5"', '"8"', '"16"'):
        assert workers in benchmark
